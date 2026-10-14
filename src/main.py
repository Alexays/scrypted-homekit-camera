"""HomeKit accessory cameras for Scrypted.

Scrypted becomes the HomeKit controller of cameras paired natively over HAP
(Aqara E1, G4...): it negotiates the live SRTP streams and relays them to the
consumer's ffmpeg, mirrors the native motion sensor, sends talk-back as
AAC-ELD, and turns Aqara pan/tilt cameras to their presets through the Aqara cloud.
Each `<name>.pairing` file (aiohomekit pairing data) in PAIRINGS_DIR is one
camera, with a companion privacy switch; doorbells ring, push their events to a
webhook, and take talk-back as raw PCM over TCP as well.
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import signal
import socket
import struct
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path

import scrypted_sdk
from scrypted_sdk import ScryptedDeviceBase
from scrypted_sdk.types import (
    BinarySensor,
    DeviceProvider,
    HttpRequestHandler,
    Intercom,
    MediaObject,
    MotionSensor,
    OnOff,
    PanTiltZoom,
    ScryptedDeviceType,
    ScryptedInterface,
    ScryptedMimeTypes,
    Settings,
    VideoCamera,
)

from aqara_cloud import SERVERS, AqaraCloud

from aiohomekit import Controller
from aiohomekit.camera import CameraStream
from aiohomekit.characteristic_cache import CharacteristicCacheMemory
from aiohomekit.model import Accessories
from aiohomekit.model.characteristics import CharacteristicsTypes
from aiohomekit.model.services import ServicesTypes
from aiohomekit.zeroconf import HAP_TYPE_TCP, HAP_TYPE_UDP, ZeroconfServiceListener
from pylibsrtp import Policy, Session
from zeroconf.asyncio import AsyncServiceBrowser, AsyncZeroconf

PAIRINGS_DIR = Path.home() / ".config" / "homekit-cameras"
ELD_ENCODER = Path(__file__).parent / "eld_encoder"
MAX_BITRATE = 2000  # kbps
# 1080p keyframes arrive as bursts of a few hundred packets; the 786 KB macOS default drops some.
UDP_BUFFER = 4 << 20
# The consumer's ffmpeg takes a while to open the SDP; relays refused before then are expected.
CONSUMER_GRACE = 20
ACCESSORY_SILENCE = 10
# Aqara's vendor characteristic holding the device id the Aqara cloud knows the camera by.
AQARA_DID = "C7EECAA7-91D9-40EB-AD0C-FFDDE3143CB9"
PRIVACY_SUFFIX = ":privacy"
# How long a ring keeps the doorbell's binary state on; HomeKit rings on the rising edge.
RING_HOLD = 5
# A viewer outside HomeKit (a dashboard) renews its watch lease every few seconds; a lapsed lease
# means it went away without saying so.
VIEW_LEASE = 15

STREAM_CHARACTERISTICS = {
    "streaming_status": CharacteristicsTypes.STREAMING_STATUS,
    "supported_video": CharacteristicsTypes.SUPPORTED_VIDEO_STREAM_CONFIGURATION,
    "supported_audio": CharacteristicsTypes.SUPPORTED_AUDIO_CONFIGURATION,
    "supported_rtp": CharacteristicsTypes.SUPPORTED_RTP_CONFIGURATION,
    "setup_endpoints": CharacteristicsTypes.SETUP_ENDPOINTS,
    "selected_rtp": CharacteristicsTypes.SELECTED_RTP_STREAM_CONFIGURATION,
}


def _free_port_pair() -> int:
    """An even port whose successor is free too: ffmpeg binds RTP there and RTCP on port + 1."""
    while True:
        port = random.randrange(40000, 60000, 2)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as rtp, socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as rtcp:
            try:
                rtp.bind(("", port))
                rtcp.bind(("", port + 1))
                return port
            except OSError:
                continue


class Relay(asyncio.DatagramProtocol):
    """Forwards the accessory's SRTP packets untouched to the consumer's ffmpeg on localhost."""

    def __init__(self, port: int) -> None:
        self.out = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.out.setblocking(False)
        self.out.connect(("127.0.0.1", port))
        self.transport: asyncio.DatagramTransport | None = None
        self.last_packet = time.monotonic()
        self.last_refused = 0.0

    def connection_made(self, transport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr) -> None:
        self.last_packet = time.monotonic()
        try:
            self.out.send(data)
        except ConnectionRefusedError:
            # Nobody listens on the consumer port: ffmpeg has not opened the SDP yet, or has exited.
            self.last_refused = self.last_packet
        except BlockingIOError:
            pass

    def close(self) -> None:
        self.transport.close()
        self.out.close()


class Stream:
    """One RTP stream management service of the camera: an independent live session."""

    def __init__(self, id: str, cam: CameraStream, attrs, sdp_path: Path) -> None:
        self.id = id
        self.cam = cam
        self.width, self.height, self.fps = attrs.width, attrs.height, attrs.fps
        self.sdp_path = sdp_path
        self.session = None
        self.video: Relay | None = None
        self.audio: Relay | None = None
        self.started = 0.0
        self.lock = asyncio.Lock()

    def options(self) -> dict:
        return {
            "id": self.id,
            "name": f"{self.height}p",
            "tool": "ffmpeg",
            "video": {"codec": "h264", "width": self.width, "height": self.height},
            "audio": {"codec": "aac", "sampleRate": 16000},
        }

    async def start(self, local_ip: str) -> None:
        loop = asyncio.get_running_loop()
        video_out, audio_out = _free_port_pair(), _free_port_pair()
        _, video = await loop.create_datagram_endpoint(lambda: Relay(video_out), local_addr=(local_ip, 0))
        _, audio = await loop.create_datagram_endpoint(lambda: Relay(audio_out), local_addr=(local_ip, 0))
        for relay in (video, audio):
            relay.transport.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, UDP_BUFFER)
            relay.out.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, UDP_BUFFER)
        try:
            session = await self.cam.start(
                local_ip=local_ip,
                video_port=video.transport.get_extra_info("sockname")[1],
                audio_port=audio.transport.get_extra_info("sockname")[1],
                width=self.width, height=self.height, fps=self.fps, audio=True, max_bitrate=MAX_BITRATE,
            )
        except Exception:
            video.close()
            audio.close()
            raise
        self.sdp_path.write_text(session.sdp("127.0.0.1", video_port=video_out, audio_port=audio_out))
        self.session, self.video, self.audio, self.started = session, video, audio, time.monotonic()
        asyncio.ensure_future(self._send_receiver_reports(session))
        print(f"{self.height}p stream started from {session.accessory_address.ip_address}")

    async def _send_receiver_reports(self, session) -> None:
        """The accessory ends a session whose controller sends no RTCP; the consumer's ffmpeg reports to localhost only."""
        address = session.accessory_address
        reports = []
        for relay, srtp, port, params in (
            (self.video, session.controller_video_srtp, address.video_rtp_port, session.selected.video_params),
            (self.audio, session.controller_audio_srtp, address.audio_rtp_port, session.selected.audio_params),
        ):
            if params:
                protect = Session(Policy(key=srtp.master_key + srtp.master_salt, ssrc_type=Policy.SSRC_ANY_OUTBOUND)).protect_rtcp
                # Empty receiver report (RC=0): version 2, PT 201, length 1 word, sender SSRC.
                reports.append((relay.transport, protect, struct.pack("!BBHI", 0x80, 201, 1, params.rtp_params.ssrc), (address.ip_address, port)))
        while self.session is session:
            for transport, protect, report, destination in reports:
                transport.sendto(protect(report), destination)
            await asyncio.sleep(0.5)

    async def end(self) -> None:
        if not self.session:
            return
        session, self.session = self.session, None
        self.video.close()
        self.audio.close()
        try:
            await asyncio.wait_for(self.cam.stop(session), 5)
            print(f"{self.height}p stream ended")
        except Exception as exc:
            print(f"{self.height}p stream end failed: {type(exc).__name__}: {exc}")


class HomeKitCamera(ScryptedDeviceBase, VideoCamera, MotionSensor, BinarySensor, Intercom, PanTiltZoom, Settings):
    def __init__(self, nativeId: str, controller: Controller, plugin: HomeKitCamerasPlugin) -> None:
        super().__init__(nativeId)
        self.plugin = plugin
        pairing_data = json.loads((PAIRINGS_DIR / f"{nativeId}.pairing").read_text())
        self.pairing = controller.load_pairing(nativeId, pairing_data)
        self.accessory_ip = pairing_data["AccessoryIP"]
        self.streams: list[Stream] = []
        self.did: str | None = None
        self.presets: dict[str, int] = {}  # Aqara Home preset name -> id
        self.preset = None  # last preset this plugin moved the camera to
        self.privacy: PrivacySwitch | None = None
        self.talkback: list[asyncio.subprocess.Process] = []
        self.talkback_stream: Stream | None = None
        self.ring_reset: asyncio.TimerHandle | None = None
        self.view_until = 0.0  # watch lease of a viewer outside HomeKit
        self.talk_server: asyncio.base_events.Server | None = None
        self.ready = asyncio.ensure_future(self._connect())
        asyncio.ensure_future(self._watch())

    async def _connect(self) -> None:
        while True:
            try:
                accessories = Accessories.from_list(await self.pairing.list_accessories_and_characteristics())
                break
            except Exception as exc:
                print(f"accessory unreachable, retrying: {type(exc).__name__}: {exc}")
                await asyncio.sleep(10)

        accessory = next(a for a in accessories if a.services.first(service_type=ServicesTypes.CAMERA_RTP_STREAM_MANAGEMENT))
        for service in accessory.services.filter(service_type=ServicesTypes.CAMERA_RTP_STREAM_MANAGEMENT):
            config = service.value(CharacteristicsTypes.SUPPORTED_VIDEO_STREAM_CONFIGURATION)
            attrs = max((a for c in config.config for a in c.video_attrs or []), key=lambda a: a.width * a.height)
            cam = CameraStream(self.pairing, accessory.aid, {name: service[t].iid for name, t in STREAM_CHARACTERISTICS.items()})
            sdp_path = Path(tempfile.gettempdir()) / f"homekit-camera-{self.nativeId}-{service.iid}.sdp"
            self.streams.append(Stream(str(service.iid), cam, attrs, sdp_path))
        self.streams.sort(key=lambda s: s.width * s.height, reverse=True)

        handlers = {}
        motion = accessory.services.first(service_type=ServicesTypes.MOTION_SENSOR)
        if motion:
            char = motion[CharacteristicsTypes.MOTION_DETECTED]
            handlers[(accessory.aid, char.iid)] = self._on_motion
            self.motionDetected = bool(char.value)
        doorbell = accessory.services.first(service_type=ServicesTypes.DOORBELL)
        if doorbell:
            handlers[(accessory.aid, doorbell[CharacteristicsTypes.INPUT_EVENT].iid)] = self._on_ring
            self.binaryState = False
            if not self.storage.getItem("doorbell"):
                self.storage.setItem("doorbell", "true")
                await scrypted_sdk.deviceManager.onDeviceDiscovered(self.plugin.manifest(self.nativeId, doorbell=True))
        if handlers:

            def on_event(events: dict) -> None:
                for key, handler in handlers.items():
                    if key in events:
                        handler(events[key]["value"])

            self.pairing.dispatcher_connect(on_event)
            await self.pairing.subscribe(list(handlers))

        self.did = next((c.value for s in accessory.services for c in s.characteristics if c.type == AQARA_DID), None)
        print(f"ready: streams {[f'{s.width}x{s.height}' for s in self.streams]}, motion sensor: {bool(motion)}, doorbell: {bool(doorbell)}, aqara did: {self.did}")
        await self.serve_talkback()
        await self.load_presets()

    def _on_motion(self, value) -> None:
        self.motionDetected = bool(value)
        self.plugin.notify(self.name, "motion", bool(value))

    def _on_ring(self, value) -> None:
        # Any press (single, double, long) rings; the reset lets HomeKit see the next rising edge.
        self.binaryState = True
        self.plugin.notify(self.name, "ring", True)
        if self.ring_reset:
            self.ring_reset.cancel()
        self.ring_reset = asyncio.get_running_loop().call_later(RING_HOLD, setattr, self, "binaryState", False)

    # Video

    async def getVideoStreamOptions(self) -> list:
        return [stream.options() for stream in self.streams]

    async def getVideoStream(self, options: dict = None) -> MediaObject:
        await asyncio.shield(self.ready)
        stream = next((s for s in self.streams if s.id == (options or {}).get("id")), self.streams[0])
        async with stream.lock:
            # One session per stream: a new request replaces the session of a consumer that went away.
            await self._end(stream)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
                probe.connect((self.accessory_ip, 9))
                local_ip = probe.getsockname()[0]
            await stream.start(local_ip)
        await self.update_position()
        return await scrypted_sdk.mediaManager.createFFmpegMediaObject({
            "inputArguments": [
                "-protocol_whitelist", "file,crypto,data,rtp,udp,srtp", "-buffer_size", str(UDP_BUFFER),
                "-f", "sdp", "-i", str(stream.sdp_path),
            ],
            "mediaStreamOptions": stream.options(),
        })

    async def _end(self, stream: Stream) -> None:
        if stream is self.talkback_stream:
            self._stop_talkback()
        await stream.end()

    async def end_all(self) -> None:
        await asyncio.gather(*(self._end(stream) for stream in self.streams))

    async def _watch(self) -> None:
        while True:
            await asyncio.sleep(2)
            if self.view_until and time.monotonic() > self.view_until:
                self.view_until = 0.0
                await self.update_position()
            for stream in self.streams:
                if not stream.session:
                    continue
                now = time.monotonic()
                talking = stream is self.talkback_stream and any(p.returncode is None for p in self.talkback)
                consumer_gone = now - stream.started > CONSUMER_GRACE and now - stream.video.last_refused < 2 and not talking
                accessory_silent = now - stream.video.last_packet > ACCESSORY_SILENCE
                if consumer_gone or accessory_silent:
                    print(f"{stream.height}p {'consumer gone' if consumer_gone else 'accessory stopped sending'} after {now - stream.started:.0f}s")
                    async with stream.lock:
                        await self._end(stream)
                    await self.update_position()

    # Pan/tilt: Aqara Home presets, and the privacy switch picking between two of them

    async def load_presets(self) -> None:
        if not self.did or not self.plugin.cloud.session:
            return
        try:
            self.presets = await self.plugin.cloud.ptz_presets(self.did)
        except Exception as exc:
            print(f"cannot load presets: {exc}")
            return
        self.ptzCapabilities = {"pan": False, "tilt": False, "zoom": False, "presets": {name: name for name in self.presets}}
        if self.presets and not self.storage.getItem("ptz"):
            self.storage.setItem("ptz", "true")
            await scrypted_sdk.deviceManager.onDeviceDiscovered(self.plugin.privacy_manifest(self.nativeId))
        await self.update_position()

    async def ptzCommand(self, command: dict) -> None:
        await self.move_to(command.get("preset"))

    async def move_to(self, preset: str | None) -> None:
        if preset not in self.presets:
            print(f"unknown preset {preset!r}, known: {list(self.presets)}")
            return
        try:
            await self.plugin.cloud.run_ptz_preset(self.did, self.presets[preset])
        except Exception as exc:
            print(f"preset {preset} failed: {exc}")
            return
        self.preset = preset
        print(f"turned to preset {preset}")

    async def view(self, watching: bool) -> None:
        """A viewer outside HomeKit starts, renews or ends watching live."""
        self.view_until = time.monotonic() + VIEW_LEASE if watching else 0.0
        await self.update_position()

    def watching(self) -> bool:
        if self.view_until:
            return True
        live = self.storage.getItem("liveStream") or (self.streams[0].id if self.streams else None)
        return any(s.session for s in self.streams if s.id == live)

    async def update_position(self) -> None:
        """Faces the privacy preset while privacy is on and nobody watches live, the watch preset otherwise."""
        if not self.privacy or not self.presets:
            return
        privacy = self.privacy.on and not self.watching()
        target = self.storage.getItem("privacyPreset" if privacy else "watchPreset")
        if target and target != self.preset:
            await self.move_to(target)

    async def getSettings(self) -> list:
        presets = list(self.presets)
        return [
            {
                "key": "privacyPreset", "title": "Privacy Preset", "choices": presets,
                "description": "Faced while the privacy switch is on and nobody watches live.",
                "value": self.storage.getItem("privacyPreset"),
            },
            {
                "key": "watchPreset", "title": "Watch Preset", "choices": presets,
                "description": "Faced while privacy is off, or while someone watches live.",
                "value": self.storage.getItem("watchPreset"),
            },
            {
                "key": "talkbackPort", "title": "Talk-back Port", "type": "number",
                "description": "TCP port taking raw s16le 16 kHz mono PCM as talk-back while a client stays connected.",
                "value": self.storage.getItem("talkbackPort"),
            },
            {
                "key": "liveStream", "title": "Live Stream", "choices": [s.id for s in self.streams],
                "description": "The stream whose session means someone watches live. Keep it out of the prebuffered streams.",
                "value": self.storage.getItem("liveStream") or (self.streams[0].id if self.streams else None),
            },
        ]

    async def putSetting(self, key: str, value) -> None:
        self.storage.setItem(key, str(value) if key == "talkbackPort" else value)
        if key == "talkbackPort":
            await self.serve_talkback()
        await self.update_position()
        await scrypted_sdk.deviceManager.onDeviceEvent(self.nativeId, ScryptedInterface.Settings.value, None)

    # Talk-back

    async def _talk_stream(self) -> Stream:
        """Talk-back rides on a running session; the live view normally has one open."""
        await asyncio.shield(self.ready)
        stream = next((s for s in self.streams if s.session), None)
        if not stream:
            await self.getVideoStream()
            stream = self.streams[0]
        return stream

    async def _start_encoder(self, stream: Stream, stdin) -> asyncio.subprocess.Process:
        # fdk-aac through ffmpeg cannot produce the 480-sample frames HomeKit accessories decode.
        encoder = await asyncio.create_subprocess_exec(
            str(ELD_ENCODER), stdin=stdin, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
        asyncio.ensure_future(self._send_talkback(stream.session, stream.audio.transport, encoder.stdout))
        return encoder

    async def startIntercom(self, media: MediaObject) -> None:
        self._stop_talkback()
        stream = await self._talk_stream()
        source = await scrypted_sdk.mediaManager.convertMediaObjectToJSON(media, ScryptedMimeTypes.FFmpegInput.value)
        ffmpeg = await scrypted_sdk.mediaManager.getFFmpegPath()
        pcm_read, pcm_write = os.pipe()
        decoder = await asyncio.create_subprocess_exec(
            ffmpeg, "-hide_banner", "-loglevel", "error", *source["inputArguments"],
            "-vn", "-ac", "1", "-ar", "16000", "-f", "s16le", "pipe:1",
            stdout=pcm_write,
        )
        encoder = await self._start_encoder(stream, pcm_read)
        os.close(pcm_read)
        os.close(pcm_write)
        self.talkback, self.talkback_stream = [decoder, encoder], stream

    async def serve_talkback(self) -> None:
        if self.talk_server:
            self.talk_server.close()
            self.talk_server = None
        port = self.storage.getItem("talkbackPort")
        if port:
            self.talk_server = await asyncio.start_server(self._on_talk_client, "0.0.0.0", int(port))

    async def _on_talk_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Raw s16le 16 kHz mono PCM in, talk-back out, for as long as the client stays connected.

        Skips ffmpeg entirely, so the only added latency is one 30 ms AAC-ELD frame.
        """
        self._stop_talkback()
        stream = await self._talk_stream()
        encoder = await self._start_encoder(stream, asyncio.subprocess.PIPE)
        self.talkback, self.talkback_stream = [encoder], stream
        try:
            while chunk := await reader.read(4096):
                if encoder.returncode is not None:
                    break
                encoder.stdin.write(chunk)
                await encoder.stdin.drain()
        except ConnectionError:
            pass
        finally:
            if encoder.returncode is None:
                encoder.stdin.close()
            writer.close()

    async def _send_talkback(self, session, transport, aus: asyncio.StreamReader) -> None:
        srtp = session.controller_audio_srtp
        protect = Session(Policy(key=srtp.master_key + srtp.master_salt, ssrc_type=Policy.SSRC_ANY_OUTBOUND)).protect
        rtp = session.selected.audio_params.rtp_params
        destination = (session.accessory_address.ip_address, session.accessory_address.audio_rtp_port)
        seq, ts = random.randrange(1 << 16), random.randrange(1 << 32)
        try:
            while True:
                size = struct.unpack("!H", await aus.readexactly(2))[0]
                au = await aus.readexactly(size)
                # One access unit per packet behind a single AU header (size << 3, index 0), as the accessory sends.
                packet = struct.pack("!BBHIIHH", 0x80, rtp.payload_type, seq, ts, rtp.ssrc, 16, size << 3) + au
                if transport.is_closing():
                    break
                transport.sendto(protect(packet), destination)
                seq = (seq + 1) & 0xFFFF
                ts = (ts + 480) & 0xFFFFFFFF
        except asyncio.IncompleteReadError:
            pass

    async def stopIntercom(self) -> None:
        self._stop_talkback()

    def _stop_talkback(self) -> None:
        for process in self.talkback:
            if process.returncode is None:
                process.kill()
        self.talkback, self.talkback_stream = [], None


class PrivacySwitch(ScryptedDeviceBase, OnOff):
    """On: the camera faces its privacy preset unless someone watches live."""

    def __init__(self, nativeId: str, camera: HomeKitCamera) -> None:
        super().__init__(nativeId)
        self.camera = camera
        camera.privacy = self
        if self.on is None:
            self.on = False

    async def turnOn(self) -> None:
        self.on = True
        await self.camera.update_position()

    async def turnOff(self) -> None:
        self.on = False
        await self.camera.update_position()


class HomeKitCamerasPlugin(ScryptedDeviceBase, DeviceProvider, Settings, HttpRequestHandler):
    def __init__(self) -> None:
        super().__init__()
        self.devices: dict[str, HomeKitCamera | PrivacySwitch] = {}
        self.controller: Controller | None = None
        session = self.storage.getItem("aqaraSession")
        self.cloud = AqaraCloud(self.storage.getItem("aqaraRegion") or "EU", json.loads(session) if session else None)
        self.started = asyncio.ensure_future(self._start())
        try:
            asyncio.get_event_loop().add_signal_handler(signal.SIGTERM, lambda: asyncio.ensure_future(self._shutdown()))
        except (NotImplementedError, RuntimeError) as exc:
            print(f"no SIGTERM handler, streams will not be ended on unload: {exc}")

    async def _start(self) -> None:
        ELD_ENCODER.chmod(0o755)  # zip extraction drops the executable bit
        zeroconf = AsyncZeroconf()
        AsyncServiceBrowser(zeroconf.zeroconf, [HAP_TYPE_TCP, HAP_TYPE_UDP], listener=ZeroconfServiceListener())
        self.controller = Controller(async_zeroconf_instance=zeroconf, char_cache=CharacteristicCacheMemory())
        await self.controller.async_start()
        devices = []
        existing = set(scrypted_sdk.deviceManager.getNativeIds())
        for path in sorted(PAIRINGS_DIR.glob("*.pairing")):
            storage = scrypted_sdk.deviceManager.getDeviceStorage(path.stem)
            devices.append(self.manifest(path.stem, bool(storage.getItem("doorbell"))))
            # Only pan/tilt cameras get a privacy switch, once their presets have shown up.
            if storage.getItem("ptz") or path.stem + PRIVACY_SUFFIX in existing:
                devices.append(self.privacy_manifest(path.stem))
        await scrypted_sdk.deviceManager.onDevicesChanged({"devices": devices})

    def manifest(self, nativeId: str, doorbell: bool = False) -> dict:
        interfaces = [
            ScryptedInterface.VideoCamera.value,
            ScryptedInterface.MotionSensor.value,
            ScryptedInterface.Intercom.value,
            ScryptedInterface.PanTiltZoom.value,
            ScryptedInterface.Settings.value,
        ]
        return {
            "nativeId": nativeId,
            "name": nativeId,
            "type": (ScryptedDeviceType.Doorbell if doorbell else ScryptedDeviceType.Camera).value,
            "interfaces": interfaces + ([ScryptedInterface.BinarySensor.value] if doorbell else []),
            "info": {"manufacturer": "HomeKit"},
        }

    def privacy_manifest(self, nativeId: str) -> dict:
        return {
            "nativeId": nativeId + PRIVACY_SUFFIX,
            "name": f"{nativeId} Privacy",
            "type": ScryptedDeviceType.Switch.value,
            "interfaces": [ScryptedInterface.OnOff.value],
        }

    def notify(self, camera: str, event: str, state: bool) -> None:
        """Posts a camera event to the webhook (a Home Assistant webhook trigger) without holding up the caller."""
        url = self.storage.getItem("eventWebhook")
        if not url:
            return
        body = json.dumps({"camera": camera, "event": event, "state": state}).encode()

        def post() -> None:
            request = urllib.request.Request(url, data=body, method="POST", headers={"Content-Type": "application/json"})
            try:
                urllib.request.urlopen(request, timeout=5).close()
            except Exception as exc:
                print(f"event webhook failed: {exc}")

        asyncio.get_running_loop().run_in_executor(None, post)

    async def _shutdown(self) -> None:
        # Scrypted sends SIGKILL one second after SIGTERM. A camera left streaming to a
        # controller that vanished can stop answering HAP until it is power cycled.
        ends = [asyncio.ensure_future(d.end_all()) for d in self.devices.values() if isinstance(d, HomeKitCamera)]
        if ends:
            await asyncio.wait(ends, timeout=0.8)
        os._exit(0)

    async def getDevice(self, nativeId: str):
        await self.started
        if nativeId not in self.devices:
            if nativeId.endswith(PRIVACY_SUFFIX):
                camera = await self.getDevice(nativeId[: -len(PRIVACY_SUFFIX)])
                self.devices[nativeId] = PrivacySwitch(nativeId, camera)
                asyncio.ensure_future(camera.update_position())
            else:
                self.devices[nativeId] = HomeKitCamera(nativeId, self.controller, self)
        return self.devices[nativeId]

    # Aqara account, for pan/tilt presets. The password only serves the login; the session token is kept.

    async def getSettings(self) -> list:
        return [
            {"key": "aqaraRegion", "title": "Aqara Region", "choices": list(SERVERS), "value": self.cloud.region},
            {"key": "aqaraEmail", "title": "Aqara Account", "value": self.storage.getItem("aqaraEmail")},
            {
                "key": "aqaraPassword", "title": "Aqara Password", "type": "password", "value": "",
                "description": "Logs in when set; only the session token is stored.",
            },
            {
                "key": "eventWebhook", "title": "Event Webhook",
                "description": "URL getting a JSON POST {camera, event: ring|motion, state} on each camera event.",
                "value": self.storage.getItem("eventWebhook"),
            },
            {
                "key": "aqaraStatus", "title": "Aqara Login", "readonly": True,
                "value": "logged in" if self.cloud.session else "not logged in",
            },
        ]

    async def putSetting(self, key: str, value) -> None:
        if key == "aqaraPassword":
            try:
                session = await self.cloud.login(self.storage.getItem("aqaraEmail") or "", value)
            except Exception as exc:
                print(f"Aqara login failed: {exc}")
            else:
                self.storage.setItem("aqaraSession", json.dumps(session))
                for device in self.devices.values():
                    if isinstance(device, HomeKitCamera):
                        await device.load_presets()
        else:
            self.storage.setItem(key, value)
            if key == "aqaraRegion":
                self.cloud.region = value
        await scrypted_sdk.deviceManager.onDeviceEvent(None, ScryptedInterface.Settings.value, None)

    async def onRequest(self, request: dict, response) -> None:
        """GET <endpoint>/public/watch?camera=<name>&watching=1|0: a dashboard opening, renewing or closing a live view."""
        url = urllib.parse.urlparse(request["url"])
        query = urllib.parse.parse_qs(url.query)
        name = query.get("camera", [""])[0]
        camera = next((d for d in self.devices.values() if isinstance(d, HomeKitCamera) and name in (d.name, d.nativeId)), None)
        if not url.path.endswith("/watch") or not camera:
            response.send("not found", {"code": 404})
            return
        await camera.view(query.get("watching", ["1"])[0] == "1")
        response.send("ok", {"code": 200, "headers": {"Access-Control-Allow-Origin": "*"}})

    async def releaseDevice(self, id: str, nativeId: str) -> None:
        device = self.devices.pop(nativeId, None)
        if isinstance(device, HomeKitCamera):
            await device.end_all()


def create_scrypted_plugin():
    return HomeKitCamerasPlugin()
