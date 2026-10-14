"""HomeKit accessory cameras for Scrypted.

Scrypted becomes the HomeKit controller of cameras paired natively over HAP
(Aqara E1, G4...): it negotiates the live SRTP stream and relays it to the
consumer's ffmpeg, mirrors the native motion sensor, and sends talk-back as
AAC-ELD. Each `<name>.pairing` file (aiohomekit pairing data) in PAIRINGS_DIR
is one camera.
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
from pathlib import Path

import scrypted_sdk
from scrypted_sdk import ScryptedDeviceBase
from scrypted_sdk.types import (
    DeviceProvider,
    Intercom,
    MediaObject,
    MotionSensor,
    ScryptedDeviceType,
    ScryptedInterface,
    ScryptedMimeTypes,
    VideoCamera,
)

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
# The consumer's ffmpeg takes a while to open the SDP; relays refused before then are expected.
CONSUMER_GRACE = 20
ACCESSORY_SILENCE = 10

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


class HomeKitCamera(ScryptedDeviceBase, VideoCamera, MotionSensor, Intercom):
    def __init__(self, nativeId: str, controller: Controller) -> None:
        super().__init__(nativeId)
        pairing_data = json.loads((PAIRINGS_DIR / f"{nativeId}.pairing").read_text())
        self.pairing = controller.load_pairing(nativeId, pairing_data)
        self.accessory_ip = pairing_data["AccessoryIP"]
        self.sdp_path = Path(tempfile.gettempdir()) / f"homekit-camera-{nativeId}.sdp"
        self.cam: CameraStream | None = None
        self.stream_size = (None, None, None)
        self.session = None
        self.video: Relay | None = None
        self.audio: Relay | None = None
        self.started = 0.0
        self.talkback: list[asyncio.subprocess.Process] = []
        self.lock = asyncio.Lock()
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

        # Cameras may expose several stream services; take the one offering the largest picture.
        best = None
        for accessory in accessories:
            for service in accessory.services.filter(service_type=ServicesTypes.CAMERA_RTP_STREAM_MANAGEMENT):
                config = service.value(CharacteristicsTypes.SUPPORTED_VIDEO_STREAM_CONFIGURATION)
                attrs = max((a for c in config.config for a in c.video_attrs or []), key=lambda a: a.width * a.height)
                if best is None or attrs.width * attrs.height > best[2].width * best[2].height:
                    best = (accessory, service, attrs)
        accessory, service, attrs = best
        self.cam = CameraStream(self.pairing, accessory.aid, {name: service[t].iid for name, t in STREAM_CHARACTERISTICS.items()})
        self.stream_size = (attrs.width, attrs.height, attrs.fps)

        motion = accessory.services.first(service_type=ServicesTypes.MOTION_SENSOR)
        if motion:
            char = motion[CharacteristicsTypes.MOTION_DETECTED]
            key = (accessory.aid, char.iid)
            self.motionDetected = bool(char.value)

            def on_event(events: dict) -> None:
                if key in events:
                    self.motionDetected = bool(events[key]["value"])

            self.pairing.dispatcher_connect(on_event)
            await self.pairing.subscribe([key])
        print(f"ready: {attrs.width}x{attrs.height}@{attrs.fps}, motion sensor: {bool(motion)}")

    def _stream_options(self) -> dict:
        width, height, _ = self.stream_size
        return {
            "id": "hap",
            "name": "HomeKit",
            "tool": "ffmpeg",
            "video": {"codec": "h264", "width": width, "height": height},
            "audio": {"codec": "aac", "sampleRate": 16000},
        }

    async def getVideoStreamOptions(self) -> list:
        return [self._stream_options()]

    async def getVideoStream(self, options: dict = None) -> MediaObject:
        await asyncio.shield(self.ready)
        async with self.lock:
            # One session at a time: a new request replaces the stream of a consumer that went away.
            await self._end_session()
            await self._start_session()
        return await scrypted_sdk.mediaManager.createFFmpegMediaObject({
            "inputArguments": ["-protocol_whitelist", "file,crypto,data,rtp,udp,srtp", "-f", "sdp", "-i", str(self.sdp_path)],
            "mediaStreamOptions": self._stream_options(),
        })

    async def _start_session(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.connect((self.accessory_ip, 9))
            local_ip = probe.getsockname()[0]
        loop = asyncio.get_running_loop()
        video_out, audio_out = _free_port_pair(), _free_port_pair()
        _, video = await loop.create_datagram_endpoint(lambda: Relay(video_out), local_addr=(local_ip, 0))
        _, audio = await loop.create_datagram_endpoint(lambda: Relay(audio_out), local_addr=(local_ip, 0))
        width, height, fps = self.stream_size
        try:
            session = await self.cam.start(
                local_ip=local_ip,
                video_port=video.transport.get_extra_info("sockname")[1],
                audio_port=audio.transport.get_extra_info("sockname")[1],
                width=width, height=height, fps=fps, audio=True, max_bitrate=MAX_BITRATE,
            )
        except Exception:
            video.close()
            audio.close()
            raise
        self.sdp_path.write_text(session.sdp("127.0.0.1", video_port=video_out, audio_port=audio_out))
        self.session, self.video, self.audio, self.started = session, video, audio, time.monotonic()
        asyncio.ensure_future(self._send_receiver_reports(session))
        print(f"stream started from {session.accessory_address.ip_address}")

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

    async def _end_session(self) -> None:
        if not self.session:
            return
        session, self.session = self.session, None
        self._stop_talkback()
        self.video.close()
        self.audio.close()
        try:
            await asyncio.wait_for(self.cam.stop(session), 5)
            print("stream ended")
        except Exception as exc:
            print(f"stream end failed: {type(exc).__name__}: {exc}")

    async def _watch(self) -> None:
        while True:
            await asyncio.sleep(2)
            if not self.session:
                continue
            now = time.monotonic()
            consumer_gone = now - self.started > CONSUMER_GRACE and now - self.video.last_refused < 2 and not any(p.returncode is None for p in self.talkback)
            accessory_silent = now - self.video.last_packet > ACCESSORY_SILENCE
            if consumer_gone or accessory_silent:
                print(f"{'consumer gone' if consumer_gone else 'accessory stopped sending'} after {now - self.started:.0f}s")
                async with self.lock:
                    await self._end_session()

    async def startIntercom(self, media: MediaObject) -> None:
        await asyncio.shield(self.ready)
        self._stop_talkback()
        async with self.lock:
            if not self.session:
                await self._start_session()
        session = self.session
        source = await scrypted_sdk.mediaManager.convertMediaObjectToJSON(media, ScryptedMimeTypes.FFmpegInput.value)
        ffmpeg = await scrypted_sdk.mediaManager.getFFmpegPath()
        pcm_read, pcm_write = os.pipe()
        decoder = await asyncio.create_subprocess_exec(
            ffmpeg, "-hide_banner", "-loglevel", "error", *source["inputArguments"],
            "-vn", "-ac", "1", "-ar", "16000", "-f", "s16le", "pipe:1",
            stdout=pcm_write,
        )
        # fdk-aac through ffmpeg cannot produce the 480-sample frames HomeKit accessories decode.
        encoder = await asyncio.create_subprocess_exec(
            str(ELD_ENCODER), stdin=pcm_read, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
        os.close(pcm_read)
        os.close(pcm_write)
        self.talkback = [decoder, encoder]
        asyncio.ensure_future(self._send_talkback(session, encoder.stdout))

    async def _send_talkback(self, session, aus: asyncio.StreamReader) -> None:
        srtp = session.controller_audio_srtp
        protect = Session(Policy(key=srtp.master_key + srtp.master_salt, ssrc_type=Policy.SSRC_ANY_OUTBOUND)).protect
        rtp = session.selected.audio_params.rtp_params
        destination = (session.accessory_address.ip_address, session.accessory_address.audio_rtp_port)
        transport = self.audio.transport
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
        self.talkback = []


class HomeKitCamerasPlugin(ScryptedDeviceBase, DeviceProvider):
    def __init__(self) -> None:
        super().__init__()
        self.cameras: dict[str, HomeKitCamera] = {}
        self.controller: Controller | None = None
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
        devices = [
            {
                "nativeId": path.stem,
                "name": path.stem,
                "type": ScryptedDeviceType.Camera.value,
                "interfaces": [
                    ScryptedInterface.VideoCamera.value,
                    ScryptedInterface.MotionSensor.value,
                    ScryptedInterface.Intercom.value,
                ],
                "info": {"manufacturer": "HomeKit"},
            }
            for path in sorted(PAIRINGS_DIR.glob("*.pairing"))
        ]
        await scrypted_sdk.deviceManager.onDevicesChanged({"devices": devices})

    async def _shutdown(self) -> None:
        # Scrypted sends SIGKILL one second after SIGTERM. A camera left streaming to a
        # controller that vanished can stop answering HAP until it is power cycled.
        ends = [asyncio.ensure_future(camera._end_session()) for camera in self.cameras.values()]
        if ends:
            await asyncio.wait(ends, timeout=0.8)
        os._exit(0)

    async def getDevice(self, nativeId: str) -> HomeKitCamera:
        await self.started
        if nativeId not in self.cameras:
            self.cameras[nativeId] = HomeKitCamera(nativeId, self.controller)
        return self.cameras[nativeId]

    async def releaseDevice(self, id: str, nativeId: str) -> None:
        camera = self.cameras.pop(nativeId, None)
        if camera:
            await camera._end_session()


def create_scrypted_plugin():
    return HomeKitCamerasPlugin()
