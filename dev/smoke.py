"""Runs the plugin against a real camera, outside Scrypted.

Usage: .venv/bin/python dev/smoke.py <pairing file> <eld_encoder binary> [talk-back audio file]

Stands in for the Scrypted SDK, then checks: the stream decodes (video and
audio), the session ends by itself once its consumer exits, and talk-back
plays the audio file on the camera speaker.
"""
import asyncio
import enum
import shutil
import subprocess
import sys
import tempfile
import time
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FFMPEG = shutil.which("ffmpeg")


class Storage(dict):
    def getItem(self, key):
        return self.get(key)

    def setItem(self, key, value):
        self[key] = value


class ScryptedDeviceBase:
    name = "camera"

    def __init__(self, nativeId=None):
        self.nativeId = nativeId
        self.storage = Storage()

    def __setattr__(self, name, value):
        if name == "motionDetected":
            print(f"[sdk] motionDetected = {value}")
        super().__setattr__(name, value)


class MediaManager:
    async def createFFmpegMediaObject(self, ffmpeg_input):
        return ffmpeg_input

    async def convertMediaObjectToJSON(self, media, mime_type):
        return media

    async def getFFmpegPath(self):
        return FFMPEG


class DeviceManager:
    async def onDevicesChanged(self, manifest):
        print(f"[sdk] devices: {[d['nativeId'] for d in manifest['devices']]}")

    async def onDeviceDiscovered(self, device):
        print(f"[sdk] device updated: {device['nativeId']} ({device['type']})")

    def getDeviceStorage(self, nativeId):
        return None  # like the runtime, for a device it has never seen


sdk = types.ModuleType("scrypted_sdk")
sdk.ScryptedDeviceBase = ScryptedDeviceBase
sdk.mediaManager = MediaManager()
sdk.deviceManager = DeviceManager()
sdk_types = types.ModuleType("scrypted_sdk.types")
for name in ("BinarySensor", "DeviceProvider", "HttpRequestHandler", "Intercom", "MediaObject", "MotionSensor", "OnOff", "PanTiltZoom", "Settings", "VideoCamera"):
    setattr(sdk_types, name, type(name, (), {}))
sdk_types.ScryptedDeviceType = enum.Enum("ScryptedDeviceType", {"Camera": "Camera", "Doorbell": "Doorbell", "Switch": "Switch"})
sdk_types.ScryptedInterface = enum.Enum(
    "ScryptedInterface", {n: n for n in ("VideoCamera", "MotionSensor", "BinarySensor", "Intercom", "PanTiltZoom", "Settings", "OnOff")}
)
sdk_types.ScryptedMimeTypes = enum.Enum("ScryptedMimeTypes", {"FFmpegInput": "x-scrypted/x-ffmpeg-input"})
sys.modules["scrypted_sdk"] = sdk
sys.modules["scrypted_sdk.types"] = sdk_types
sys.path.insert(0, str(ROOT / "src"))

import main  # noqa: E402


async def wait_for(condition, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise TimeoutError(what)
        await asyncio.sleep(0.5)


async def run(pairing: Path, encoder: Path, talk: Path | None) -> None:
    pairings = Path(tempfile.mkdtemp())
    shutil.copy(pairing, pairings / "camera.pairing")
    main.PAIRINGS_DIR = pairings
    main.ELD_ENCODER = encoder

    plugin = main.create_scrypted_plugin()
    await plugin.started
    camera = await plugin.getDevice("camera")
    await camera.ready
    try:
        stream = await camera.getVideoStream()
        out = pairings / "clip.mkv"
        record = await asyncio.create_subprocess_exec(
            FFMPEG, "-hide_banner", "-loglevel", "error", *stream["inputArguments"], "-t", "8", "-c", "copy", str(out)
        )
        await record.wait()
        probe = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "stream=codec_name,width,height,sample_rate,profile", "-of", "compact", str(out)],
            capture_output=True, text=True,
        )
        print(f"[check] recorded streams:\n{probe.stdout.strip()}")
        await wait_for(lambda: camera.session is None, main.CONSUMER_GRACE + 15, "session not ended after consumer exit")
        print("[check] session ended after its consumer exited")

        if talk:
            await camera.startIntercom({"inputArguments": ["-re", "-i", str(talk)]})
            await wait_for(lambda: all(p.returncode is not None for p in camera.talkback), 120, "talk-back did not finish")
            print("[check] talk-back sent, listen to the camera speaker")
            await asyncio.sleep(1)
    finally:
        await plugin.releaseDevice("", "camera")
        await plugin.controller.async_stop()


asyncio.run(run(Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]) if len(sys.argv) > 3 else None))
