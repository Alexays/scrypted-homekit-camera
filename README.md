# HomeKit Accessory Cameras for Scrypted

Brings cameras paired natively over HomeKit (HAP), such as the Aqara E1 or G4,
into Scrypted. The plugin is the camera's HomeKit controller:

- live H.264 video and AAC-ELD audio, relayed as SRTP to Scrypted's ffmpeg
  (pair it with Rebroadcast for prebuffering and HomeKit Secure Video)
- the camera's native motion sensor
- two-way audio (Intercom), encoded as 480-sample AAC-ELD frames

A HomeKit accessory accepts a single controller, so a camera used here is not
in the Apple Home app directly; re-expose it with the Scrypted HomeKit plugin.

## Why

Some cameras speak only HomeKit: no RTSP, no ONVIF, no local API. Their video
is reachable only through Apple Home, so nothing else on the network can use
it. Home Assistant's HomeKit Controller integration fetches snapshots, never
the live stream or the talk-back channel.

Making Scrypted the controller turns such a camera into a regular one:
Rebroadcast prebuffers it, the HomeKit plugin puts it back in Apple Home with
HomeKit Secure Video and two-way audio, and its RTSP rebroadcast feeds Home
Assistant, NVRs or dashboards, all locally.

aiohomekit, the controller library behind Home Assistant, does not implement
the streaming side of HAP, so this relies on a fork that adds it: stream
negotiation, SRTP keys, and the
AAC-ELD framing (480-sample frames, one access unit per packet) the cameras
expect for talk-back.

## Cameras

Each camera is an aiohomekit pairing file named after the device:
`~/.config/homekit-cameras/<Name>.pairing`. Restart the plugin after adding one.

## Build and deploy

Run on the Mac hosting Scrypted:

```sh
brew install fdk-aac
npm install
npx scrypted login 127.0.0.1   # once
./deploy.py
```

The aiohomekit fork with camera streaming
([`camera-streaming` branch](https://github.com/Alexays/aiohomekit/tree/camera-streaming))
is expected at `../aiohomekit`, or at `$AIOHOMEKIT`.

## Development

`dev/smoke.py` runs the plugin outside Scrypted against a real camera:

```sh
uv venv -p 3.10 .venv && uv pip install -p .venv/bin/python -e ../aiohomekit pylibsrtp
.venv/bin/python dev/smoke.py <pairing file> <eld_encoder binary> [talk-back audio file]
```
