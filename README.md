# HomeKit Accessory Cameras for Scrypted

Brings cameras paired natively over HomeKit (HAP), such as the Aqara E1 or G4,
into Scrypted. The plugin is the camera's HomeKit controller:

- live H.264 video and AAC-ELD audio, relayed as SRTP to Scrypted's ffmpeg
  (pair it with Rebroadcast for prebuffering and HomeKit Secure Video)
- the camera's native motion sensor
- two-way audio (Intercom), encoded as 480-sample AAC-ELD frames

A HomeKit accessory accepts a single controller, so a camera used here is not
in the Apple Home app directly; re-expose it with the Scrypted HomeKit plugin.

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
