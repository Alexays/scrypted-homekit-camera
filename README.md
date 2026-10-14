# HomeKit Accessory Cameras for Scrypted

Brings cameras paired natively over HomeKit (HAP), such as the Aqara E1 or G4,
into Scrypted. The plugin is the camera's HomeKit controller:

- live H.264 video and AAC-ELD audio, relayed as SRTP to Scrypted's ffmpeg
  (pair it with Rebroadcast for prebuffering and HomeKit Secure Video)
- the camera's native motion sensor
- two-way audio (Intercom), encoded as 480-sample AAC-ELD frames
- one stream per RTP stream service the camera offers (the E1 has 1080p and 720p)
- for Aqara pan/tilt cameras, the presets saved in the Aqara Home app, and a
  privacy switch that turns the camera away unless someone watches live

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

## Pan/tilt and privacy

Aqara cameras such as the E1 take pan/tilt moves only from the Aqara cloud:
the Aqara Home app itself sends them there, never over the local network. The
plugin does the same, with the presets saved in the app, once you log in to
your Aqara account in the plugin settings (the password only serves the login;
the session token is kept). Request signing follows
[Aqara-LANLink](https://github.com/copystring/Aqara-LANLink) (MIT).

Each camera gets a `<Name> Privacy` switch. While it is on, the camera faces
its Privacy Preset, unless someone watches the Live Stream; then, and while
the switch is off, it faces its Watch Preset. Keep the Live Stream out of
Rebroadcast's prebuffered streams (prebuffer and record the other one for
HomeKit Secure Video), so its session only runs while a viewer is there.
Drive the switch from presence automations in the Home app.

## Doorbells, events and talk-back from other apps

A camera with a HomeKit doorbell service becomes a Scrypted doorbell: a press
turns its binary state on, which the Scrypted HomeKit plugin rings with.

Plugin settings, **Event Webhook**: each ring and motion change is posted as
`{"camera", "event": "ring" | "motion", "state"}`, without delaying the
event. A Home Assistant webhook trigger feeding template binary sensors turns
them into entities.

Camera settings, **Talk-back Port**: a TCP client sending raw s16le 16 kHz
mono PCM talks through the camera for as long as it stays connected. The PCM
goes straight to the AAC-ELD encoder, with no ffmpeg in the path.

`GET http://<scrypted>:11080/endpoint/scrypted-homekit-camera/public/watch?camera=<name>&watching=1|0`
lets a viewer outside HomeKit (a dashboard) hold a watch lease: renew it every
few seconds while its live view is open and release it on close, and a
pan/tilt camera turns right away instead of waiting for the stream to wind
down. An unrenewed lease lapses after 15 s.

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
