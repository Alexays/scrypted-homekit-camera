"""Pairs an unpaired HomeKit camera and writes the pairing file the plugin loads.

Usage: pair.py <accessory id, e.g. 7C:F3:3C:F1:E3:CB> <name>   (setup code on stdin)
The pairing lands in ~/.config/homekit-cameras/<name>.pairing; restart the plugin to pick it up.
"""
import asyncio
import getpass
import json
import re
import sys
from pathlib import Path

from aiohomekit import Controller
from aiohomekit.characteristic_cache import CharacteristicCacheMemory
from aiohomekit.zeroconf import HAP_TYPE_TCP, HAP_TYPE_UDP, ZeroconfServiceListener
from zeroconf.asyncio import AsyncServiceBrowser, AsyncZeroconf

PAIRINGS_DIR = Path.home() / ".config" / "homekit-cameras"


async def main(accessory_id: str, name: str, code: str) -> None:
    target = PAIRINGS_DIR / f"{name}.pairing"
    if target.exists():
        sys.exit(f"{target} exists already")
    zeroconf = AsyncZeroconf()
    AsyncServiceBrowser(zeroconf.zeroconf, [HAP_TYPE_TCP, HAP_TYPE_UDP], listener=ZeroconfServiceListener())
    async with Controller(async_zeroconf_instance=zeroconf, char_cache=CharacteristicCacheMemory()) as controller:
        discovery = await controller.async_find(accessory_id.lower(), timeout=30)
        print(f"pairing {discovery.description.name} ({discovery.description.model})")
        finish = await discovery.async_start_pairing(name)
        pairing = await finish(code)
        PAIRINGS_DIR.mkdir(parents=True, exist_ok=True)
        target.touch(mode=0o600)
        target.write_text(json.dumps(pairing.pairing_data))
        print(f"paired, saved {target}")
    await zeroconf.async_close()


code = getpass.getpass("HomeKit setup code: ") if sys.stdin.isatty() else sys.stdin.readline()
digits = re.sub(r"\D", "", code)  # labels print it with or without dashes; HAP wants XXX-XX-XXX
asyncio.run(main(sys.argv[1], sys.argv[2], f"{digits[:3]}-{digits[3:5]}-{digits[5:]}"))
