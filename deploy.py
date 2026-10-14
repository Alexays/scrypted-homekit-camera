#!/usr/bin/env python3
"""Build the plugin zip and deploy it to a Scrypted server (run on the server's Mac).

Usage: ./deploy.py [host:port]   (default 127.0.0.1:10443)
Needs the aiohomekit camera-streaming fork at $AIOHOMEKIT (default ../aiohomekit),
`npm install` for the SDK, Homebrew fdk-aac, and `npx scrypted login <host>` once.
"""
import base64
import json
import os
import ssl
import subprocess
import sys
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:10443"
AIOHOMEKIT = Path(os.environ.get("AIOHOMEKIT", ROOT.parent / "aiohomekit")) / "aiohomekit"
SDK = ROOT / "node_modules/@scrypted/sdk/types/scrypted_python/scrypted_sdk"
BREW = Path("/opt/homebrew")

dist = ROOT / "dist"
dist.mkdir(exist_ok=True)
encoder = dist / "eld_encoder"
# fdk-aac is linked statically so the plugin does not depend on Homebrew at runtime; it is C++.
subprocess.run(["cc", "-O2", "-mmacosx-version-min=26.0", "-I", BREW / "include", ROOT / "native/eld_encoder.c", BREW / "lib/libfdk-aac.a", "-lc++", "-o", encoder], check=True)

zip_path = dist / "plugin.zip"
with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
    for base, prefix in ((ROOT / "src", ""), (AIOHOMEKIT, "aiohomekit/"), (SDK, "scrypted_sdk/")):
        for path in sorted(base.rglob("*")):
            if path.is_file() and "__pycache__" not in path.parts:
                z.write(path, prefix + str(path.relative_to(base)))
    z.write(encoder, "eld_encoder")
    z.write(ROOT / "README.md", "README.md")

login = json.loads((Path.home() / ".scrypted/login.json").read_text())[HOST]
auth = "Basic " + base64.b64encode(f"{login['username']}:{login['token']}".encode()).decode()
package = json.loads((ROOT / "package.json").read_text())
context = ssl._create_unverified_context()  # Scrypted serves a self-signed certificate
for action, body, content_type in (
    ("setup", json.dumps(package).encode(), "application/json"),
    ("deploy", zip_path.read_bytes(), "application/zip"),
):
    request = urllib.request.Request(
        f"https://{HOST}/web/component/script/{action}?npmPackage={package['name']}",
        data=body, method="POST", headers={"Authorization": auth, "Content-Type": content_type},
    )
    with urllib.request.urlopen(request, context=context, timeout=30) as response:
        print(action, response.status)
