"""Aqara Home cloud API, called the way the Aqara Home app calls it: login and pan/tilt presets.

Aqara cameras such as the E1 accept pan/tilt moves only from the cloud; the app
itself sends no local command for them. The endpoints and the request signing
come from Aqara-LANLink (MIT License, Dan Gibson), which reverse-engineered
them from the Android app.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import time
import urllib.request
import uuid

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding

SERVERS = {
    "EU": "https://rpc-ger.aqara.com",
    "US": "https://aiot-rpc-usa.aqara.com",
    "CN": "https://aiot-rpc.aqara.cn",
    "RU": "https://rpc-ru.aqara.com",
    "KR": "https://rpc-kr.aqara.com",
    "AU": "https://rpc-au.aqara.com",
}
# The app's (appid, appkey) signing pairs; the server answers code 106 to a pair it does not take.
SIGNING = (
    ("7be1984f0556276133336839", "Jddz01kIORDYrBzqGYgpUXKBnIHfW8E3"),
    ("94549908487478b220992a70", "euGhPe2rcmxwculATNj45eEtnd50zp0I"),
    ("94549908487478b220992a70", "Jddz01kIORDYrBzqGYgpUXKBnIHfW8E3"),
)
# Encrypts the password hash at login, extracted from the app.
PUBLIC_KEY = b"""-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCG46slB57013JJs4Vvj5cVyMpR
9b+B2F+YJU6qhBEYbiEmIdWpFPpOuBikDs2FcPS19MiWq1IrmxJtkICGurqImRUt
4lP688IWlEmqHfSxSRf2+aH0cH8VWZ2OaZn5DWSIHIPBF2kxM71q8stmoYiV0oZs
rZzBHsMuBwA4LQdxBwIDAQAB
-----END PUBLIC KEY-----"""


class AqaraCloudError(Exception):
    pass


class AqaraCloud:
    def __init__(self, region: str, session: dict | None = None) -> None:
        self.region = region
        self.session = session  # {"userId", "token", "phoneId"} after login

    def _request(self, method: str, path: str, sign_source: str, body: str | None, signing) -> dict:
        appid, appkey = signing
        token = self.session["token"] if self.session else None
        nonce = uuid.uuid4().hex
        ts = str(round(time.time() * 1000))
        # Never log this string: it holds the session token.
        source = f"Appid={appid}&Nonce={nonce}&Time={ts}" + (f"&Token={token}" if token else "") + f"&{sign_source}&{appkey}"
        headers = {
            "User-Agent": "okhttp/4.12.0", "App-Version": "6.1.6", "Sys-Type": "1", "Lang": "en",
            "Phone-Model": "Scrypted", "PhoneId": self.session["phoneId"] if self.session else str(uuid.uuid4()).upper(),
            "Area": self.region, "Appid": appid, "Nonce": nonce, "Time": ts, "Content-Type": "application/json",
            "Sign": hashlib.md5(source.encode()).hexdigest(),
        }
        if token:
            headers["Token"] = token
        request = urllib.request.Request(SERVERS[self.region] + path, data=body.encode() if body else None, method=method, headers=headers)
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.loads(response.read())

    async def _call(self, method: str, path: str, sign_source: str, body: str | None = None):
        for signing in SIGNING:
            reply = await asyncio.to_thread(self._request, method, path, sign_source, body, signing)
            if str(reply.get("code")) != "106":
                break
        if str(reply.get("code")) != "0":
            raise AqaraCloudError(f"{path}: code {reply.get('code')} {reply.get('message')}")
        return reply.get("result")

    async def _get(self, path: str, **params: str):
        # The signature covers the query string with its keys sorted.
        query = "&".join(f"{key}={params[key]}" for key in sorted(params))
        return await self._call("GET", f"{path}?{query}", query)

    async def login(self, email: str, password: str) -> dict:
        key = serialization.load_pem_public_key(PUBLIC_KEY)
        encrypted = base64.b64encode(key.encrypt(hashlib.md5(password.encode()).hexdigest().encode(), padding.PKCS1v15())).decode()
        # The server checks the signature against json.dumps' default spacing.
        body = json.dumps({"account": email, "encryptType": 2, "password": encrypted})
        self.session = {"phoneId": str(uuid.uuid4()).upper(), "token": None}
        try:
            result = await self._call("POST", "/app/v1.0/lumi/user/login", body, body)
        except Exception:
            self.session = None
            raise
        self.session = {"userId": result["userId"], "token": result["token"], "phoneId": self.session["phoneId"]}
        return self.session

    async def ptz_presets(self, did: str) -> dict[str, int]:
        """Preset name -> preset id, as saved in the Aqara Home app."""
        positions = await self._get("/app/v1.0/lumi/devex/camera/ptz/position/list", did=did)
        return {p["name"].strip(): p["id"] for p in positions or []}

    async def run_ptz_preset(self, did: str, preset_id: int) -> None:
        await self._get("/app/v1.0/lumi/devex/camera/ptz/position/run", did=did, posId=str(preset_id))
