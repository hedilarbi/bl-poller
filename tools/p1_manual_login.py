"""
Manual P1 login — gives the poller a fresh access_token + refresh_token when the automatic
Playwright login is blocked by Blacklane's human verification (captcha).

A human completes the login (captcha included) in a visible browser; the script captures the
OAuth code from the app redirect, exchanges it, and stores the tokens on the EC2. From then on
the poller renews the token by itself with the refresh_token (no login page, no captcha).

Run on a computer with a screen, from the bl-poller folder (uses its .env):
    python3 -m playwright install chromium        # once
    python3 tools/p1_manual_login.py --bot-id bl_oussema_1_bot --telegram-id 6481416396

If this computer cannot reach the EC2 internal API, the tokens are written to p1_tokens.json;
copy that file to the VPS and run there:
    venv/bin/python tools/p1_manual_login.py --bot-id ... --telegram-id ... --from-file p1_tokens.json
"""
import argparse
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests  # noqa: E402

from poller_core.config import MOBILE_CLIENT_ID  # noqa: E402  (first: loads .env)
from poller_core import ec2_api  # noqa: E402  (reads EC2_API_URL at import)
from poller_core.p1_auth import (  # noqa: E402
    _jwt_claim,
    exchange_pkce_code,
    extract_pkce_code,
    new_pkce_login_url,
)

LOGIN_TIMEOUT_S = 600
TOKENS_FILE = "p1_tokens.json"


async def human_login() -> tuple:
    """Open a visible browser on the Blacklane login and wait for the human to finish.
    Returns (code, code_verifier)."""
    from playwright.async_api import async_playwright

    authorize_url, code_verifier = new_pkce_login_url()
    found: dict = {}

    def grab(url):
        code = extract_pkce_code(url)
        if code:
            found["code"] = code

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        page.on("request", lambda req: grab(req.url))
        page.on("framenavigated", lambda frame: grab(frame.url))
        # Auth0 sends the app redirect as a 302 Location header: catch it there too.
        page.on("response", lambda resp: grab(resp.headers.get("location")))
        await page.goto(authorize_url)
        print("👉 Log in in the browser window (captcha included). Waiting up to 10 min...")
        for _ in range(LOGIN_TIMEOUT_S * 2):
            if found.get("code"):
                break
            await asyncio.sleep(0.5)
        await browser.close()

    if not found.get("code"):
        raise SystemExit("❌ No authorization code received (login not completed within 10 min).")
    return found["code"], code_verifier


def save_to_ec2(bot_id: str, telegram_id: int, access: str, refresh: str) -> bool:
    """Store token + refresh_token for this user (mobile headers are kept as they are)."""
    payload = {
        "token": access,
        "mobile_headers": None,
        "auth_meta": {"refresh_token": refresh, "client_id": MOBILE_CLIENT_ID},
    }
    try:
        r = requests.post(
            ec2_api._url(f"/internal/poller/user/{bot_id}/{telegram_id}/token"),
            json=payload,
            headers=ec2_api._headers(),
            timeout=10,
        )
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"⚠️ Could not reach the EC2 internal API from this machine: {type(e).__name__}: {e}")
        return False


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bot-id", required=True)
    ap.add_argument("--telegram-id", required=True, type=int)
    ap.add_argument("--from-file", help=f"save tokens from a {TOKENS_FILE} made on another machine")
    args = ap.parse_args()

    if not MOBILE_CLIENT_ID:
        raise SystemExit("❌ BL_MOBILE_CLIENT_ID is missing from .env")

    if args.from_file:
        with open(args.from_file) as f:
            data = json.load(f)
        access, refresh = data["token"], data["refresh_token"]
    else:
        code, verifier = asyncio.run(human_login())
        ok, access, refresh, note = exchange_pkce_code(code, verifier)
        if not ok:
            raise SystemExit(f"❌ Code exchange failed: {note}")
        if not refresh:
            raise SystemExit("❌ Blacklane returned no refresh_token — nothing durable to store.")

    print(f"✅ Tokens obtained for chauffeur_id={_jwt_claim(access, 'chauffeur_id')} sub={_jwt_claim(access, 'sub')}")
    if save_to_ec2(args.bot_id, args.telegram_id, access, refresh):
        print(f"✅ Saved on the EC2 for {args.bot_id}/{args.telegram_id}. The poller picks it up within a few seconds.")
        return
    if args.from_file:
        raise SystemExit("❌ Not saved.")
    with open(TOKENS_FILE, "w") as f:
        json.dump({"token": access, "refresh_token": refresh}, f)
    os.chmod(TOKENS_FILE, 0o600)
    print(
        f"💾 Tokens written to {TOKENS_FILE}. Copy it to the VPS (/opt/chauffeur/bl-poller) and run there:\n"
        f"   venv/bin/python tools/p1_manual_login.py --bot-id {args.bot_id} "
        f"--telegram-id {args.telegram_id} --from-file {TOKENS_FILE}\n"
        f"   then delete the file (it contains your login tokens)."
    )


if __name__ == "__main__":
    main()
