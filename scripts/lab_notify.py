"""Posts an alert to Slack, and to email too if you set it up. With neither, it prints the alert.

Source: ~/de_lab/lab_kit/lab_notify.py. new_project.sh copies it into scripts/ of each repo,
so the repo and its CI run without the kit. It uses only the standard library and never raises:
an alert that crashes the pipeline it watches is worse than a lost alert.
Email turns on when SMTP_HOST and ALERT_EMAIL_TO are both set in .env.
"""

import json
import os
import smtplib
import urllib.request
from email.message import EmailMessage


def _slack(text, url):
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps({"text": text}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return "posted" if resp.status == 200 else "failed"
    except Exception as exc:  # noqa: BLE001 - see the module docstring
        print(f"[slack failed: {type(exc).__name__}] " + text)
        return "failed"


def _email(title, text, severity):
    host, to = os.environ.get("SMTP_HOST", "").strip(), os.environ.get("ALERT_EMAIL_TO", "").strip()
    if not host or not to:
        return
    try:
        msg = EmailMessage()
        msg["Subject"] = f"[{severity}] {title}"
        msg["From"] = os.environ.get("SMTP_FROM") or "lab@localhost"
        msg["To"] = to
        msg.set_content(text)
        with smtplib.SMTP(host, int(os.environ.get("SMTP_PORT") or "25"), timeout=10) as smtp:
            if os.environ.get("SMTP_USER"):
                smtp.starttls()
                smtp.login(os.environ["SMTP_USER"], os.environ.get("SMTP_PASSWORD", ""))
            smtp.send_message(msg)
        print(f"email: sent to {to}")
    except Exception as exc:  # noqa: BLE001 - see the module docstring
        print(f"[email failed: {type(exc).__name__}] {title}")


def notify(title, lines=(), severity="ERROR"):
    """Returns the Slack result: "posted", "printed" or "failed". Email is extra and never changes it."""
    text = f"[{severity}] {title}" + "".join(f"\n- {ln}" for ln in lines)
    url = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    if url:
        result = _slack(text, url)
    else:
        print("[slack off] " + text)
        result = "printed"
    _email(title, text, severity)
    return result


if __name__ == "__main__":
    print(notify("lab_notify test", ["this is a test alert"], "INFO"))