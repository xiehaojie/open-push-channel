"""Send a notification or request a task for an existing gateway conversation."""

import argparse
import http.client
import json
import os
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Keep the authenticated POST on the explicitly configured endpoint.
        return None


def send_request(
    *, gateway: str, token: str, mode: str, client: str, session: str,
    agent: str, event: str, content: str, runtime: str | None = None,
) -> dict[str, object] | None:
    """Return a task receipt or None for a notification; raise on invalid input or failure."""
    if mode not in ("send", "task"):
        raise ValueError("mode must be send or task")
    if any(not isinstance(value, str) or not value.strip() for value in (client, session, agent, event, content)):
        raise ValueError("--client, --session, --agent, --event and --content must be nonempty")
    if runtime is not None and (not isinstance(runtime, str) or not runtime.strip()):
        raise ValueError("--runtime must be nonempty when provided")
    if not isinstance(token, str) or not token.strip() or any(ord(char) < 32 or ord(char) == 127 for char in token):
        raise ValueError("set PUSH_CHANNEL_PUSH_TOKEN to a valid gateway server token")
    if not isinstance(gateway, str):
        raise ValueError("--gateway must be an HTTP or HTTPS base URL without credentials, query or fragment")
    try:
        base = urllib.parse.urlsplit(gateway)
        valid = (
            gateway == gateway.strip()
            and not any(ord(char) < 32 or ord(char) == 127 for char in gateway)
            and base.scheme in ("http", "https")
            and base.hostname
            and base.username is None
            and base.password is None
            and not base.query
            and not base.fragment
        )
        base.port  # Validate a supplied port before opening a connection.
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("--gateway must be an HTTP or HTTPS base URL without credentials, query or fragment")

    payload = {
        "clientId": client, "sessionId": session, "agentId": agent,
        "eventId": event, "content": content,
    }
    if runtime is not None:
        payload["runtimeId"] = runtime
    route, expected = ("api/send", 200) if mode == "send" else ("api/tasks", 202)
    # Ignore environment proxies and redirects: keep credentials on the trusted endpoint.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        endpoint = urllib.parse.urlunsplit((base.scheme, base.netloc, base.path.rstrip("/") + "/" + route, "", ""))
        request = urllib.request.Request(
            endpoint, data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
            method="POST",
        )
        with opener.open(request, timeout=10) as response:
            if response.status != expected:
                raise RuntimeError(f"POST /{route} returned HTTP {response.status} (expected {expected})")
            if mode == "send":
                return None
            receipt = json.load(response)
            if not isinstance(receipt, dict) or any(
                not isinstance(receipt.get(field), str) or not receipt[field].strip()
                for field in ("runId", "status")
            ) or not isinstance(receipt.get("duplicate"), bool):
                raise RuntimeError("Invalid task receipt: expected runId, status and duplicate")
            return {field: receipt[field] for field in ("runId", "status", "duplicate")}
    except urllib.error.HTTPError as error:
        error.close()
        raise RuntimeError(f"POST /{route} returned HTTP {error.code} (expected {expected})") from None
    except (OSError, urllib.error.URLError, ValueError, http.client.HTTPException):
        raise RuntimeError("Gateway request failed or returned invalid JSON") from None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("send", "task"), default="send")
    parser.add_argument("--gateway", default="http://127.0.0.1:3001")
    for field in ("client", "session", "agent", "event", "content"):
        parser.add_argument(f"--{field}", required=True)
    parser.add_argument("--runtime", help="bound runtime ID (optional)")
    args = parser.parse_args()
    try:
        receipt = send_request(**vars(args), token=os.environ.get("PUSH_CHANNEL_PUSH_TOKEN", "").strip())
    except ValueError as error:
        parser.error(str(error))
    except RuntimeError as error:
        parser.exit(1, f"{error}\n")
    else:
        print(json.dumps(receipt) if receipt is not None else "Notification accepted (HTTP 200)")


if __name__ == "__main__":
    main()
