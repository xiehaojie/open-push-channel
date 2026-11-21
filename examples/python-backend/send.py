"""Send a notification or request a task for an existing gateway conversation."""

import argparse
import json
import os
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Keep the authenticated POST on the explicitly configured endpoint.
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("send", "task"), default="send")
    parser.add_argument("--gateway", default="http://127.0.0.1:3001")
    for field in ("client", "session", "agent", "event", "content"):
        parser.add_argument(f"--{field}", required=True)
    parser.add_argument("--runtime", help="bound runtime ID (optional)")
    args = parser.parse_args()
    if any(not value.strip() for value in (args.client, args.session, args.agent, args.event, args.content)):
        parser.error("--client, --session, --agent, --event and --content must be nonempty")
    if args.runtime is not None and not args.runtime.strip():
        parser.error("--runtime must be nonempty when provided")
    token = os.environ.get("PUSH_CHANNEL_PUSH_TOKEN", "").strip()
    if not token or any(ord(char) < 32 or ord(char) == 127 for char in token):
        parser.error("set PUSH_CHANNEL_PUSH_TOKEN to a valid gateway server token")
    try:
        gateway = urllib.parse.urlsplit(args.gateway)
        valid = (
            args.gateway == args.gateway.strip()
            and not any(ord(char) < 32 or ord(char) == 127 for char in args.gateway)
            and gateway.scheme in ("http", "https")
            and gateway.hostname
            and gateway.username is None
            and gateway.password is None
            and not gateway.query
            and not gateway.fragment
        )
        gateway.port  # Validate a supplied port before opening a connection.
    except ValueError:
        valid = False
    if not valid:
        parser.error("--gateway must be an HTTP or HTTPS base URL without credentials, query or fragment")

    payload = {
        "clientId": args.client, "sessionId": args.session, "agentId": args.agent,
        "eventId": args.event, "content": args.content,
    }
    if args.runtime is not None:
        payload["runtimeId"] = args.runtime
    route, expected = ("api/send", 200) if args.mode == "send" else ("api/tasks", 202)
    endpoint = urllib.parse.urlunsplit((gateway.scheme, gateway.netloc, gateway.path.rstrip("/") + "/" + route, "", ""))
    request = urllib.request.Request(
        endpoint, data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    # Ignore environment proxies; this example calls the trusted gateway directly.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        with opener.open(request, timeout=10) as response:
            if response.status != expected:
                parser.exit(1, f"POST /{route} returned HTTP {response.status} (expected {expected})\n")
            if args.mode == "send":
                print("Notification accepted (HTTP 200)")
                return
            receipt = json.load(response)
            if not isinstance(receipt, dict) or any(
                not isinstance(receipt.get(field), str) or not receipt[field].strip()
                for field in ("runId", "status")
            ) or not isinstance(receipt.get("duplicate"), bool):
                parser.exit(1, "Invalid task receipt: expected runId, status and duplicate\n")
            print(json.dumps({field: receipt[field] for field in ("runId", "status", "duplicate")}))
    except urllib.error.HTTPError as error:
        error.close()
        parser.exit(1, f"POST /{route} returned HTTP {error.code} (expected {expected})\n")
    except (OSError, urllib.error.URLError, ValueError):
        parser.exit(1, "Gateway request failed or returned invalid JSON\n")


if __name__ == "__main__":
    main()
