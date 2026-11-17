"""Subscribe to and replay one Open Push Channel conversation."""

import argparse
import asyncio
import json
import os
import sys

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException


async def wait_for(socket: ClientConnection, expected: str) -> dict:
    async with asyncio.timeout(10):
        while True:
            frame = json.loads(await socket.recv())
            if frame["type"] == "error":
                raise ValueError(frame["message"])
            if frame["type"] == expected:
                return frame


async def listen(args: argparse.Namespace, token: str) -> None:
    async with connect(args.gateway) as socket:
        await socket.send(json.dumps({
            "type": "register", "token": token, "protocolVersion": 1,
        }))
        registered = await wait_for(socket, "registered")
        if registered.get("protocolVersion") != 1:
            raise ValueError("Gateway did not confirm protocol version 1")
        await socket.send(json.dumps({
            "type": "session.start",
            "sessionId": args.session,
            "runtimeId": args.runtime,
            "agentId": args.agent,
            "afterCursor": args.after_cursor,
        }))
        session = await wait_for(socket, "session.started")
        target = (session["sessionId"], session["runtimeId"], session["agentId"])
        if target != (args.session, args.runtime, args.agent):
            raise ValueError("Gateway confirmed a different session binding")

        cursor = args.after_cursor
        count = 0
        while args.count is None or count < args.count:
            frame = json.loads(await socket.recv())
            if frame["type"] == "error":
                raise ValueError(frame["message"])
            if (
                frame.get("version") != 1
                or frame.get("audience") != "conversation"
                or (frame.get("conversationId"), frame.get("runtimeId"), frame.get("agentId")) != target
            ):
                continue

            # The gateway replays the boundary cursor inclusively.
            next_cursor = frame["cursor"]
            if next_cursor <= cursor:
                continue
            print(json.dumps(frame), flush=True)
            cursor = next_cursor
            count += 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", default="ws://127.0.0.1:3001", help="trusted gateway WebSocket URL")
    parser.add_argument("--session", required=True, help="conversation ID")
    parser.add_argument("--runtime", default="openclaw", help="runtime ID")
    parser.add_argument("--agent", default="main", help="agent ID")
    parser.add_argument("--after-cursor", type=int, default=0, help="last processed cursor (default: 0)")
    parser.add_argument("--count", type=int, help="stop after this many events")
    args = parser.parse_args()
    if not all(value and value == value.strip() for value in (args.session, args.runtime, args.agent)):
        parser.error("--session, --runtime and --agent must be nonempty and have no surrounding spaces")
    if not 0 <= args.after_cursor <= 2**53 - 1:
        parser.error("--after-cursor must be between 0 and 2^53 - 1")
    if args.count is not None and args.count < 1:
        parser.error("--count must be positive")
    token = os.environ.get("PUSH_GATEWAY_CLIENT_TOKEN")
    if not token:
        parser.error("set PUSH_GATEWAY_CLIENT_TOKEN to the gateway's client token")

    try:
        asyncio.run(listen(args, token))
    except KeyboardInterrupt:
        sys.exit(130)
    except TimeoutError:
        parser.exit(1, "Timed out waiting for the gateway to register or open the session\n")
    except (OSError, WebSocketException, ValueError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
