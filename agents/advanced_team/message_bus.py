#!/usr/bin/env python3
"""Shared Message Bus — append-only JSONL file simulating MCP protocol.

Each message is a JSON object on its own line with:
  - id: unique message ID (monotonic counter)
  - timestamp: ISO 8601 UTC
  - sender: agent name that created the message
  - recipient: agent name that should process it (or "broadcast")
  - msg_type: message category (research_memo, strategy_proposal, risk_verdict, etc.)
  - payload: arbitrary dict with message-specific data
  - cycle: which DFA cycle this belongs to

Thread-safe via file locking (fcntl on macOS/Linux).
"""

import fcntl
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

BUS_FILE = str(Path(__file__).parent / "message_bus.jsonl")

# Message counter file for monotonic IDs
_COUNTER_FILE = str(Path(__file__).parent / ".msg_counter")


def _next_id():
    """Get next monotonic message ID (file-based for process safety)."""
    try:
        if os.path.exists(_COUNTER_FILE):
            with open(_COUNTER_FILE, "r") as f:
                val = int(f.read().strip())
        else:
            val = 0
    except (ValueError, OSError):
        val = 0
    val += 1
    with open(_COUNTER_FILE, "w") as f:
        f.write(str(val))
    return val


class MessageBus:
    """Append-only JSONL message bus with file-level locking."""

    def __init__(self, bus_file=None):
        self.bus_file = bus_file or BUS_FILE
        # Ensure the file exists
        Path(self.bus_file).touch(exist_ok=True)

    def publish(self, sender, recipient, msg_type, payload, cycle=0):
        """Append a message to the bus. Returns the message ID."""
        msg_id = _next_id()
        message = {
            "id": msg_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sender": sender,
            "recipient": recipient,
            "msg_type": msg_type,
            "payload": payload,
            "cycle": cycle,
        }
        line = json.dumps(message, separators=(",", ":")) + "\n"

        with open(self.bus_file, "a") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                f.write(line)
                f.flush()
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

        return msg_id

    def read_for(self, recipient, since_id=0, msg_type=None):
        """Read messages addressed to a specific recipient (or broadcast).

        Args:
            recipient: agent name to filter for
            since_id: only return messages with id > since_id
            msg_type: optional filter by message type

        Returns:
            list of message dicts, sorted by id ascending
        """
        messages = []
        if not os.path.exists(self.bus_file):
            return messages

        with open(self.bus_file, "r") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            try:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    if msg.get("id", 0) <= since_id:
                        continue

                    if msg.get("recipient") not in (recipient, "broadcast"):
                        continue

                    if msg_type and msg.get("msg_type") != msg_type:
                        continue

                    messages.append(msg)
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

        messages.sort(key=lambda m: m.get("id", 0))
        return messages

    def read_latest(self, recipient, msg_type=None, count=1):
        """Read the N most recent messages for a recipient."""
        all_msgs = self.read_for(recipient, since_id=0, msg_type=msg_type)
        return all_msgs[-count:] if all_msgs else []

    def read_cycle(self, cycle):
        """Read all messages from a specific DFA cycle."""
        messages = []
        if not os.path.exists(self.bus_file):
            return messages

        with open(self.bus_file, "r") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            try:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if msg.get("cycle") == cycle:
                        messages.append(msg)
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

        return messages

    def get_max_id(self):
        """Get the highest message ID on the bus."""
        max_id = 0
        if not os.path.exists(self.bus_file):
            return max_id

        with open(self.bus_file, "r") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            try:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                        msg_id = msg.get("id", 0)
                        if msg_id > max_id:
                            max_id = msg_id
                    except json.JSONDecodeError:
                        continue
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

        return max_id

    def truncate(self, keep_last_n=500):
        """Truncate bus to keep only the last N messages (garbage collection)."""
        if not os.path.exists(self.bus_file):
            return

        with open(self.bus_file, "r+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                lines = f.readlines()
                if len(lines) <= keep_last_n:
                    return
                keep = lines[-keep_last_n:]
                f.seek(0)
                f.truncate()
                f.writelines(keep)
                f.flush()
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)
