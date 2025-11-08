"""
fsm.py
------
Implements the Finite State Machine for Raft.
This is a simple, in-memory key-value store.
"""

from typing import Dict, Any


class KVStoreFSM:
    """A simple key-value state machine for Raft."""

    def __init__(self):
        self.kv: Dict[str, str] = {}
        self._last_applied = 0

    def apply(self, entry: Any):
        """Apply a Raft log entry to the key-value store."""
        cmd = entry.command if hasattr(entry, "command") else entry.get("command")
        if cmd.get("op") == "SET":
            self.kv[str(cmd["key"])] = str(cmd["value"])
        self._last_applied = entry.index if hasattr(entry, "index") else entry.get("index", self._last_applied)

    def last_applied_index(self) -> int:
        return self._last_applied