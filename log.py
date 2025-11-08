"""
log.py
------
Helper utilities for handling Raft logs.

This module is deliberately decoupled from raft_node.py
to avoid circular dependencies.
"""

from typing import List, Any


def get_term_at(logs: List[Any], index: int) -> int:
    """
    Returns the term for the given log index.

    Args:
        logs: List of log entries (each entry must have a .term attribute).
        index: 1-based log index (0 = no entry)

    Returns:
        int: The term of the log at that index, or 0 if none.
    """
    if index <= 0:
        return 0
    if index <= len(logs):
        return getattr(logs[index - 1], "term", 0)
    return -1
