#!/usr/bin/env python3
"""
client.py — simple CLI client for interacting with the Raft key-value cluster.
Usage:
    python client.py write <key> <value>     # set key=value
    python client.py read <key>              # read value
    python client.py status                  # show node states
"""

import sys
import json
import requests

# --- cluster configuration ---
NODES = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
    "http://localhost:8004",
    "http://localhost:8005",
]


def get_leader():
    """Find the current leader by querying all nodes."""
    for url in NODES:
        try:
            r = requests.get(f"{url}/admin/status", timeout=1.5)
            if r.status_code == 200:
                data = r.json()
                if data.get("state") == 2:  # leader
                    return url
        except Exception:
            pass
    # fallback: ask any node for its known leader
    for url in NODES:
        try:
            r = requests.get(f"{url}/admin/status", timeout=1.5)
            if r.status_code == 200:
                leader = data.get("leader")
                if leader and leader > 0:
                    leader_url = f"http://localhost:800{leader}"
                    return leader_url
        except Exception:
            pass
    return None


def write_key(key, value):
    leader_url = get_leader()
    if not leader_url:
        print("❌ No leader available")
        return
    try:
        r = requests.post(
            f"{leader_url}/client/write",
            json={"key": key, "value": value},
            timeout=2.5,
        )
        if r.status_code in (200, 307):
            print(json.dumps(r.json(), indent=2))
        else:
            print(f"❌ Error {r.status_code}: {r.text}")
    except Exception as e:
        print(f"❌ Write failed: {e}")


def read_key(key):
    # Try all nodes (read from any)
    for url in NODES:
        try:
            r = requests.get(f"{url}/client/read", params={"key": key}, timeout=1.5)
            if r.status_code == 200:
                print(f"{url} → {r.json()}")
        except Exception:
            pass


def show_status():
    state_map = {0: "Follower", 1: "Candidate", 2: "Leader"}
    for url in NODES:
        try:
            r = requests.get(f"{url}/admin/status", timeout=1.5)
            if r.status_code == 200:
                d = r.json()
                state_name = state_map.get(d["state"], f"Unknown({d['state']})")
                print(
                    #f"{url.split(':')[-1]:>5} | "
                    f"Node ID {d['id']:<2} | Term {d['term']:<3} | "
                    f"{state_name:<9} | Leader Node ID: {d['leader']}"
                )
        except Exception:
            pass


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)

    cmd = sys.argv[1].lower()

    if cmd == "write" and len(sys.argv) == 4:
        write_key(sys.argv[2], sys.argv[3])
    elif cmd == "read" and len(sys.argv) == 3:
        read_key(sys.argv[2])
    elif cmd == "status":
        show_status()
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
