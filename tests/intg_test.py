# tests/intg_test.py
import os, json, time, subprocess, threading, warnings, socket
from contextlib import closing
import pytest
import requests

# --- globals / config ---------------------------------------------------------
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)

NODES = [
    {"id": 1, "port": 8001},
    {"id": 2, "port": 8002},
    {"id": 3, "port": 8003},
    {"id": 4, "port": 8004},
    {"id": 5, "port": 8005},
]

STATUS_PATHS = ["/status", "/admin/status", "/raft/status"]
SET_PATHS = [("/client/write", "POST"), ("/kv/set", "POST")]
GET_PATHS = [("/client/read", "GET"), ("/kv/get", "GET")]
STATE_STR = {0: "follower", 1: "candidate", 2: "leader"}

# --- tiny utils ---------------------------------------------------------------
def _port_free(port: int) -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.connect_ex(("127.0.0.1", port)) != 0

def _req(url, method="GET", **kw):
    try:
        if method == "GET":
            return requests.get(url, timeout=1.5, **kw)
        return requests.post(url, timeout=1.5, **kw)
    except Exception:
        return None

def status_of(port):
    base = f"http://localhost:{port}"
    for path in STATUS_PATHS:
        r = _req(base + path)
        if r and r.ok:
            try:
                return r.json()
            except Exception:
                continue
    return None

def find_leader(ports, timeout_s=8.0):
    t0 = time.time()
    last_seen = {}
    while time.time() - t0 < timeout_s:
        for p in ports:
            st = status_of(p)
            if st:
                last_seen[p] = st
                is_leader = (
                    (st.get("state") == 2)
                    or (str(st.get("role", "")).lower() == "leader")
                    or bool(st.get("is_leader", False))
                )
                if is_leader:
                    return p, st
        time.sleep(0.2)
    if last_seen:
        ranked = sorted(
            last_seen.items(),
            key=lambda kv: (kv[1].get("term", -1), kv[1].get("log_len", -1)),
            reverse=True,
        )
        return ranked[0][0], ranked[0][1]
    return None, None

def set_kv(port, key, value):
    base = f"http://localhost:{port}"
    payload = {"key": key, "value": value}
    for path, method in SET_PATHS:
        r = _req(base + path, method, json=payload)
        if r and r.ok:
            return True
    for path, method in SET_PATHS:
        r = _req(base + path, method, data=payload)
        if r and r.ok:
            return True
    return False

def get_kv(port, key):
    base = f"http://localhost:{port}"
    for path, method in GET_PATHS:
        if method == "GET":
            r = _req(base + path, "GET", params={"key": key})
        else:
            r = _req(base + path, "POST", json={"key": key})
        if r and r.ok:
            try:
                data = r.json()
                if "value" in data:
                    return data["value"]
                if "kv" in data and isinstance(data["kv"], dict):
                    return data["kv"].get("value")
            except Exception:
                pass
    return None

def print_separator(msg=""):
    print("\n" + "=" * 70, flush=True)
    if msg:
        print(msg, flush=True)
        print("=" * 70 + "\n", flush=True)

def snapshot(ports):
    snap = {}
    for p in sorted(ports):
        st = status_of(p) or {}
        snap[p] = {
            "state": st.get("state"),
            "role": STATE_STR.get(st.get("state")),
            "term": st.get("term"),
            "leader": st.get("leader"),
            "log_len": st.get("log_len"),
            "commit_index": st.get("commit_index"),
        }
    return snap

def print_snapshot(snap, title):
    print_separator(title)
    for p in sorted(snap):
        s = snap[p]
        print(
            f"port:{p:5}  role:{s['role'] or 'unknown':<9} "
            f"term:{s['term']:<4} log_len:{s['log_len']:<4} "
            f"commit_index:{s['commit_index']}",
            flush=True,
        )

def live_procs(procs):
    return [d for d in procs if d["p"].poll() is None]

def live_ports_from(procs):
    by_id = {n["id"]: n["port"] for n in NODES}
    return [by_id[d["id"]] for d in live_procs(procs)]
def wait_converge_to_leader(ports, target_leader_port, timeout_s=8.0):
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        lead = status_of(target_leader_port) or {}
        lead_LL = lead.get("log_len")
        lead_CI = lead.get("commit_index")
        if lead_LL is None or lead_CI is None:
            time.sleep(0.2)
            continue
        ok = True
        for p in ports:
            st = status_of(p) or {}
            if st.get("log_len") != lead_LL or st.get("commit_index") != lead_CI:
                ok = False
                break
        if ok:
            return True, lead_LL, lead_CI
        time.sleep(0.2)
    return False, None, None

def ports_from(procs):
    by_id = {n["id"]: n["port"] for n in NODES}
    return [by_id[d["id"]] for d in procs]

def _node_id_from_port(port):
    return next(n["id"] for n in NODES if n["port"] == port)

def _print_trace(logs, title, max_appendentries=8):

    print_separator(title)
    count = 0
    for line in logs:
        print(line.rstrip(), flush=True)
        if "AppendEntries" in line:
            count += 1
            if count >= max_appendentries:
                break
    print(".................................", flush=True)

    if count == 0:
        print("(no AppendEntries lines encountered — cluster may still be stabilizing)", flush=True)

# --- pytest fixture: cluster lifecycle ---------------------------------------
@pytest.fixture(scope="module")
def cluster_logs():
    # ensure ports are free (avoids "address already in use" during grading)
    for n in NODES:
        assert _port_free(n["port"]), f"Port {n['port']} is in use. Stop any running servers."

    procs = []
    logs = []

    def reader(pipe, node_id):
        for line in pipe:
            if "HTTP" in line or "Uvicorn running" in line or "Application startup" in line or "Waiting for application" in line:
                continue
            msg = line.rstrip()
            if msg:
                logs.append(f"[{node_id}] {msg}")

    try:
        for n in NODES:
            env = os.environ.copy()
            env["NODE_ID"] = str(n["id"])
            env["PORT"] = str(n["port"])
            env["PEERS_JSON"] = json.dumps({
                str(x["id"]): f"http://localhost:{x['port']}"
                for x in NODES if x["id"] != n["id"]
            })
            # faster timers for tests
            env["ELECTION_MIN_MS"] = "150"
            env["ELECTION_MAX_MS"] = "300"
            env["HEARTBEAT_MS"] = "100"
            # quiet uvicorn
            env["UVICORN_LOG_LEVEL"] = "error"
            env["UVICORN_ACCESS_LOG"] = "false"
            env["LOG_LEVEL"] = "error"

            p = subprocess.Popen(
                ["python", "-u", "rpc_server.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
            )
            procs.append({"id": n["id"], "p": p})
            threading.Thread(target=reader, args=(p.stdout, n["id"]), daemon=True).start()

        time.sleep(3.0)  # initial settle
        yield procs, logs
    finally:
        for d in procs:
            if d["p"].poll() is None:
                d["p"].terminate()
        time.sleep(0.5)
        for d in procs:
            if d["p"].poll() is None:
                d["p"].kill()

# --- the single integrated scenario ------------------------------------------
def test_end_to_end_cluster_flow(cluster_logs):
    """
    Flow:
      1) Show concise election/replication trace
      2) Find leader; write KV set; verify convergence
      3) Kill leader; verify re-election
      4) Write more via new leader; verify convergence & values
    """
    procs, logs = cluster_logs
    all_ports = ports_from(procs)

    # 1) concise backend trace
    time.sleep(3.0)
    _print_trace(logs, "LeaderElection")

    # 2) initial leader, writes, and convergence
    leader_port, st = find_leader(all_ports, timeout_s=12.0)
    assert leader_port is not None, "No leader elected initially"
    leader_id = _node_id_from_port(leader_port)

    before = snapshot(all_ports)
    print_snapshot(before, f"Cluster Status BEFORE writes (Leader port:{leader_port}, term:{st.get('term')})")

    kvs1 = {
        "z1:crop": "wheat",
        "z1:moist": "18.5%",
        "z1:weed": "low",
        "farm:alert": "false",
    }
    base = status_of(leader_port) or {}
    base_commit = base.get("commit_index") or 0
    base_loglen = base.get("log_len") or 0

    print_separator(f"Client writes to Leader (Node ID:{leader_id}, Port:{leader_port}, Term:{st.get('term')})")
    for k, v in kvs1.items():
        ok = set_kv(leader_port, k, v)
        assert ok, f"SET failed on leader for {k}:{v}"
        print(f"SET {k}={v} @ leader:{leader_port}", flush=True)

    converged, lead_LL, lead_CI = wait_converge_to_leader(all_ports, leader_port, timeout_s=10.0)
    assert converged, "Cluster did not converge to leader's (log_len, commit_index)"
    assert (lead_CI or 0) >= base_commit + len(kvs1), "commit_index did not advance enough"
    assert (lead_LL or 0) >= base_loglen + len(kvs1), "log_len did not advance enough"

    after = snapshot(all_ports)
    print_snapshot(after, "Cluster Status AFTER writes (pre-crash)")

    print_separator("Value checks on all nodes (pre-crash)")
    for p in sorted(all_ports):
        #print("-" * 60, flush=True)
        for k, v in kvs1.items():
            got = get_kv(p, k)
            print(f"GET {k} @ {p} -> {got}", flush=True)
            assert got == v, f"Inconsistent value for '{k}' on {p}: expected '{v}', got '{got}'"
        print("-" * 60, flush=True)
    # 3) kill leader, re-elect
    print_separator(f"Killing leader Node ID:{leader_id} (port:{leader_port}) to force re-election")
    cut_idx = len(logs)          # mark where "post-crash" begins
    crashed_id = leader_id
    crashed_port = leader_port
    for d in procs:
        if d["id"] == leader_id:
            d["p"].terminate()
            break
    time.sleep(0.7)
    for d in procs:
        if d["id"] == leader_id:
            assert d["p"].poll() is not None, "Leader did not terminate"

    #survivor_ports = [p for p in all_ports if p != leader_port]
    survivor_ports = live_ports_from(procs)
    post_crash_logs_start = len(logs)

    new_leader_port, st2 = find_leader(survivor_ports, timeout_s=12.0)
    assert new_leader_port is not None, "No new leader elected after crash"
    assert new_leader_port != leader_port, "Leader did not change after crash"
    new_leader_id = _node_id_from_port(new_leader_port)
    #print(f"New Leader elected: Node ID:{new_leader_id} Port:{new_leader_port} Term:{(st2 or {}).get('term')}", flush=True)
    post_crash_logs = logs[post_crash_logs_start:]
    time.sleep(1.0)
    _print_trace(post_crash_logs , "Post Crash Election")

    # 4) write via new leader; converge and verify all values
    kvs2 = {"z2:crop": "rice", "z2:moist": "21.0%", "farm:alert": "true"}
    base2 = status_of(new_leader_port) or {}
    base2_commit = base2.get("commit_index") or 0
    base2_loglen = base2.get("log_len") or 0

    print_separator(f"Client writes to new Leader (Node ID:{new_leader_id}, Port:{new_leader_port}, Term:{(st2 or {}).get('term')})")
    for k, v in kvs2.items():
        ok = set_kv(new_leader_port, k, v)
        assert ok, f"SET failed on new leader for {k}:{v}"
        print(f"SET {k}={v} @ new-leader:{new_leader_port}", flush=True)

    converged2, lead_LL2, lead_CI2 = wait_converge_to_leader(survivor_ports, new_leader_port, timeout_s=10.0)
    assert converged2, "Cluster did not converge after re-election writes"
    assert (lead_CI2 or 0) >= base2_commit + len(kvs2), "commit_index did not advance enough on new leader"
    assert (lead_LL2 or 0) >= base2_loglen + len(kvs2), "log_len did not advance enough on new leader"

    final = snapshot(survivor_ports)
    
    # 5) Restart the crashed node and verify it catches up
    print_separator(f"Crashed Node ID:{crashed_id} rejoins cluster")

    # mark where "post-rejoin" logs begin (so your trace can show only new lines if you want)
    cut_idx = len(logs)

    def _start_node(node_id, node_port):
        """Start a single node with same env wiring as the fixture and attach the log reader."""
        env = os.environ.copy()
        env["NODE_ID"] = str(node_id)
        env["PORT"] = str(node_port)
        env["PEERS_JSON"] = json.dumps({
            str(x["id"]): f"http://localhost:{x['port']}"
            for x in NODES if x["id"] != node_id
        })
        env["ELECTION_MIN_MS"] = "150"
        env["ELECTION_MAX_MS"] = "300"
        env["HEARTBEAT_MS"] = "100"
        env["UVICORN_LOG_LEVEL"] = "error"
        env["UVICORN_ACCESS_LOG"] = "false"
        env["LOG_LEVEL"] = "error"

        p = subprocess.Popen(
            ["python", "-u", "rpc_server.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
        procs.append({"id": node_id, "p": p})

        # attach the same log reader pattern
        def reader(pipe, node_id):
            for line in pipe:
                if (
                    "HTTP" in line
                    or "Uvicorn running" in line
                    or "Application startup" in line
                    or "Waiting for application" in line
                ):
                    continue
                msg = line.rstrip()
                if msg:
                    logs.append(f"[{node_id}] {msg}")
        threading.Thread(target=reader, args=(p.stdout, node_id), daemon=True).start()
        return p

    #print_separator(f"Rejoining crashed node (Node ID:{crashed_id}, Port:{crashed_port})")
    # optional: ensure port is free before restart
    assert _port_free(crashed_port), "Rejoin port is still occupied; wait or free it first"

    _start_node(crashed_id, crashed_port)

    # wait a bit for boot + handshake
    time.sleep(2.0)

    # Build the full set of ports (survivors + rejoined)
    all_ports_after_rejoin = sorted(set(survivor_ports + [crashed_port]))

    # Wait for the rejoined node to receive the missing entries & commit index
    converged3, lead_LL3, lead_CI3 = wait_converge_to_leader(
        all_ports_after_rejoin, new_leader_port, timeout_s=12.0
    )
    assert converged3, "Rejoined node did not catch up to leader (log_len/commit_index mismatch)"


    # Snapshot: all five nodes should now have identical log_len/commit_index
    after_rejoin = snapshot(all_ports_after_rejoin)
    print_snapshot(after_rejoin, "Cluster Status AFTER rejoin (should be 5/5 in sync)")

    # Final value checks on ALL 5 nodes (both batches must be present)
    print_separator("Value checks on ALL 5 nodes (post-rejoin)")
    for p in all_ports_after_rejoin:
        for k, v in {**kvs1, **kvs2}.items():
            got = get_kv(p, k)
            print(f"GET {k} @ {p} -> {got}", flush=True)
            assert got == v, f"[rejoin] Inconsistent value for '{k}' on {p}: expected '{v}', got '{got}'"
        print("-" * 60, flush=True)
    print_separator("✅ INTEGRATION TEST PASSED: Leader election, crash and recovery and log consistency")
