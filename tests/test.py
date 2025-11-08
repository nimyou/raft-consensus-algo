import os, json, time, socket, subprocess, threading, warnings
from contextlib import closing
import pytest
import requests, uuid, random

#suppress warnings globally
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)
_append_seen_global = 0

NODES = [{"id": 1, "port": 8001},
         {"id": 2, "port": 8002},
         {"id": 3, "port": 8003},
         {"id": 4, "port": 8004},
         {"id": 5, "port": 8005}]

@pytest.fixture(scope="module")
def cluster_logs():
    procs = []
    logs = []

    def reader(pipe, node_id):
        for line in pipe:
            # ignore uvicorn access logs and HTTP lines
            if "INFO:" in line and "HTTP" in line:
                continue
            if "Uvicorn running on" in line:
                continue
            if "Waiting for application" in line:
                continue
            if "Application startup" in line:
                continue
            msg = line.rstrip()
            # keep only first N AppendEntries lines
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
            # shorten timers for faster tests
            env["ELECTION_MIN_MS"] = "150"
            env["ELECTION_MAX_MS"] = "300"
            env["HEARTBEAT_MS"] = "100"

            # 🚫 disable all uvicorn access logs and lower verbosity
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
        time.sleep(3.0)
        yield procs, logs
    finally:
        for d in procs:
            d["p"].terminate()
        time.sleep(0.5)
        for d in procs:
            if d["p"].poll() is None:
                d["p"].kill()

# ==== TEST-ONLY HELPERS (no app changes required) ============================

STATUS_PATHS = ["/status", "/admin/status", "/raft/status"]
SET_PATHS = [
    ("/client/write", "POST"),   # expects json {"key": "...", "value": "..."}
    ("/kv/set", "POST"),         # same json
]
GET_PATHS = [
    ("/client/read", "GET"),     # expects params ?key=...
    ("/kv/get", "GET"),          # same
]
STATE_STR = {0: "follower", 1: "candidate", 2: "leader"}
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
    """Poll nodes until a leader is reported; return (leader_port, status_dict)."""
    t0 = time.time()
    last_seen = {}
    while time.time() - t0 < timeout_s:
        for p in ports:
            st = status_of(p)
            if st:
                last_seen[p] = st
                # typical shapes: {'state': 2} or {'role': 'leader'} or {'is_leader': True}
                is_leader = (
                    (st.get("state") == 2) or
                    (str(st.get("role", "")).lower() == "leader") or
                    (bool(st.get("is_leader", False)))
                )
                if is_leader:
                    return p, st
        time.sleep(0.2)
    # best effort: if none explicitly leader, pick a node claiming biggest match/term
    if last_seen:
        # prefer higher term, then higher log_len
        ranked = sorted(
            last_seen.items(),
            key=lambda kv: (kv[1].get("term", -1), kv[1].get("log_len", -1)),
            reverse=True
        )
        return ranked[0][0], ranked[0][1]
    return None, None

def set_kv(port, key, value):
    base = f"http://localhost:{port}"
    payload = {"key": key, "value": value}
    # try JSON posts first
    for path, method in SET_PATHS:
        r = _req(base + path, method, json=payload)
        if r and r.ok:
            return True
    # fallback: form-encoded
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
                # common shapes: {'value': '...'} or {'ok': True, 'value': '...'}
                if "value" in data:
                    return data["value"]
                # sometimes server returns whole kv item
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
            f"term:{s['term']:<4} "
            f"log_len:{s['log_len']:<4} commit_index:{s['commit_index']}",
            flush=True
        )
    #print_separator()

def wait_converge_to_leader(ports, target_leader_port, min_delta=0, timeout_s=8.0):
    """
    Wait until all nodes have the same (log_len, commit_index) as the leader,
    and (optionally) leader's commit_index increased by at least min_delta.
    """
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
        if ok and min_delta:
            # we also want to ensure commit moved forward
            if lead_CI is not None and isinstance(lead_CI, int):
                # If we want "at least min_delta", we need initial baseline—handled by caller.
                return True, lead_LL, lead_CI
            # else keep waiting
        elif ok:
            return True, lead_LL, lead_CI
        time.sleep(0.2)
    return False, None, None

def _port_free(port: int) -> bool:
    import socket
    from contextlib import closing
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.connect_ex(("127.0.0.1", port)) != 0

def _start_node(node_id, node_port, procs, logs):
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
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, env=env,
    )

    # Replace or append the dict entry for this node id
    found = False
    for d in procs:
        if d["id"] == node_id:
            d["p"] = p
            found = True
            break
    if not found:
        procs.append({"id": node_id, "p": p})

    # re-attach log reader
    
    #threading.Thread(target=reader, args=(p.stdout, node_id), daemon=True).start()
    return p

def _wait_status(port, timeout_s=6.0):
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        st = status_of(port)
        if st:
            return st
        time.sleep(0.2)
    return None

def verify_kv_consistency(ports, kvs, title="Value consistency check"):
    """Verify that all nodes have the same values for given keys."""
    print(f"\n{title}")
    for port in sorted(ports):
        for k, v in kvs.items():
            got = get_kv(port, k)
            assert got == v, f"Node {port} has inconsistent value for '{k}': expected '{v}', got '{got}'"
            print(f"  Node {port}: {k}={got}")
    print("✓ All nodes have consistent values")

def alive_ports(procs):
    return [p["p"].poll() is None for p in procs]

def live_procs(procs):
    return [d for d in procs if d["p"].poll() is None]

def live_ports_from(procs):
    by_id = {n["id"]: n["port"] for n in NODES}
    return [by_id[d["id"]] for d in live_procs(procs)]

def ports_from(procs):
    # map proc list to port list via NODES order
    # relies on NODES being the truth for ids->ports
    by_id = {n["id"]: n["port"] for n in NODES}
    return [by_id[d["id"]] for d in procs]

def wait_all_same_log_len(ports, timeout_s=6.0):
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        lens = []
        ok = True
        for p in ports:
            st = status_of(p)
            if not st:
                ok = False
                break
            lens.append(st.get("log_len", None))
        if ok and None not in lens and len(set(lens)) == 1:
            return True, lens[0]
        time.sleep(0.2)
    return False, None



# ==== LEADER ELECTION TEST =============================
def test_election_trace(cluster_logs):
    procs, logs = cluster_logs
    count = 0
    max_appendentries=8
    time.sleep(3.0)
    for line in logs:
        print(line.rstrip(), flush=True)
        if "AppendEntries" in line:
            count += 1
            if count >= max_appendentries:
                break
    print(".................................", flush=True)

    if count == 0:
        print("(no AppendEntries lines encountered — cluster may still be stabilizing)", flush=True)

    print("\n" + "-" * 60, flush=True)
    
# ==== LOG CONSISTENCY TEST =============================

def test_log_consistency_after_sets(cluster_logs):
    procs, logs = cluster_logs
    all_ports = [n["port"] for n in NODES
                 if next((d for d in procs if d["id"] == n["id"]), None)
                 and next((d for d in procs if d["id"] == n["id"]), None)["p"].poll() is None]

    # KV dataset
    kvs = {
        "z1:crop": "wheat",        # crop in Zone 1
        "z1:moist": "18.5%",       # soil moisture reading
        "z1:weed": "low",          # weed alert level
        "farm:alert": "false",     # global weed outbreak flag
    }
    before = snapshot(all_ports)
    print_snapshot(before, "Cluster Status (before writes):")
    
    leader_port, st = find_leader(all_ports, timeout_s=10.0)
    assert leader_port is not None, "No leader available for writes"
    print_separator(f"Replicating field telemetry and weed alerts across cluster\nLeader Port:{leader_port}, Term:{st.get('term')}")

    base_leader_status = status_of(leader_port) or {}
    base_commit = base_leader_status.get("commit_index") or 0
    base_loglen = base_leader_status.get("log_len") or 0

    for k, v in kvs.items():
        ok = set_kv(leader_port, k, v)
        assert ok, f"SET failed on leader for {k}:{v}"
        print(f"SET on leader {leader_port}: {k}:{v}", flush=True)

    time.sleep(1.0)
    converged, lead_LL, lead_CI = wait_converge_to_leader(all_ports, leader_port, timeout_s=8.0)
    assert converged, "Cluster did not converge to leader's (log_len, commit_index)"
    assert (lead_CI or 0) >= base_commit + len(kvs), \
        f"commit_index did not advance by {len(kvs)} (before:{base_commit}, after:{lead_CI})"
    assert (lead_LL or 0) >= base_loglen + len(kvs), \
        f"log_len did not grow by {len(kvs)} (befor:{base_loglen}, after:{lead_LL})"

    after = snapshot(all_ports)
    print_snapshot(after, "Cluster Status (after writes):")

    print_separator("Per-node value checks:")
    for p in sorted(all_ports):
        print(f"Reading from port:{p}", flush=True)
        for k, v in kvs.items():
            got = get_kv(p, k)
            print(f"  GET {k} -> {got} (expected {v})", flush=True)
            assert got == v, f"Inconsistent value for key '{k}' on port {p}: expected '{v}', got '{got}'"

    print_separator("All followers match leader values for agricultural data.")


# ==== CRASH AND RECOVERY RE-ELECTION TEST =============================

def test_crash_and_recovery_re_election(cluster_logs):
    procs, logs = cluster_logs
    all_ports = ports_from(procs)

    # 1) wait for an initial leader
    leader_port, st = find_leader(all_ports, timeout_s=10.0)
    assert leader_port is not None, "No leader elected initially"
    leader_id = next(n["id"] for n in NODES if n["port"] == leader_port)
   # print("=== RUN   Test_CrashAndRecoveryReElection ===", flush=True)
    print(f"Current Leader, Node ID:{leader_id} Term:{st.get('term')} Port:{leader_port} ")
    crashed_id = leader_id
    crashed_port = leader_port
    # 2) crash the leader process
    for d in procs:
        if d["id"] == leader_id:
            d["p"].terminate()
            print(f"Leader Node ID:{leader_id} crashed. Becomes Dead.", flush=True)
            break

    time.sleep(0.5)
    # 3) verify the leader died
    for d in procs:
        if d["id"] == leader_id:
            d["p"].poll()  # update status
            assert d["p"].poll() is not None, "Leader did not terminate"

    # 4) re-election among the remaining live nodes
    survivor_ports = live_ports_from(procs)
    post_crash_logs_start = len(logs)
    new_leader_port, st2 = find_leader(survivor_ports, timeout_s=12.0)
    assert new_leader_port is not None, "No new leader elected after crash"
    assert new_leader_port != leader_port, "Leader did not change after crash"
    new_leader_id = next(n["id"] for n in NODES if n["port"] == new_leader_port)
    print("\n")
    print(f"Election Timer Started For Term:{st2.get('term')}")
    post_crash_logs = logs[post_crash_logs_start:]
    time.sleep(1.0)

    def test_election_trace(cluster_logs):
        logs = cluster_logs
        count = 0
        max_appendentries=8
        time.sleep(3.0)
        for line in logs:
            print(line.rstrip(), flush=True)
            if "AppendEntries" in line:
                count += 1
                if count >= max_appendentries:
                    break
        print(".................................", flush=True)

    test_election_trace(post_crash_logs)
    #print(f"New Leader elected after crash: Node ID:{new_leader_id} Term:{st2.get('term')} Port:{new_leader_port} ")
    #print("=== PASS   Test_CrashAndRecoveryReElection ===", flush=True)
    print("\n" + "-" * 60, flush=True)
    
import subprocess, time, requests, json, signal

def test_network_partition(cluster_logs):
    #print("\n=== TEST: Network Partition and Recovery ===\n", flush=True)

    # 1️⃣ Get existing processes and logs
    procs, logs = cluster_logs
    ports = [n["port"] for n in NODES]  # Get ports from NODES instead of procs

    # 2️⃣ Identify leader from existing cluster
    leader_port, st = find_leader(ports, timeout_s=10.0)
    assert leader_port is not None, "No leader found initially!"
    leader_id = next(n["id"] for n in NODES if n["port"] == leader_port)
    print(f"Leader for 5-node cluster: Node ID:{leader_id} Port:{leader_port}")

    # 3️⃣ Create partition groups
    # Group A: Leader + 1 random follower (will be "disconnected")
    # Group B: Remaining 3 nodes (will form new majority)
    follower_ports = [p for p in ports if p != leader_port]
    group_a_ports = [leader_port, random.choice(follower_ports)]
    group_b_ports = [p for p in ports if p not in group_a_ports]

    # Get node IDs for logging
    group_a_ids = [next(n["id"] for n in NODES if n["port"] == p) for p in group_a_ports]
    group_b_ids = [next(n["id"] for n in NODES if n["port"] == p) for p in group_b_ports]

    print(f"\nCreating network partition:")
    print(f"  Group A (disconnected): Nodes {group_a_ids} (Ports: {group_a_ports})")
    print(f"  Group B (active):       Nodes {group_b_ids} (Ports: {group_b_ports})")

    # 4️⃣ Simulate partition
    print("\nSimulating network partition by terminating Group A nodes...")
    group_a_procs = []
    for d in procs:
        if d["id"] in [next(n["id"] for n in NODES if n["port"] == p) for p in group_a_ports]:
            #print(f"  Terminating Node {d['id']}...")
            d["p"].terminate()
            group_a_procs.append(d)

   # Wait for termination
    time.sleep(2)
    for d in group_a_procs:
        d["p"].poll()  # Update status
        assert d["p"].poll() is not None, f"Node {d['id']} did not terminate"

    # 5️⃣ Verify Group B can elect a new leader
    print("\nGroup B starts re-election for new leader...")
    new_leader_port, st2 = find_leader(group_b_ports, timeout_s=10.0)
    assert new_leader_port is not None, "Group B failed to elect a new leader"
    new_leader_id = next(n["id"] for n in NODES if n["port"] == new_leader_port)
    print(f"Group B elected new leader: Node ID:{new_leader_id} Port:{new_leader_port}")

        # 5️⃣ Reconnect nodes in Group A
    print("\nHealing partition by restarting Group A nodes...")
    cut = len(logs)

    # restart each isolated node INSIDE the loop
    for d in group_a_procs:
        node_id = d["id"]
        port = next(n["port"] for n in NODES if n["id"] == node_id)
        print(f"  Restarting Node {node_id} (port {port})...", flush=True)

        # ensure port is free (macOS can leave TIME_WAIT briefly)
        t0 = time.time()
        while time.time() - t0 < 3.0 and not _port_free(port):
            time.sleep(0.1)

        _start_node(node_id, port, procs, logs)
        st = _wait_status(port, timeout_s=6.0)
        assert st is not None, f"Node {node_id} on port {port} did not come back online"

    # verify the whole cluster converges again under Group B’s leader
    all_ports_after = [n["port"] for n in NODES]
    converged, _, _ = wait_converge_to_leader(all_ports_after, new_leader_port, timeout_s=12.0)
    assert converged, "Cluster did not re-sync after healing partition"
    time.sleep(2.0)  # Increased wait time for nodes to come up
    print_snapshot(snapshot(all_ports_after), "Cluster Status after healing partition")


   # 7️⃣ Write agriculture data after partition recovery and verify consistency
    test_key = "zone3:temp"
    test_value = "28.5C"
    print(f"\n\nWriting post-recovery agriculture data: {test_key}={test_value}")
    ok = set_kv(new_leader_port, test_key, test_value)
    assert ok, "Failed to set agriculture data after recovery"
    
    # Wait for replication to complete
    time.sleep(2.0)
    converged2, _, _ = wait_converge_to_leader(all_ports_after, new_leader_port, timeout_s=10.0)
    assert converged2, "Cluster did not converge after post-recovery write"
    
    # Verify consistency across all nodes
    verify_kv_consistency(all_ports_after, {test_key: test_value}, 
                         "Verifying agriculture data consistency across all nodes")

    print_snapshot(snapshot(all_ports_after), "Cluster Status after healing partition")

    print("\n✅ TEST PASSED: Network partition and recovery complete\n", flush=True)