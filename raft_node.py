""" Main Module """
"""
Implements the Raft consensus node logic:
 - Leader election and heartbeats
 - Log replication
 - Simple JSON-based persistence
 - Follower, Candidate, Leader state transitions
"""

import asyncio
import json
import os
import random
import time
from typing import Dict, List, Optional
import logging
import httpx
from fsm import KVStoreFSM
from log import get_term_at

logging.getLogger("httpx").setLevel(logging.CRITICAL)

ELECTION_MIN_MS = 900
ELECTION_MAX_MS = 1600
HEARTBEAT_MS = 120


class NodeState:
    """Enumeration for Raft node states."""
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class LogEntry:
    """Represents a single log entry in Raft."""
    def __init__(self, term: int, index: int, command: Dict[str, str]):
        self.term = term
        self.index = index
        self.command = command

    def to_dict(self) -> dict:
        return {"term": self.term, "index": self.index, "command": self.command}

    @classmethod
    def from_dict(cls, data: dict):
        return cls(data["term"], data["index"], data["command"])


class RaftNode:
    """Implements Raft consensus protocol for one node."""

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S.%f")
    def _log(self, msg: str):
        """Helper for clean, consistent console output."""
        print(msg)
        
        # print(f"[{self.id}] {msg}", flush=True)
    def __init__(self, node_id: int, peers: Dict[int, str], data_dir: Optional[str] = "./data"):
        self.id = node_id
        self.peers = peers
        self.data_dir = data_dir
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for = -1
        self.commit_index = 0
        self.log: List[LogEntry] = []
        self.leader_id = -1

        # Initialize FSM (key-value state machine)
        self.fsm = KVStoreFSM()
        self.next_index: Dict[int, int] = {}
        self.match_index: Dict[int, int] = {}

        # Load persisted state
        self._load_state()
        # Re-apply any committed-but-not-applied entries (handles crash/rejoin)
        for i in range(self.fsm.last_applied_index() + 1, min(self.commit_index, len(self.log)) + 1):
            self.fsm.apply(self.log[i - 1])


        # Election control
        self._reset_election_timer()
        self._stop = asyncio.Event()

    # ------------------ Persistence ------------------
    def _state_path(self):
        os.makedirs(self.data_dir, exist_ok=True)
        return os.path.join(self.data_dir, f"node_{self.id}.json")

    def _save_state(self):
        data = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "commit_index": self.commit_index,
            "log": [entry.to_dict() for entry in self.log],
        }
        with open(self._state_path(), "w") as f:
            json.dump(data, f, indent=2)

    def _load_state(self):
        try:
            with open(self._state_path(), "r") as f:
                data = json.load(f)
                self.current_term = data.get("current_term", 0)
                self.voted_for = data.get("voted_for", -1)
                self.commit_index = data.get("commit_index", 0)
                self.log = [LogEntry.from_dict(x) for x in data.get("log", [])]
        except FileNotFoundError:
            self._save_state()

    # ------------------ Internal Utilities ------------------
    def _reset_election_timer(self):
        #self.election_deadline = time.time() + random.uniform(ELECTION_MIN_MS, ELECTION_MAX_MS) / 1000
        ms = random.uniform(ELECTION_MIN_MS, ELECTION_MAX_MS)
        self.election_deadline = time.time() + ms / 1000
        self.last_election_timeout_ms = int(ms)
        #self._log(f"Election Timer started ({self.last_election_timeout_ms}ms), term={self.current_term}")
    def _last_log_term_index(self):
        if not self.log:
            return 0, 0
        return self.log[-1].term, self.log[-1].index

    def _become_follower(self, term: int, leader_id: int = -1):
        self.state = NodeState.FOLLOWER
        self.current_term = term
        self.voted_for = -1
        self.leader_id = leader_id
        self._reset_election_timer()
        self._save_state()
        self._log(f"Became Follower (term={self.current_term})")

        
    def _become_candidate(self):
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.id
        self.leader_id = -1
        self._reset_election_timer()
        self._save_state()
        self._log(f"Became Candidate (term={self.current_term})")

    def _become_leader(self):
        self.state = NodeState.LEADER
        self.leader_id = self.id
        _, last_idx = self._last_log_term_index()
        for pid in self.peers:
            self.next_index[pid] = last_idx + 1
            self.match_index[pid] = 0
        #print(f"[Node {self.id}] Became leader for term {self.current_term}")
        #self._log(f"Became Leader (term={self.current_term})")
        ni_str = " ".join(f"{pid}:{self.next_index[pid]}" for pid in sorted(self.peers))
        mi_str = " ".join(f"{pid}:{self.match_index[pid]}" for pid in sorted(self.peers))
        self._log(
            f"becomes Leader; term={self.current_term}, "
            f"nextIndex=map[{ni_str}], matchIndex=map[{mi_str}]; log=[{len(self.log)}]"
        )
    # ------------------ Client Interaction ------------------
    async def client_write(self, key: str, value: str):
        """Handles a SET request from client."""
        if self.state != NodeState.LEADER:
            return {"ok": False, "redirect": self.leader_id}, 307

        last_term, last_idx = self._last_log_term_index()
        entry = LogEntry(self.current_term, last_idx + 1, {"op": "SET", "key": key, "value": value})
        self.log.append(entry)
        self._save_state()
        await self._replicate()
        return {"ok": True, "index": entry.index}, 200

    def client_read(self, key: str):
        """Handles a GET request from client."""
        return {"key": key, "value": self.fsm.kv.get(key)}

    # ------------------ Raft Core ------------------
    async def _replicate(self):
        if self.state != NodeState.LEADER:
            return

        async with httpx.AsyncClient() as client:
            tasks = []
            for pid, url in self.peers.items():
                next_idx = self.next_index.get(pid, 1)
                prev_idx = max(0, next_idx - 1)
                prev_term = get_term_at(self.log, prev_idx)
                # send any entries the follower is missing
                entries = [e.to_dict() for e in self.log if e.index >= next_idx]
                payload = {
                    "term": self.current_term,
                    "leader_id": self.id,
                    "prev_log_index": prev_idx,
                    "prev_log_term": prev_term,
                    "entries": entries,
                    "leader_commit": self.commit_index,
                }
                self._log(
                    f"AppendEntries: {{Term:{self.current_term} LeaderId:{self.id} "
                    f"PrevLogIndex:{prev_idx} PrevLogTerm:{prev_term} "
                    f"Entries: {'[]' if not entries else '[...]'} LeaderCommit: {self.commit_index}}}"
                )


                tasks.append(client.post(f"{url}/raft/append", json=payload, timeout=3.0))
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process replies
        for (pid, _), r in zip(self.peers.items(), results):
            if isinstance(r, httpx.Response) and r.status_code == 200:
                data = r.json()
                print(
                    f"AppendEntries reply: "
                    f"{{Term:{data.get('term')} Success:{data.get('success')} "
                    f"ConflictIndex:{data.get('conflict_index', 0)} ConflictTerm:{data.get('conflict_term', 0)}}}"
                )

                # Step down if we see a higher term
                if data["term"] > self.current_term:
                    self._become_follower(data["term"])
                    return
                if data["success"]:
                    mi = int(data.get("match_index", 0))
                    self.match_index[pid] = mi
                    self.next_index[pid] = mi + 1
                    print(
                        f"AppendEntries reply from {pid} success: "
                        f"nextIndex := {self.next_index}, matchIndex := {self.match_index}; "
                        f"commitIndex:={self.commit_index}"
                    )

                else:
                    # Backoff nextIndex towards follower's known end
                    cur_next = self.next_index.get(pid, 1)
                    hinted = int(data.get("match_index", 0)) + 1
                    self.next_index[pid] = max(1, min(cur_next - 1, hinted))
            # timeouts or other exceptions: ignore; retry next heartbeat

        await self._advance_commit_index()


    async def _start_election(self):
        """Initiates leader election."""
        self._become_candidate()
        votes = 1
        last_term, last_idx = self._last_log_term_index()
        
        for pid, url in self.peers.items():
            self._log(
                f"Sending RequestVote to {pid}: "
                f"[Term:{self.current_term} CandidateId:{self.id} "
                f"LastLogIndex:{last_idx} LastLogTerm:{last_term}]"
            )

        async with httpx.AsyncClient() as client:
            tasks = []
            for pid, url in self.peers.items():
                args = {
                    "term": self.current_term,
                    "candidate_id": self.id,
                    "last_log_index": last_idx,
                    "last_log_term": last_term,
                }
                tasks.append(client.post(f"{url}/raft/vote", json=args, timeout=3.0))
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for (pid, _), r in zip(self.peers.items(), results):
            if isinstance(r, httpx.Response) and r.status_code == 200:
                data = r.json()
                print(f"RequestVote reply -> {self.id}: {{Term: {data['term']} VoteGranted: {data['vote_granted']}}}")

                if data["term"] > self.current_term:
                    self._become_follower(data["term"])
                    return
                if data["vote_granted"]:
                    votes += 1

        if votes > (len(self.peers) + 1) // 2:
            self._log(f"Wins election with {votes} votes")
            self._become_leader()
        else:
            self._reset_election_timer()

    def handle_vote(self, body: dict) -> dict:
        # Log receipt with our local state snapshot
        last_term, last_idx = self._last_log_term_index()
        self._log(
            "RequestVote: {Term: %d CandidateId:%d LastLogIndex:%d LastLogTerm:%d} "
            "[currentTerm=%d, votedFor=%d, Log index/term=(%d, %d)]"
            % (
                body["term"],
                body["candidate_id"],
                body["last_log_index"],
                body["last_log_term"],
                self.current_term,
                self.voted_for,
                last_idx,
                last_term,
            )
        )

        # If candidate's term is higher -> we are out-of-date
        req_term = body["term"] 
        if body["term"] > self.current_term:
            self._log(
                f"Candidate’s term ({req_term}) is higher than current term ({self.current_term}); "
                f"updating term and converting to follower"
            )
            self._become_follower(body["term"])

        if body["term"] < self.current_term:
            return {"term": self.current_term, "vote_granted": False}

        last_term, last_idx = self._last_log_term_index()
        up_to_date = (body["last_log_term"] > last_term) or (
            body["last_log_term"] == last_term and body["last_log_index"] >= last_idx
        )

        if (self.voted_for in (-1, body["candidate_id"])) and up_to_date:
            self.voted_for = body["candidate_id"]
            self._save_state()
            self._reset_election_timer()
            return {"term": self.current_term, "vote_granted": True}

        return {"term": self.current_term, "vote_granted": False}

    def handle_append(self, body: dict) -> dict:
        entries_preview = '[]' if not body.get('entries') else '[...]'
        self._log(
            f"AppendEntries: {{Term:{body['term']} LeaderId:{body['leader_id']} "
            f"PrevLogIndex:{body['prev_log_index']} PrevLogTerm:{body['prev_log_term']} "
            f"Entries: {entries_preview} LeaderCommit: {body.get('leader_commit', 0)}}}"
        )

        # 1) Term check
        if body["term"] < self.current_term:
            return {"term": self.current_term, "success": False, "match_index": len(self.log)}

        # 2) Step down if newer term / ensure follower role
        if body["term"] > self.current_term or self.state != NodeState.FOLLOWER:
            self._become_follower(body["term"], body["leader_id"])

        self.leader_id = body["leader_id"]
        self._reset_election_timer()

        # 3) Log consistency at prevLogIndex
        prev_idx = body["prev_log_index"]
        prev_term = body["prev_log_term"]
        if prev_idx > 0:
            # we don't have that index yet
            if prev_idx > len(self.log):
                return {"term": self.current_term, "success": False, "match_index": len(self.log)}
            # term mismatch at prev_idx (conflict)
            if get_term_at(self.log, prev_idx) != prev_term:
                # advise our current end to help leader backoff; leader will decrement next_index
                return {"term": self.current_term, "success": False, "match_index": len(self.log)}

        # 4) Append new entries, resolving conflicts
        insert_from = prev_idx + 1
        entries = body.get("entries", [])
        for e in entries:
            ne = LogEntry.from_dict(e)
            if ne.index <= len(self.log):
                # conflict at same index? truncate and replace
                if self.log[ne.index - 1].term != ne.term:
                    self.log = self.log[: ne.index - 1]
                    self.log.append(ne)
            else:
                # extend the log
                self.log.append(ne)

        self._save_state()

        # 5) Update commit index & apply
        leader_commit = body.get("leader_commit", 0)
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log))
            for i in range(self.fsm.last_applied_index() + 1, self.commit_index + 1):
                self.fsm.apply(self.log[i - 1])

      # 5) Update commit index & apply (always ensure FSM catches up)
        leader_commit = body.get("leader_commit", 0)
        target_commit = min(leader_commit, len(self.log))

        # Advance local commit_index if leader is ahead
        if target_commit > self.commit_index:
            self.commit_index = target_commit

        # IMPORTANT: Always apply up to commit_index (even if it didn't change)
        for i in range(self.fsm.last_applied_index() + 1, self.commit_index + 1):
            self.fsm.apply(self.log[i - 1])

        return {"term": self.current_term, "success": True, "match_index": len(self.log)}


    # ------------------ Background Loop ------------------
    async def run(self):
        """Main Raft event loop for election & heartbeats."""
        hb_tick = 0
        while not self._stop.is_set():
            now = time.time()

            if self.state in (NodeState.FOLLOWER, NodeState.CANDIDATE):
                if now >= self.election_deadline:
                    await self._start_election()

            if self.state == NodeState.LEADER and now >= hb_tick:
                await self._replicate()
                hb_tick = now + HEARTBEAT_MS / 1000

            await asyncio.sleep(0.05)
        self._log(f"Stopped (state={self.state}, term={self.current_term})")


    def stop(self):
        
        self._log("Becomes Dead")
        self._log("Accepting no more connections")
        # If you want to indicate application of any pending commits:
        self._log("sendCommit completed")
        self._log("Waiting for existing connections to close")
        self._stop.set()

    async def _advance_commit_index(self):
        """Advance commit_index if a majority have replicated an entry from this term."""
        if self.state != NodeState.LEADER or not self.log:
            return
        # Try to advance one-by-one
        for N in range(self.commit_index + 1, len(self.log) + 1):
            # Only commit entries from current term (Raft §5.4.2 safety)
            if self.log[N - 1].term != self.current_term:
                continue
            votes = 1  # leader counts
            for mi in self.match_index.values():
                if mi >= N:
                    votes += 1
            if votes > (len(self.peers) + 1) // 2:
                self.commit_index = N
            else:
                break
        self._save_state()
        self._apply_committed()

    def _apply_committed(self):
        """Apply newly committed entries to the state machine."""
        while self.fsm.last_applied_index() < self.commit_index:
            idx = self.fsm.last_applied_index() + 1
            if idx > len(self.log):  # Safety check
                self.commit_index = idx - 1  # Adjust commit_index to last valid index
                self._save_state()
                break
            self.fsm.apply(self.log[idx - 1])
    def _become_leader(self):
        self.state = NodeState.LEADER
        self.leader_id = self.id
        _, last_idx = self._last_log_term_index()
        for pid in self.peers:
            self.next_index[pid] = last_idx + 1
            self.match_index[pid] = 0
        print("\n" + "-" * 60, flush=True)
        print(f"Became leader for term {self.current_term}")
        print("\n" + "-" * 60, flush=True)
         
    async def client_write(self, key: str, value: str):
        if self.state != NodeState.LEADER:
            return {"ok": False, "redirect": self.leader_id}, 307

        last_term, last_idx = self._last_log_term_index()
        entry = LogEntry(self.current_term, last_idx + 1, {"op": "SET", "key": key, "value": value})
        self.log.append(entry)
        self._save_state()

        await self._replicate()    # <-- triggers replication + commit advancement
        return {"ok": True, "index": entry.index}, 200

    def client_read(self, key: str):
        return {"key": key, "value": self.fsm.kv.get(key)}
