"""

FastAPI-based HTTP interface for Raft nodes.

This module exposes the Raft consensus node (RaftNode) as a web service:
- Starts the Raft background loop asynchronously within FastAPI’s event loop.
- Provides REST endpoints for:
  • /admin – health, status, dump, and forced election controls.
  • /client – client-facing GET/SET operations on the distributed key-value store.
  • /raft   – internal Raft RPCs (RequestVote and AppendEntries) for inter-node communication.
- Reads NODE_ID, PORT, and PEERS_JSON from environment variables to configure each node.
- Suppresses Uvicorn/FastAPI logs for cleaner Raft trace output during testing.
"""

import warnings
import os
import json
import asyncio
import logging, os
import uvicorn

from fastapi import FastAPI, HTTPException
from raft_node import RaftNode


warnings.filterwarnings("ignore", category=DeprecationWarning)
if os.getenv("TEST_MODE") == "true":
    logging.getLogger("uvicorn").setLevel(logging.ERROR)
    logging.getLogger("uvicorn.access").disabled = True
    logging.getLogger("fastapi").disabled = True



# Silence default Uvicorn / FastAPI logs
uvicorn_logger = logging.getLogger("uvicorn")
uvicorn_logger.setLevel(logging.CRITICAL)

uvicorn_access = logging.getLogger("uvicorn.access")
uvicorn_access.setLevel(logging.CRITICAL)


def build_server(node: RaftNode) -> FastAPI:
    app = FastAPI(title=f"Raft Node {node.id}")
    
    # -------- lifecycle: start the raft event loop in THIS uvicorn loop --------
    @app.on_event("startup")
    async def _start_raft_loop():
        # run Raft in the same event loop as FastAPI/Uvicorn
        
        asyncio.create_task(node.run())
        #print("[Server] Waiting for input...", flush=True)
   

    # ---------------------- admin endpoints ----------------------
    @app.get("/admin/health")
    async def health():
        return {"ok": True}

    @app.get("/admin/status")
    async def status():
        return {
            "id": node.id,
            "state": node.state,          # 0 follower, 1 candidate, 2 leader
            "term": node.current_term,
            "leader": node.leader_id,
            "log_len": len(node.log),
            "commit_index": node.commit_index,
        }

    @app.post("/admin/force_election")
    async def force_election():
        node.force_election()
        return {"ok": True, "msg": "Election timeout set to now"}

    # ---------------------- client endpoints ----------------------
    @app.post("/client/write")
    async def write(body: dict):
        if "key" not in body or "value" not in body:
            raise HTTPException(400, "key and value required")
        res, code = await node.client_write(str(body["key"]), str(body["value"]))
        if code == 307:
            # follower: tell client who the leader is (by id)
            raise HTTPException(307, detail=res)
        return res

    @app.get("/client/read")
    async def read(key: str):
        return node.client_read(key)

    @app.get("/admin/dump")
    async def dump():
        return {
            "id": node.id,
            "state": node.state,
            "term": node.current_term,
            "leader": node.leader_id,
            "log_len": len(node.log),
            "commit_index": node.commit_index,
            "last_applied": node.fsm.last_applied_index(),
            "kv": node.fsm.kv,
            "match_index": node.match_index,
            "next_index": node.next_index,
        }
    

    # ---------------------- raft rpc ----------------------
    @app.post("/raft/vote")
    async def vote(body: dict):
        return node.handle_vote(body)

    @app.post("/raft/append")
    async def append(body: dict):
        return node.handle_append(body)

    @app.post("/raft/add_server")
    async def add_server(body: dict):
        return node.handle_add_server(body)
    
    @app.post("/raft/remove_server")
    async def remove_server(body: dict):
        return node.handle_remove_server(body)

    @app.post("/raft/snapshot")
    async def snapshot():
        node.take_snapshot()
        return {"ok": True}

    return app

   
   
    

if __name__ == "__main__":
    node_id = int(os.getenv("NODE_ID", "1"))
    port = int(os.getenv("PORT", "8001"))
    peers_json = os.getenv("PEERS_JSON", "{}")
    # build peer map excluding self
    peers = {int(k): v for k, v in json.loads(peers_json).items() if int(k) != node_id}

    node = RaftNode(node_id=node_id, peers=peers)
    app = build_server(node)

    for name in ["uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"]:
        logging.getLogger(name).setLevel(logging.CRITICAL)

    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=port,
        log_level="critical",     # suppress internal INFO
        access_log=False          # suppress per-request logs
    )
    server = uvicorn.Server(config)
    try:
        server.run()
    except KeyboardInterrupt:
        print("\n[Server] Shutdown requested. Exiting cleanly...", flush=True)