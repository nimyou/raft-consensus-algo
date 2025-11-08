---
output:
  html_document: default
  word_document: default
  pdf_document: default
---



A Python implementation of the Raft consensus algorithm for distributed systems, featuring leader election, log replication, and fault tolerance.

### Overview

This project implements the Raft consensus protocol with:

- **Leader Election**: Automatic election of a leader node among cluster members
- **Log Replication**: Consistent replication of state machine commands across all nodes
- **Fault Tolerance**: Handles node crashes and network partitions
- **Persistence**: State is persisted to disk for crash recovery
- **Key-Value Store**: Simple distributed key-value store as the state machine

### Architecture

#### Core Components

- **`raft_node.py`**: Core Raft implementation with leader election and log replication
- **`rpc_server.py`**: FastAPI-based RPC server for inter-node communication
- **`fsm.py`**: Key-value store state machine
- **`client.py`**: Client interface for interacting with the cluster

#### Node States

1. **Follower**: Default state, receives AppendEntries from leader
2. **Candidate**: Requests votes during election
3. **Leader**: Handles client requests and replicates logs

### Installation

#### Pre-requisites

- Python 3.8+
- pip

#### Setup

1. **Navigate to project directory**:
   ```bash
   cd raft-asg03-nimrah9570
   ```

2. **Create a virtual environment** (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Install pytest for testing**:
   ```bash
   pip install pytest requests
   ```

### Running the System


#### Option 1: Using Docker Compose

```bash
docker compose up --build
```
#### Option 2: Manual Node Startup

Start each node in a separate terminal:

```bash
# Terminal 1 - Node 1
export NODE_ID=1
export PORT=8001
export PEERS_JSON='{"2":"http://localhost:8002","3":"http://localhost:8003","4":"http://localhost:8004","5":"http://localhost:8005"}'
python rpc_server.py

# Terminal 2 - Node 2
export NODE_ID=2
export PORT=8002
export PEERS_JSON='{"1":"http://localhost:8001","3":"http://localhost:8003","4":"http://localhost:8004","5":"http://localhost:8005"}'
python rpc_server.py

# Terminal 3 - Node 3
export NODE_ID=3
export PORT=8003
export PEERS_JSON='{"1":"http://localhost:8001","2":"http://localhost:8002","4":"http://localhost:8004","5":"http://localhost:8005"}'
python rpc_server.py

# Terminal 4 - Node 4
export NODE_ID=4
export PORT=8004
export PEERS_JSON='{"1":"http://localhost:8001","2":"http://localhost:8002","3":"http://localhost:8003","5":"http://localhost:8005"}'
python rpc_server.py

# Terminal 5 - Node 5
export NODE_ID=5
export PORT=8005
export PEERS_JSON='{"1":"http://localhost:8001","2":"http://localhost:8002","3":"http://localhost:8003","4":"http://localhost:8004"}'
python rpc_server.py
```

### Client Usage

#### Using Python Client

The included [client.py] provides a CLI to interact with the Raft cluster. It automatically discovers the leader node and handles communication.

##### Write a key-value pair:
```bash
python client.py write zone1:temp "25.5C"
```

##### Read a value:
```bash
python client.py read zone1:temp
```
#### Check status of nodes:
```bash
python client.py status
```


### Testing

### Running All Tests

```bash
pytest tests/ -v
```

#### Test Suite Overview

The test suite includes both automated pytest scripts and a manual test runner:

#### Run Tests with run_test.py

For manual testing and demonstration, use the interactive test runner:

```bash
python run_test.py
```

This provides a menu-driven interface to run individual test scenarios:
1. Leader Election Test
2. Log Consistency Test
3. Crash and Recovery Test
4. Network Partition Test
5. Integration Test

#### 1. Leader Election Test

Tests the basic leader election mechanism:

```bash
pytest tests/test.py::test_election_trace -v -s
```

#### 2. Log Consistency Test

Tests log replication and consistency:

```bash
pytest tests/test.py::test_log_consistency_after_sets -v -s
```

#### 3. Crash and Recovery Test

Tests fault tolerance and re-election:

```bash
pytest tests/test.py::test_crash_and_recovery_re_election -v -s
```

#### 4. Network Partition Test

Tests split-brain scenarios and partition recovery:

```bash
pytest tests/test.py::test_network_partition -v -s
```

#### 5. Integration Test

Comprehensive end-to-end test:

```bash
pytest tests/intg_test.py::test_end_to_end_cluster_flow -v -s
```


## Configuration

### Environment Variables

| Variable | Description | Default | Example |
|----------|-------------|---------|----------|
| `NODE_ID` | Unique identifier for the node | Required | `1` |
| `PORT` | HTTP port for the node | Required | `8001` |
| `PEERS_JSON` | JSON map of peer node IDs to URLs | Required | `{"2":"http://localhost:8002"}` |
| `ELECTION_MIN_MS` | Minimum election timeout (ms) | `150` | `1000` |
| `ELECTION_MAX_MS` | Maximum election timeout (ms) | `300` | `3000` |
| `HEARTBEAT_MS` | Heartbeat interval (ms) | `100` | `500` |
| `LOG_LEVEL` | Logging level | `error` | `info` |

### Tuning Parameters

**For faster testing:**
```bash
export ELECTION_MIN_MS=150
export ELECTION_MAX_MS=300
export HEARTBEAT_MS=100
```

**For production:**
```bash
export ELECTION_MIN_MS=900
export ELECTION_MAX_MS=1600
export HEARTBEAT_MS=120
```

## Data Persistence

Each node persists its state to `./data/node_{id}.json`:

- Current term
- Voted for (in current term)
- Log entries
- State machine snapshot (key-value pairs)

**To reset cluster state:**
```bash
rm -rf data/*.json
```

**Note:** The test suite automatically cleans up persisted data before each test run to ensure consistent results.

### Architecture Details

#### Leader Election Process

1. **Initial State**: All nodes start as followers
2. **Timeout**: If no heartbeat received within election timeout, become candidate
3. **Vote Request**: Increment term, vote for self, request votes from peers
4. **Vote Decision**: Peers grant vote if:
   - Haven't voted in this term yet
   - Candidate's log is at least as up-to-date
5. **Become Leader**: If majority votes received, become leader
6. **Heartbeats**: Leader sends periodic heartbeats to maintain authority
7. **Step Down**: If discover higher term, revert to follower

#### Log Replication Flow

1. **Client Request**: Client sends write request to leader
2. **Append to Log**: Leader appends entry to local log
3. **Replicate**: Leader sends `AppendEntries` RPC to all followers
4. **Follower Append**: Followers append entry and acknowledge
5. **Commit**: Once majority acknowledges, leader commits entry
6. **Apply**: Leader applies entry to state machine
7. **Notify**: Leader notifies followers of commit in next heartbeat
8. **Follower Apply**: Followers apply committed entries to their state machines

#### Safety Properties

- **Election Safety**: At most one leader per term
- **Leader Append-Only**: Leader never overwrites or deletes entries
- **Log Matching**: If two logs contain an entry with same index and term, they are identical up to that point
- **Leader Completeness**: If entry is committed in a term, it will be present in all future leaders
- **State Machine Safety**: If a node applies a log entry at a given index, no other node will apply a different entry at that index

### Project Structure

```
raft-asg03-nimrah/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── raft_node.py             # Core Raft implementation
├── rpc_server.py            # FastAPI RPC server
├── fsm.py                   # Key-value state machine
├── client.py                # Client interface
├── run_test.py              # Interactive test runner
├── log.py                   # Logging utilities
├── docker-compose.yml       # Docker configuration
├── Dockerfile               # Docker image definition
├── data/                    # Persisted node state
│   ├── node_1.json
│   ├── node_2.json
│   └── ...
└── tests/                   # Test suite
    ├── test.py              # Unit tests
    ├── intg_test.py         # Integration tests
```

#### References

- **Raft Paper**: ["In Search of an Understandable Consensus Algorithm"](https://raft.github.io/raft.pdf) by Diego Ongaro and John Ousterhout


#### License

This project is for educational purposes.

---
