#!/usr/bin/env python3
import sys
import pytest



MENU = {
    "1": {
        "name": "Leader Election",
        "kexpr": "test_election_trace",
        "nodeid": "tests/test.py::test_election_trace"
    },
    "2": {
        "name": "Log Consistency After Writes KVs",
        "kexpr": "test_log_consistency_after_sets",
        "nodeid": "tests/test.py::test_log_consistency_after_sets",
    },
    "3": {
        "name": "Crash And Recovery - Re Election",
        "kexpr": "test_crash_and_recovery_re_election",
        "nodeid": "tests/test.py::test_crash_and_recovery_re_election",
    },
    "4": {
        "name": "Network Partition and Recovery",
        "kexpr": "test_network_partition",
        "nodeid": "tests/test.py::test_network_partition",
    },
    "5": {
        "name": "Integration Test (End-to-End)",
        "kexpr": "test_end_to_end_cluster_flow",
        "nodeid": "tests/intg_test.py::test_end_to_end_cluster_flow",
    },
    "A": {
        "name": "All_Tests",
        "kexpr": "",
        "nodeid": "tests",
    },
    "a": {
        "name": "All_Tests",
        "kexpr": "",
        "nodeid": "tests",
    },
}

def run(nodeid: str, test_name: str) -> int:
    # -s to not capture output; -q for quieter pytest noise; disable warnings to keep your output clean
    print("\n" + "-" * 60, flush=True)
    print(f"\nSelected Test: {test_name}\n", flush=True)
    exitcode = pytest.main([
        nodeid,
        "-s", "-q",
        "--disable-warnings",
        "-o", "log_cli=false",
        "-o", "console_output_style=classic",
    ])
    # Final standardized line:
    if exitcode == 0:
        print(f"\nTEST: {test_name} --- PASSED", flush=True)
    else:
        print(f"\nTEST: {test_name} --- FAILED (exitcode={exitcode})", flush=True)
    return exitcode

def menu():
    print("\n===== Raft Test Scenarios =====")
    print("  1) Leader Election")
    print("  2) Log Consistency")
    print("  3) Crash & recovery re-election")
    print("  4) Network Partition and Recovery")
    print("  5) Integration Test (End-to-End)")
    print("  A) Run all tests")
    print("  Q) Quit")
    choice = input("\nEnter choice: ").strip()
    return choice

def main():
    if len(sys.argv) > 1:
        # Optional: allow non-interactive call: python run_tests.py 1
        arg = sys.argv[1]
        if arg in MENU:
            cfg = MENU[arg]
            return run(cfg["nodeid"], cfg["name"])
        else:
            print("Unknown argument. Valid: 1,2,3,A", flush=True)
            return 2

    while True:
        choice = menu()
        if choice in ("Q", "q"):
            return 0
        if choice in MENU:
            cfg = MENU[choice]
            rc = run(cfg["nodeid"], cfg["name"])
            # after a test run, pause to let you see the tail before returning to menu
            input("\n(press Enter to return to menu) ")
        else:
            print("Invalid choice, try again.", flush=True)

if __name__ == "__main__":
    raise SystemExit(main())
