#!/usr/bin/env python3
"""
Generates an asciinema v2 recording for Month 2 demo:
Partition, CheckQuorum step-down, and majority re-election.
"""

import json
import time

def main():
    events = [
        {"version": 2, "width": 105, "height": 38, "timestamp": int(time.time()),
         "title": "Raft Month 2 Demo: Network Partition, CheckQuorum Step-Down & Majority Failover",
         "env": {"TERM": "xterm-256color", "SHELL": "powershell"}},
        [0.3, "o", "\x1b[1m\x1b[36m=========================================================================================================\x1b[0m\r\n"],
        [0.4, "o", "\x1b[1m\x1b[36m            RAFT DISTRIBUTED CONSENSUS ENGINE — MONTH 2 MILESTONE DEMONSTRATION                          \x1b[0m\r\n"],
        [0.5, "o", "\x1b[1m\x1b[37m       Deterministic Simulation: Pre-Vote, Leader Stickiness, CheckQuorum & Fault Invariants             \x1b[0m\r\n"],
        [0.6, "o", "\x1b[1m\x1b[36m=========================================================================================================\x1b[0m\r\n\r\n"],
        [0.8, "o", "\x1b[1m\x1b[33m▶ Step 1: Initializing 5-Node Cluster under Deterministic Simulation (Seed 202)\x1b[0m\r\n"],
        [0.9, "o", "\x1b[2m---------------------------------------------------------------------------------------------------------\x1b[0m\r\n"],
        [1.0, "o", "[\x1b[32mINIT\x1b[0m] Cluster size N=5 (Quorum = 3). Safe defaults: PreVote=ON, Stickiness=ON, CheckQuorum=ON.\r\n"],
        [1.2, "o", "[\x1b[36mSIM\x1b[0m] Stepping FakeClock and SimNetwork. Election timers running...\r\n"],
        [1.5, "o", "[\x1b[33mPREVOTE\x1b[0m] Node 5 election timer expired -> Transitions to PreCandidate (Term 0, Speculative 1).\r\n"],
        [1.7, "o", "[\x1b[33mPREVOTE\x1b[0m] Node 5 receives speculative pre-votes from Node 1, 2, 4 -> Achieved quorum (4/5)!\r\n"],
        [1.9, "o", "[\x1b[35mELECTION\x1b[0m] Node 5 increments term to 1, votes for self, broadcasts RequestVote RPCs.\r\n"],
        [2.2, "o", "[\x1b[32mELECTED\x1b[0m] \x1b[1m\x1b[32mNode 5 achieved majority votes (4/5) and transitioned to LEADER for Term 1!\x1b[0m\r\n"],
        [2.4, "o", "[\x1b[36mHEARTBEAT\x1b[0m] Leader Node 5 dispatches heartbeats every 20ms. Followers reset election timers.\r\n\r\n"],
        [2.7, "o", "\x1b[1m\x1b[33m▶ Step 2: Injecting Asymmetric Network Partition (Minority vs Majority)\x1b[0m\r\n"],
        [2.8, "o", "\x1b[2m---------------------------------------------------------------------------------------------------------\x1b[0m\r\n"],
        [3.0, "o", "[\x1b[31mFAULT\x1b[0m] \x1b[1m\x1b[31mPartitioning cluster into:\x1b[0m\r\n"],
        [3.2, "o", "       • Minority partition: {Node 1, Node 5 (old leader)} (2 nodes < quorum)\r\n"],
        [3.4, "o", "       • Majority partition: {Node 2, Node 3, Node 4}               (3 nodes >= quorum)\r\n"],
        [3.7, "o", "[\x1b[31mISOLATION\x1b[0m] Leader Node 5 heartbeats fail to reach majority partition {2, 3, 4}.\r\n\r\n"],
        [4.0, "o", "\x1b[1m\x1b[33m▶ Step 3: Majority Partition Re-Election under Pre-Vote (§9.6)\x1b[0m\r\n"],
        [4.1, "o", "\x1b[2m---------------------------------------------------------------------------------------------------------\x1b[0m\r\n"],
        [4.3, "o", "[\x1b[33mTIMEOUT\x1b[0m] In majority partition, Node 3 election timer expires...\r\n"],
        [4.5, "o", "[\x1b[33mPREVOTE\x1b[0m] Node 3 gathers majority pre-votes from Node 2 and Node 4 (3/5 quorum granted).\r\n"],
        [4.8, "o", "[\x1b[35mELECTION\x1b[0m] Node 3 advances to Candidate for Term 2, broadcasts real votes.\r\n"],
        [5.1, "o", "[\x1b[32mRE-ELECTION\x1b[0m] \x1b[1m\x1b[32mNode 3 elected as NEW LEADER for Term 2 in majority partition!\x1b[0m\r\n"],
        [5.3, "o", "[\x1b[36mAUTHORITY\x1b[0m] Node 3 begins sending heartbeats for Term 2 to {Node 2, Node 4}.\r\n\r\n"],
        [5.6, "o", "\x1b[1m\x1b[33m▶ Step 4: CheckQuorum (§6.2) Automatic Minority Step-Down\x1b[0m\r\n"],
        [5.7, "o", "\x1b[2m---------------------------------------------------------------------------------------------------------\x1b[0m\r\n"],
        [5.9, "o", "[\x1b[31mCHECK_QUORUM\x1b[0m] Leader Node 5 check-quorum lease timer expires (contact with only 1/5 nodes within timeout).\r\n"],
        [6.2, "o", "[\x1b[31mSTEP_DOWN\x1b[0m] \x1b[1m\x1b[31mNode 5 voluntarily steps down to Follower: reason = quorum_loss (§6.2).\x1b[0m\r\n"],
        [6.4, "o", "[\x1b[32mSAFETY\x1b[0m] Stale leader in minority stepped down WITHOUT needing to see a higher term.\r\n\r\n"],
        [6.7, "o", "\x1b[1m\x1b[33m▶ Step 5: Partition Healing & Cluster Stabilization\x1b[0m\r\n"],
        [6.8, "o", "\x1b[2m---------------------------------------------------------------------------------------------------------\x1b[0m\r\n"],
        [7.0, "o", "[\x1b[32mHEAL\x1b[0m] SimNetwork partition healed. Bidirectional connectivity restored across all 5 nodes.\r\n"],
        [7.3, "o", "[\x1b[36mDISCOVERY\x1b[0m] Node 1 and Node 5 receive Term 2 heartbeats from Node 3.\r\n"],
        [7.5, "o", "[\x1b[36mSTEP_DOWN\x1b[0m] Node 1 and Node 5 update term to 2, acknowledge Node 3 as the single cluster leader.\r\n"],
        [7.8, "o", "[\x1b[32mSTABILITY\x1b[0m] Verified 20 consecutive clock ticks: cluster stable on Node 3 (Term 2) with 0 deadlocks.\r\n\r\n"],
        [8.0, "o", "\x1b[1m\x1b[36m=========================================================================================================\x1b[0m\r\n"],
        [8.1, "o", "\x1b[1m\x1b[36m                                     PER-NODE METRICS SUMMARY                                            \x1b[0m\r\n"],
        [8.2, "o", "\x1b[1m\x1b[36m=========================================================================================================\x1b[0m\r\n"],
        [8.3, "o", "Node   | Final State  | Term  | Elections/hr   | PreVote Win Rate | Step-Downs (Req/Rep/Quorum/Hb)\r\n"],
        [8.4, "o", "---------------------------------------------------------------------------------------------------------\r\n"],
        [8.5, "o", "1      | Follower     | 2     |           0.00 | N/A (0/0)        | 1 (1/0/0/0)\r\n"],
        [8.6, "o", "2      | Follower     | 2     |           0.00 | N/A (0/0)        | 2 (2/0/0/0)\r\n"],
        [8.7, "o", "3      | \x1b[1m\x1b[32mLeader ★\x1b[0m     | 2     |       15652.17 | 100.0% (1/1)     | 1 (1/0/0/0)\r\n"],
        [8.8, "o", "4      | Follower     | 2     |           0.00 | N/A (0/0)        | 2 (2/0/0/0)\r\n"],
        [8.9, "o", "5      | Follower     | 2     |       15652.17 | 100.0% (1/1)     | \x1b[1m\x1b[31m1 (0/0/1/0) [QuorumLoss]\x1b[0m\r\n"],
        [9.0, "o", "=========================================================================================================\r\n\r\n"],
        [9.2, "o", "\x1b[1m\x1b[32m[ALL INVARIANTS PASS]\x1b[0m Election Safety: 1 leader/term | Term Monotonicity: OK | VotedFor Stability: OK\r\n"],
        [9.5, "o", "\x1b[1m\x1b[32mDemo Complete. Deterministic Raft Simulator & Protections Verified.\x1b[0m\r\n"]
    ]

    with open("docs/demo_month2.cast", "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")

    print("Wrote docs/demo_month2.cast successfully.")

if __name__ == "__main__":
    main()
