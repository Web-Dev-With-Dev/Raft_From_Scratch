#!/usr/bin/env python3
"""
Renders a text timeline and per-node metrics summary from a Raft simulation trace.

Usage:
    python scripts/render_timeline.py --seed 42 --scenario election
    python scripts/render_timeline.py --trace traces/election_seed_42.json
    python scripts/render_timeline.py --seed 101 --export-dir ./traces/seed_101
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime


def format_event(ev):
    ev_type = ev.get("type", "")
    term = ev.get("term", 0)
    details = ev.get("details") or {}
    msg = ev.get("message", "")

    if ev_type == "server_started":
        return f"[INIT] Server started (T:{term})"
    elif ev_type == "state_transition":
        return f"[STATE] {msg}"
    elif ev_type == "election_started":
        return f"[ELECTION] Started (T:{term})"
    elif ev_type == "vote_granted":
        if details.get("self_vote"):
            return f"[VOTE] Voted for self (T:{term})"
        if details.get("pre_vote"):
            return f"[PREVOTE] Granted (T:{term})"
        return f"[VOTE] Granted (T:{term})"
    elif ev_type == "pre_vote_granted":
        return f"[PREVOTE] Granted (T:{term})"
    elif ev_type == "pre_vote_denied":
        reason = details.get("reason", "")
        return f"[PREVOTE] Denied: {reason} (T:{term})"
    elif ev_type == "vote_denied":
        reason = details.get("reason", "")
        return f"[VOTE] Denied: {reason} (T:{term})"
    elif ev_type == "term_change":
        return f"[TERM] {msg}"
    elif ev_type == "timer_reset":
        reason = details.get("reason", "")
        return f"[TIMER] Reset ({reason})"
    elif ev_type == "timer_timeout":
        return f"[TIMEOUT] Election timer (T:{term})"
    elif ev_type == "heartbeat_sent":
        return f"[HEARTBEAT] Sent (T:{term})"
    elif ev_type == "heartbeat_received":
        leader = int(details.get("leader_id", 0))
        return f"[HEARTBEAT] Recv L:{leader} (T:{term})"
    elif ev_type == "step_down":
        reason = details.get("reason", "")
        return f"[STEPDOWN] {reason}"
    return f"[{ev_type}] {msg}"


def parse_timestamp(ts_str):
    if not ts_str:
        return datetime.min
    # Remove Z or handle fractional seconds
    ts_str = ts_str.rstrip("Z")
    try:
        return datetime.fromisoformat(ts_str)
    except Exception:
        return datetime.min


def render_timeline(trace):
    scenario = trace.get("scenario", "unknown")
    seed = trace.get("seed", 0)
    status = trace.get("status", "UNKNOWN")
    nodes_data = trace.get("nodes", {})
    if not nodes_data:
        print("No node data found in trace.")
        return

    node_ids = sorted(int(k) for k in nodes_data.keys())
    all_events = []
    base_time = None

    for nid in node_ids:
        nd = nodes_data[str(nid)] if str(nid) in nodes_data else nodes_data[nid]
        for ev in nd.get("events") or []:
            dt = parse_timestamp(ev.get("timestamp"))
            if base_time is None or dt < base_time:
                base_time = dt
            all_events.append((dt, nid, ev))

    all_events.sort(key=lambda x: x[0])

    col_width = 30
    total_width = 13 + len(node_ids) * (col_width + 3)
    banner = "=" * total_width
    sep = "-" * total_width

    print(banner)
    print("                   RAFT SCENARIO EXECUTION TIMELINE")
    print(f"  Scenario: {scenario:<12} | Seed: {seed:<6} | Status: {status:<4}")
    print(banner)

    header = f"{'Time (+ms)':<11} |"
    for nid in node_ids:
        header += f" {'Node ' + str(nid):<{col_width}} |"
    print(header)
    print(sep)

    for dt, nid, ev in all_events:
        delta_ms = (dt - base_time).total_seconds() * 1000.0 if base_time else 0.0
        time_str = f"+{delta_ms:7.2f}ms"
        summary = format_event(ev)
        if len(summary) > col_width:
            summary = summary[:col_width - 3] + "..."

        row = f"{time_str:<11} |"
        for curr_id in node_ids:
            if curr_id == nid:
                row += f" {summary:<{col_width}} |"
            else:
                row += f" {'':<{col_width}} |"
        print(row)

    render_metrics(nodes_data, node_ids)


def render_metrics(nodes_data, node_ids):
    total_width = 95
    banner = "=" * total_width
    sep = "-" * total_width

    print("\n" + banner)
    print("                                   PER-NODE METRICS SUMMARY")
    print(banner)
    print(f"{'Node':<6} | {'Final State':<12} | {'Term':<5} | {'Elections/hr':<14} | {'PreVote Win Rate':<16} | Step-Downs (Req/Rep/Quorum/Hb)")
    print(sep)

    for nid in node_ids:
        nd = nodes_data[str(nid)] if str(nid) in nodes_data else nodes_data[nid]
        state = nd.get("state", "Unknown")
        term = nd.get("term", 0)
        metrics = nd.get("metrics") or {}

        elections_hr = metrics.get("elections_per_hour", 0.0)
        pv_started = metrics.get("pre_votes_started", 0)
        pv_won = metrics.get("pre_votes_won", 0)
        pv_rate = metrics.get("pre_vote_success_rate", 0.0) * 100.0
        pv_str = f"{pv_rate:5.1f}% ({pv_won}/{pv_started})" if pv_started > 0 else "N/A (0/0)"

        sd = metrics.get("step_down_reasons") or {}
        req = sd.get("higher_term_request", 0)
        rep = sd.get("higher_term_reply", 0)
        qrm = sd.get("quorum_loss", 0)
        hb = sd.get("leader_heartbeat", 0)
        total_sd = req + rep + qrm + hb
        sd_str = f"{total_sd} ({req}/{rep}/{qrm}/{hb})"

        print(f"{nid:<6} | {state:<12} | {term:<5} | {elections_hr:14.2f} | {pv_str:<16} | {sd_str}")

    print(banner)


def export_per_node(trace, export_dir):
    if not export_dir:
        return
    os.makedirs(export_dir, exist_ok=True)
    nodes = trace.get("nodes", {})
    exported = []
    for nid, node_data in nodes.items():
        fname = f"node_{nid}_trace.json"
        fpath = os.path.join(export_dir, fname)
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(node_data, f, indent=2)
        exported.append((nid, fpath))

    print(f"\nExported {len(exported)} per-node event trace JSON files to {export_dir}:")
    for nid, p in exported:
        print(f"  - Node {nid}: {p}")


def main():
    parser = argparse.ArgumentParser(description="Render Raft seed timeline and metrics")
    parser.add_argument("--trace", type=str, help="Path to trace JSON file")
    parser.add_argument("--scenario", type=str, default="election", help="Scenario to run")
    parser.add_argument("--seed", type=int, default=42, help="Seed to run")
    parser.add_argument("--export-dir", type=str, default="", help="Directory to export per-node traces")
    args = parser.parse_args()

    if args.trace and os.path.exists(args.trace):
        with open(args.trace, "r", encoding="utf-8") as f:
            trace = json.load(f)
    else:
        # Run via cmd/timeline
        cmd = ["go", "run", "./cmd/timeline", f"-scenario={args.scenario}", f"-seed={args.seed}"]
        if args.export_dir:
            cmd.append(f"-export-dir={args.export_dir}")
        subprocess.run(cmd, check=True)
        return

    render_timeline(trace)
    if args.export_dir:
        export_per_node(trace, args.export_dir)


if __name__ == "__main__":
    main()
