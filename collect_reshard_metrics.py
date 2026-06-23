#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone


RISK_THRESHOLD_WARN = 80_000    # approaching default dynamic-reshard trigger
RISK_THRESHOLD_CRIT = 100_000   # at or above default trigger (~102,400 obj/shard)

HEALTH_STATUS_MAP = {"HEALTH_OK": 0, "HEALTH_WARN": 1, "HEALTH_ERR": 2}
RISK_LEVEL_MAP = {"ok": 0, "warning": 1, "critical": 2}


def run_cmd(args, timeout=30):
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            print(f"[WARN] {' '.join(args)}: {result.stderr.strip()}", file=sys.stderr)
            return None
        return json.loads(result.stdout)
    except subprocess.TimeoutExpired:
        print(f"[WARN] timed out: {' '.join(args)}", file=sys.stderr)
        return None
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"[WARN] {' '.join(args)}: {e}", file=sys.stderr)
        return None


def collect_reshard_queue():
    entries = []
    marker = ""
    while True:
        cmd = ["radosgw-admin", "reshard", "list", "--format=json"]
        if marker:
            cmd += ["--marker", marker]
        data = run_cmd(cmd)
        if data is None:
            break
        batch = data if isinstance(data, list) else data.get("entries", [])
        entries.extend(batch)
        is_truncated = data.get("truncated", False) if isinstance(data, dict) else False
        next_marker = data.get("marker", "") if isinstance(data, dict) else ""
        if not is_truncated or not next_marker or next_marker == marker:
            break
        marker = next_marker
    return {"queue_depth": len(entries), "entries": entries}


def collect_bucket_stats(bucket_spec):
    parts = bucket_spec.split("/", 1)
    cmd = ["radosgw-admin", "bucket", "stats", "--format=json"]
    cmd += ["--tenant", parts[0], "--bucket", parts[1]] if len(parts) == 2 else ["--bucket", parts[0]]
    data = run_cmd(cmd, timeout=60)
    if data is None:
        return {"error": f"failed to get stats for {bucket_spec}"}

    layout = data.get("layout", {})
    current_index = layout.get("current_index", {}).get("layout", {})
    target_index = layout.get("target_index", {}).get("layout", {}) if layout.get("target_index") else None
    usage = data.get("usage", {})

    total_objects = sum(v.get("num_objects", 0) for v in usage.values()) or usage.get("rgw.main", {}).get("num_objects", 0)
    total_bytes = sum(v.get("size_actual", 0) for v in usage.values())
    
    num_shards = data.get("num_shards") or current_index.get("num_shards", 1) or 1

    objects_per_shard = total_objects / num_shards

    risk_level = "ok"
    if objects_per_shard >= RISK_THRESHOLD_CRIT:
        risk_level = "critical"
    elif objects_per_shard >= RISK_THRESHOLD_WARN:
        risk_level = "warning"

    result = {
        "bucket": data.get("bucket", bucket_spec),
        "tenant": data.get("tenant", ""),
        "num_shards": num_shards,
        "shard_generation": layout.get("current_index", {}).get("gen", 0),
        "total_objects": total_objects,
        "total_bytes": total_bytes,
        "objects_per_shard": round(objects_per_shard, 1),
        "reshard_state": layout.get("resharding", "none"),
        "reshard_status": data.get("reshard_status"),
        "judge_reshard_lock_time": layout.get("judge_reshard_lock_time"),
        "risk_level": risk_level,
    }
    if target_index:
        result["target_num_shards"] = target_index.get("num_shards")
    return result

def collect_cluster_health():
    status = run_cmd(["ceph", "-s", "--format=json"], timeout=30)
    if status is None:
        return {"error": "ceph -s failed"}

    health = status.get("health", {})
    osdmap = status.get("osdmap", {})
    pgmap = status.get("pgmap", {})

    osd = {
        "num_osds": osdmap.get("num_osds", 0),
        "num_up_osds": osdmap.get("num_up_osds", 0),
        "num_remapped_pgs": osdmap.get("num_remapped_pgs", 0),
    }
    osd["num_down_osds"] = osd["num_osds"] - osd["num_up_osds"]

    pg_by_state = {}
    degraded_pgs = recovering_pgs = remapped_pgs = 0
    for s in pgmap.get("pgs_by_state", []):
        name, count = s.get("state_name", ""), s.get("count", 0)
        pg_by_state[name] = count
        if "degraded" in name:
            degraded_pgs += count
        if "recovering" in name or "recovery_wait" in name:
            recovering_pgs += count
        if "remapped" in name:
            remapped_pgs += count

    health_checks = {
        k: {"severity": v.get("severity"), "summary": v.get("summary", {}).get("message", "")}
        for k, v in health.get("checks", {}).items()
    }
    large_omap = 0
    if "LARGE_OMAP_OBJECTS" in health_checks:
        try:
            large_omap = int(health_checks["LARGE_OMAP_OBJECTS"]["summary"].split()[0])
        except (ValueError, IndexError):
            large_omap = -1

    return {
        "health_status": health.get("status", "unknown"),
        "health_checks": health_checks,
        "large_omap_objects": large_omap,
        "osd": osd,
        "pgs": {
            "num_pgs": pgmap.get("num_pgs", 0),
            "degraded_pgs": degraded_pgs,
            "recovering_pgs": recovering_pgs,
            "remapped_pgs": remapped_pgs,
            "degraded_objects": pgmap.get("degraded_objects", 0),
            "degraded_ratio": pgmap.get("degraded_ratio", 0.0),
            "by_state": pg_by_state,
        },
        "client_io": {
            "read_bytes_sec": pgmap.get("read_bytes_sec", 0),
            "write_bytes_sec": pgmap.get("write_bytes_sec", 0),
            "read_op_per_sec": pgmap.get("read_op_per_sec", 0),
            "write_op_per_sec": pgmap.get("write_op_per_sec", 0),
        },
        "recovery_io": {
            "recovering_bytes_per_sec": pgmap.get("recovering_bytes_per_sec", 0),
            "recovering_keys_per_sec": pgmap.get("recovering_keys_per_sec", 0),
            "recovering_objects_per_sec": pgmap.get("recovering_objects_per_sec", 0),
        },
    }


def detect_correlations(reshard, cluster):
    depth = reshard.get("queue_depth", 0)
    osd = cluster.get("osd", {})
    pgs = cluster.get("pgs", {})
    out = []
    if depth > 0 and osd.get("num_down_osds", 0) > 0:
        out.append(f"reshard_queue_with_osd_down: queue={depth}, down_osds={osd['num_down_osds']}")
    if depth > 0 and pgs.get("degraded_pgs", 0) > 0:
        out.append(f"reshard_queue_with_degraded_pgs: queue={depth}, degraded_pgs={pgs['degraded_pgs']}")
    if depth > 0 and pgs.get("remapped_pgs", 0) > 0:
        out.append(f"reshard_queue_with_remapped_pgs: queue={depth}, remapped_pgs={pgs['remapped_pgs']}")
    if depth > 0 and cluster.get("large_omap_objects", 0) > 0:
        out.append(f"reshard_queue_with_large_omap: queue={depth}, large_omap_objects={cluster['large_omap_objects']}")
    return out


def format_prometheus(snapshot):
    lines = []
    ts_ms = int(datetime.fromisoformat(snapshot["timestamp"]).timestamp() * 1000)

    def metric(name, value, labels=None):
        label_str = ""
        if labels:
            parts = ",".join(f'{k}="{v}"' for k, v in labels.items())
            label_str = f"{{{parts}}}"
        lines.append(f"{name}{label_str} {value} {ts_ms}")

    def header(name, help_text, type_="gauge"):
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {type_}")

    q = snapshot["reshard_queue"]
    header("ceph_rgw_reshard_queue_depth", "Number of buckets pending resharding in the RGW reshard queue")
    metric("ceph_rgw_reshard_queue_depth", q["queue_depth"])

    c = snapshot["cluster_health"]
    if "error" not in c:
        header("ceph_health_status", "Ceph cluster health status: 0=HEALTH_OK 1=HEALTH_WARN 2=HEALTH_ERR")
        metric("ceph_health_status", HEALTH_STATUS_MAP.get(c["health_status"], -1))

        header("ceph_large_omap_objects", "Number of large OMAP objects reported by cluster health")
        metric("ceph_large_omap_objects", c["large_omap_objects"])

        osd = c["osd"]
        header("ceph_osd_total", "Total number of OSDs")
        metric("ceph_osd_total", osd["num_osds"])
        header("ceph_osd_up", "Number of OSDs that are up")
        metric("ceph_osd_up", osd["num_up_osds"])
        header("ceph_osd_down", "Number of OSDs that are down")
        metric("ceph_osd_down", osd["num_down_osds"])
        header("ceph_osd_remapped_pgs", "Number of remapped PGs reported by osdmap")
        metric("ceph_osd_remapped_pgs", osd["num_remapped_pgs"])

        pgs = c["pgs"]
        header("ceph_pg_total", "Total number of placement groups")
        metric("ceph_pg_total", pgs["num_pgs"])
        header("ceph_pg_degraded", "Number of degraded placement groups")
        metric("ceph_pg_degraded", pgs["degraded_pgs"])
        header("ceph_pg_recovering", "Number of recovering placement groups")
        metric("ceph_pg_recovering", pgs["recovering_pgs"])
        header("ceph_pg_remapped", "Number of remapped placement groups")
        metric("ceph_pg_remapped", pgs["remapped_pgs"])
        header("ceph_pg_degraded_objects", "Number of degraded objects")
        metric("ceph_pg_degraded_objects", pgs["degraded_objects"])
        header("ceph_pg_degraded_ratio", "Ratio of degraded objects to total objects")
        metric("ceph_pg_degraded_ratio", pgs["degraded_ratio"])

        io = c["client_io"]
        header("ceph_client_read_bytes_per_second", "Client read throughput in bytes per second")
        metric("ceph_client_read_bytes_per_second", io["read_bytes_sec"])
        header("ceph_client_write_bytes_per_second", "Client write throughput in bytes per second")
        metric("ceph_client_write_bytes_per_second", io["write_bytes_sec"])
        header("ceph_client_read_ops_per_second", "Client read operations per second")
        metric("ceph_client_read_ops_per_second", io["read_op_per_sec"])
        header("ceph_client_write_ops_per_second", "Client write operations per second")
        metric("ceph_client_write_ops_per_second", io["write_op_per_sec"])

        rec = c["recovery_io"]
        header("ceph_recovery_bytes_per_second", "Recovery throughput in bytes per second")
        metric("ceph_recovery_bytes_per_second", rec["recovering_bytes_per_sec"])
        header("ceph_recovery_keys_per_second", "Recovery keys per second")
        metric("ceph_recovery_keys_per_second", rec["recovering_keys_per_sec"])
        header("ceph_recovery_objects_per_second", "Recovery objects per second")
        metric("ceph_recovery_objects_per_second", rec["recovering_objects_per_sec"])

    bucket_metrics = [b for b in snapshot["watched_buckets"] if "error" not in b]
    if bucket_metrics:
        for name, help_text, key in [
            ("ceph_rgw_bucket_objects_total", "Total number of objects in the bucket", "total_objects"),
            ("ceph_rgw_bucket_bytes_total", "Total bytes used by the bucket", "total_bytes"),
            ("ceph_rgw_bucket_shards", "Number of index shards for the bucket", "num_shards"),
            ("ceph_rgw_bucket_objects_per_shard", "Average objects per index shard", "objects_per_shard"),
            ("ceph_rgw_bucket_shard_generation", "Current index shard generation", "shard_generation"),
        ]:
            header(name, help_text)
            for b in bucket_metrics:
                metric(name, b[key], {"bucket": b["bucket"], "tenant": b.get("tenant", "")})

        header("ceph_rgw_bucket_resharding", "1 if bucket is currently being resharded, 0 otherwise")
        for b in bucket_metrics:
            val = 0 if b.get("reshard_state") in (None, "none", "None") else 1
            metric("ceph_rgw_bucket_resharding", val, {"bucket": b["bucket"], "tenant": b.get("tenant", "")})

        header("ceph_rgw_bucket_risk_level", "Reshard risk level: 0=ok 1=warning 2=critical")
        for b in bucket_metrics:
            metric("ceph_rgw_bucket_risk_level", RISK_LEVEL_MAP.get(b["risk_level"], -1),
                   {"bucket": b["bucket"], "tenant": b.get("tenant", "")})

    return "\n".join(lines) + "\n"


def print_summary(snapshot):
    q = snapshot["reshard_queue"]
    c = snapshot["cluster_health"]

    print(f"\n{'='*60}")
    print(f"Timestamp: {snapshot['timestamp']}")
    print(f"{'='*60}")

    print(f"RESHARD QUEUE  (depth: {q['queue_depth']})")
    for e in q.get("entries", []):
        initiator = {1: "admin", 2: "dynamic"}.get(e.get("initiator"), "unknown")
        print(f"  - {e.get('bucket_name')}: {e.get('old_num_shards')} -> {e.get('new_num_shards')} shards [{initiator}] at {e.get('time')}")

    if "error" in c:
        print(f"\nCLUSTER HEALTH: ERROR - {c['error']}")
    else:
        print(f"\nCLUSTER HEALTH: {c['health_status']}")
        osd = c["osd"]
        print(f"  OSDs: {osd['num_up_osds']}/{osd['num_osds']} up  ({osd['num_down_osds']} down, {osd['num_remapped_pgs']} remapped PGs)")
        pgs = c["pgs"]
        print(f"  PGs: {pgs['num_pgs']} total  |  degraded={pgs['degraded_pgs']}  recovering={pgs['recovering_pgs']}  remapped={pgs['remapped_pgs']}")
        if c["large_omap_objects"]:
            print(f"  Large OMAP objects: {c['large_omap_objects']}")
        io = c["client_io"]
        print(f"  Client I/O: {io['read_op_per_sec']:.0f} rd/s  {io['write_op_per_sec']:.0f} wr/s  ({io['read_bytes_sec']/1e6:.1f} MB/s rd  {io['write_bytes_sec']/1e6:.1f} MB/s wr)")
        rec = c["recovery_io"]
        if rec["recovering_objects_per_sec"] > 0:
            print(f"  Recovery: {rec['recovering_objects_per_sec']:.0f} obj/s  {rec['recovering_keys_per_sec']:.0f} keys/s  {rec['recovering_bytes_per_sec']/1e6:.1f} MB/s")

    if snapshot["watched_buckets"]:
        print(f"\nWATCHED BUCKETS")
        for b in snapshot["watched_buckets"]:
            if "error" in b:
                print(f"  {b.get('bucket', '?')}: ERROR - {b['error']}")
                continue
            marker = {"ok": "  ", "warning": "! ", "critical": "!!"}[b["risk_level"]]
            reshard_info = f"  [RESHARDING -> {b.get('target_num_shards')} shards]" if b.get("reshard_state") not in (None, "none", "None") else ""
            print(f"  {marker}{b['bucket']}: {b['total_objects']:,} objects / {b['num_shards']} shards = {b['objects_per_shard']:,.0f} obj/shard  [{b['risk_level'].upper()}]{reshard_info}")

    if snapshot["correlations"]:
        print(f"\nCORRELATIONS DETECTED:")
        for ev in snapshot["correlations"]:
            print(f"  !! {ev}")


def take_snapshot(watched_buckets):
    print("[INFO] Collecting reshard queue ...", file=sys.stderr)
    reshard = collect_reshard_queue()

    print("[INFO] Collecting cluster health ...", file=sys.stderr)
    cluster = collect_cluster_health()

    bucket_stats = []
    for bspec in watched_buckets:
        print(f"[INFO] Collecting stats for bucket: {bspec} ...", file=sys.stderr)
        bucket_stats.append(collect_bucket_stats(bspec))

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "reshard_queue": reshard,
        "cluster_health": cluster,
        "watched_buckets": bucket_stats,
        "correlations": detect_correlations(reshard, cluster),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect RGW reshard metrics")
    parser.add_argument("buckets", nargs="*", metavar="BUCKET", help="Buckets to watch (tenant/name or name)")
    parser.add_argument("--output", default="snapshot", metavar="PREFIX", help="Output file prefix (default: snapshot)")
    parser.add_argument("--interval", type=int, metavar="SECONDS", help="Repeat every N seconds; omit for a single run")
    parser.add_argument("--format", dest="fmt", choices=["human", "prometheus"], default="human",
                        help="Stdout output format: human-readable (default) or prometheus exposition format")
    parser.add_argument("--prom-file", metavar="PATH",
                        help="Write Prometheus exposition format to this file on each run (e.g. for node_exporter textfile collector)")
    args = parser.parse_args()

    run = 0
    while True:
        run += 1
        snapshot = take_snapshot(args.buckets)

        ts = snapshot["timestamp"].replace(":", "").replace("-", "").replace("+00:00", "Z")
        filename = f"{args.output}_{ts}.json"
        with open(filename, "w") as f:
            json.dump(snapshot, f, indent=2)
        print(f"[INFO] Saved {filename}", file=sys.stderr)

        if args.fmt == "prometheus":
            print(format_prometheus(snapshot), end="")
        else:
            print_summary(snapshot)

        if args.prom_file:
            prom_text = format_prometheus(snapshot)
            tmp = args.prom_file + ".tmp"
            with open(tmp, "w") as f:
                f.write(prom_text)
            os.replace(tmp, args.prom_file)
            print(f"[INFO] Wrote {args.prom_file}", file=sys.stderr)

        if not args.interval:
            break
        print(f"[INFO] Next snapshot in {args.interval}s (Ctrl-C to stop) ...", file=sys.stderr)
        time.sleep(args.interval)
