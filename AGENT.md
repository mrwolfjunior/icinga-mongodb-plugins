# AGENT.md — icinga-mongodb-plugins

## Project Overview

**check_mongodb.py** is a **monolithic** Icinga/Nagios plugin for monitoring on-prem MongoDB instances.
Single entry point, no sub-packages. Supports Standalone, ReplicaSet, ReplicaSet+Arbiter, Sharded Cluster.
Compatible with MongoDB 5.x–8.2, Python ≥ 3.8.

**Version**: `__version__` in `check_mongodb.py` (format: `YY.MM.DD`, e.g. `26.02.12`)
**License**: MIT

---

## Repository Layout

```
check_mongodb.py          # ← ONLY production file (all logic here)
requirements.txt          # pymongo>=4.0,<5.0  dnspython>=2.0
requirements-dev.txt      # + pytest>=7.0  pytest-timeout>=2.0
tests/
  test_check_mongodb.py   # Unit tests (mocked, no Docker)
  test_e2e.py             # End-to-end tests (require Docker network)
  fault_injection.sh      # Stop/restore nodes & simulate network partitions
docker/
  docker-compose.single-{5,7}.0.yml
  docker-compose.replicaset-{5,7}.0.yml
  docker-compose.replicaset-arbiter-{5,7}.0.yml
  docker-compose.sharded-{5,7}.0.yml
  init-replicaset.sh
run_e2e.sh                # Helper: runs E2E tests inside Docker network
assets/
  banner.png
README.md
```

---

## Key Architecture (check_mongodb.py)

All classes live in the single file. Dependency order:

```
IcingaOutput              → formats plugin output (status, perfdata, long output)
MongoConnectionManager    → builds MongoClient kwargs, parses URI, direct node connections
TopologyDetector          → detect() → "standalone" | "replicaset" | "sharded"
ThresholdEngine           → evaluates metrics against warning/critical thresholds (above/below modes)
AvailabilityChecker       → --availability logic per topology
MetricsChecker            → --metrics logic (serverStatus, repl lag, oplog, WiredTiger, etc.)
FilesystemChecker         → --filesystem logic (dbStats, dynamic logarithmic thresholds)
main()                    → CLI entrypoint (argparse)
```

### Exit Codes (Nagios standard)
| Code | Meaning |
|------|---------|
| 0 | OK |
| 1 | WARNING |
| 2 | CRITICAL |
| 3 | UNKNOWN |

---

## CLI Parameters

| Flag | Required | Notes |
|------|----------|-------|
| `--uri` | **Yes** | Standard or `mongodb+srv://` |
| `--availability` | mutual | Check node reachability |
| `--metrics` | mutual | Collect serverStatus metrics |
| `--filesystem` | mutual | Check disk via dbStats |
| `--username / -u` | No | |
| `--password / -p` | No | |
| `--auth-mechanism` | No | `SCRAM-SHA-256`, `SCRAM-SHA-1`, `PLAIN` |
| `--auth-source` | No | Default: `admin` (`$external` for LDAP) |
| `--tls` | No | Enable TLS |
| `--tls-insecure` | No | Skip cert verification |
| `--timeout` | No | Seconds, default 10 |
| `--thresholds` | No | JSON string, keys per metric |
| `--replicaset` | No | Override RS name check |
| `--verbose / -v` | No | Extra long_output lines |

---

## Threshold Format

```json
{
  "conn_usage_pct":    {"warning": 80, "critical": 90},
  "oplog_window":      {"warning": 48, "critical": 24, "mode": "below"},
  "repl_lag":          {"warning": 30, "critical": 120},
  "tickets_read_pct":  {"warning": 80, "critical": 95},
  "tickets_write_pct": {"warning": 80, "critical": 95},
  "cache_usage_pct":   {"warning": 80, "critical": 90},
  "fs_usage_pct":      {"warning": 85, "critical": 95}
}
```

- Default `mode` is `"above"` (alert when value exceeds threshold).
- `oplog_window` uses `"below"` (alert when window drops **under** threshold).

---

## Coding Conventions

- **Black** formatter (`code style: black` badge)
- Python 3.8+ compatible — no walrus operator, no `match`, no f-string `=` specifier unless 3.8-safe
- All metric perfdata labels: `sanitize_metric_name()` (lowercase, `.:-` → `_`)
- Perfdata label format: `<role>_<rs_name>_<host_port>_<metric>` or `<host_port>_<metric>`
- Never use `print()` outside `IcingaOutput.exit()` — all output goes through `IcingaOutput`
- Status escalation is **one-way** (OK → WARNING → CRITICAL, UNKNOWN only if not already W/C)
- Topology detection uses `hello` command, falls back to `isMaster` for older MongoDB
- Direct connections (`directConnection=True`) are used for per-node checks
- SRV URIs: use pymongo topology discovery to resolve hostnames

---

## Testing

### Unit Tests (no Docker)
```bash
python -m pytest tests/test_check_mongodb.py -v
```

### E2E Tests (requires Docker)
1. Start the desired topology:
   ```bash
   docker compose -f docker/docker-compose.replicaset-7.0.yml up -d
   sleep 30
   ```
2. Run via helper script (spins a Python container inside `docker_mongonet`):
   ```bash
   ./run_e2e.sh -v
   # Or target a specific test:
   ./run_e2e.sh -v -k test_availability_quorum_lost
   ```
3. Tear down:
   ```bash
   docker compose -f docker/docker-compose.replicaset-7.0.yml down -v
   ```

#### Available Docker topologies
| File | Topology |
|------|---------|
| `single-{5,7}.0.yml` | Standalone |
| `replicaset-{5,7}.0.yml` | 3-node ReplicaSet |
| `replicaset-arbiter-{5,7}.0.yml` | 2 data + 1 arbiter |
| `sharded-{5,7}.0.yml` | Full sharded cluster |

#### E2E Environment Variables (set by run_e2e.sh)
```
RS_URI           mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0
RS_ARBITER_URI   mongodb://mongo-data1:27017,mongo-data2:27017,mongo-arbiter:27017/?replicaSet=rsArbiter
SHARDED_URI      mongodb://mongos1:27017,mongos2:27017/
SINGLE_URI       mongodb://mongo-single:27017/
```

#### Fault Injection
```bash
./tests/fault_injection.sh stop-node mongo2
./tests/fault_injection.sh network-partition mongo2
./tests/fault_injection.sh start-node mongo2
./tests/fault_injection.sh restore-network mongo2
```

---

## Common Pitfalls / Known Nuances

- **Arbiter direct reachability**: An arbiter unreachable directly but healthy per `replSetGetStatus` is **OK** — it may be on a segregated network. Do not treat this as CRITICAL.
- **Oplog window thresholds are inverted**: `"mode": "below"` means alert when the window is *smaller* than the threshold.
- **WiredTiger tickets**: MongoDB 7.0+ uses a dynamic pool (max 128). Thresholds are expressed as **usage %**, not absolute counts.
- **Filesystem thresholds scale logarithmically**: On multi-TB volumes, the effective percentage threshold is raised automatically to enforce a minimum absolute free space.
- **Quorum check**: Only PRIMARY (1), SECONDARY (2), and ARBITER (7) states count as "healthy voting" for quorum calculation.
- **pymongo version**: Must be `>=4.0,<5.0`. Do not upgrade to pymongo 5.x.
- **Status escalation**: `set_status()` only raises severity, never lowers it. UNKNOWN only sets when current status is OK.

---

## Icinga2 Integration

The plugin's `CheckCommand` definition and `Service` example are in `README.md`.
Deploy `check_mongodb.py` to `PluginDir` on the Icinga node, install `requirements.txt`, ensure it's executable (`chmod +x`).
