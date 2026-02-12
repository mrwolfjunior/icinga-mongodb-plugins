#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_mongodb.py - Icinga/Nagios plugin for MongoDB monitoring.

Monitors MongoDB instances (single node, ReplicaSet, ReplicaSet+arbiter,
Sharded Cluster) on-prem. Supports LDAP and SCRAM authentication,
SRV and standard connection strings, MongoDB 5.x-8.2.

Exit codes:
    0 = OK
    1 = WARNING
    2 = CRITICAL
    3 = UNKNOWN
"""

import argparse
import json
import math
import sys
import time
import traceback
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, unquote_plus

try:
    import pymongo
    from pymongo import MongoClient
    from pymongo.read_preferences import ReadPreference
    from pymongo.errors import (
        ConnectionFailure,
        OperationFailure,
        ServerSelectionTimeoutError,
        ConfigurationError,
        AutoReconnect,
    )
except ImportError:
    print("UNKNOWN - pymongo is not installed. Install it with: pip install 'pymongo>=4.0,<5.0'")
    sys.exit(3)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

__version__ = "26.02.12"

NAGIOS_OK = 0
NAGIOS_WARNING = 1
NAGIOS_CRITICAL = 2
NAGIOS_UNKNOWN = 3

STATUS_LABELS = {
    NAGIOS_OK: "OK",
    NAGIOS_WARNING: "WARNING",
    NAGIOS_CRITICAL: "CRITICAL",
    NAGIOS_UNKNOWN: "UNKNOWN",
}

# MongoDB replica set member states
RS_STATES = {
    0: "STARTUP",
    1: "PRIMARY",
    2: "SECONDARY",
    3: "RECOVERING",
    5: "STARTUP2",
    6: "UNKNOWN",
    7: "ARBITER",
    8: "DOWN",
    9: "ROLLBACK",
    10: "REMOVED",
}


def bytes_to_gb(value):
    """Convert bytes to GB, rounded to 2 decimal places."""
    return round(value / (1024 ** 3), 2)


def sanitize_metric_name(name):
    """Sanitize metric name: lowercase, replace . : - with _"""
    return name.lower().replace(".", "_").replace(":", "_").replace("-", "_")


# ---------------------------------------------------------------------------
# IcingaOutput — formats plugin output
# ---------------------------------------------------------------------------

class IcingaOutput:
    """Formats output compliant with Icinga/Nagios plugin specification."""

    def __init__(self):
        self.status = NAGIOS_OK
        self.messages = []
        self.perfdata = []
        self.long_output = []

    def set_status(self, status):
        """Set status, but only escalate (OK < WARNING < CRITICAL)."""
        if status == NAGIOS_UNKNOWN and self.status == NAGIOS_OK:
            self.status = NAGIOS_UNKNOWN
        elif status > self.status and status != NAGIOS_UNKNOWN:
            self.status = status
        elif status == NAGIOS_UNKNOWN and self.status not in (NAGIOS_WARNING, NAGIOS_CRITICAL):
            self.status = NAGIOS_UNKNOWN

    def add_message(self, status, message):
        """Add a status message and escalate status if needed."""
        self.set_status(status)
        self.messages.append(message)

    def add_perfdata(self, label, value, uom="", warn="", crit="", min_val="", max_val=""):
        """Add performance data in Nagios format: label=value[UOM];warn;crit;min;max."""
        # Quote labels with spaces
        if " " in label:
            label = f"'{label}'"
        pd = f"{label}={value}{uom};{warn};{crit};{min_val};{max_val}"
        self.perfdata.append(pd)

    def add_long_output(self, line):
        """Add a line to the long (multi-line) output."""
        self.long_output.append(line)

    def get_output(self):
        """Return the formatted plugin output string."""
        status_label = STATUS_LABELS.get(self.status, "UNKNOWN")
        summary = ", ".join(self.messages) if self.messages else "No issues detected"
        first_line = f"{status_label} - {summary}"
        if self.perfdata:
            first_line += " | " + " ".join(self.perfdata)
        lines = [first_line]
        if self.long_output:
            lines.extend(self.long_output)
        return "\n".join(lines)

    def exit(self):
        """Print output and exit with appropriate code."""
        print(self.get_output())
        sys.exit(self.status)


# ---------------------------------------------------------------------------
# MongoConnectionManager — handles connections and URI parsing
# ---------------------------------------------------------------------------

class MongoConnectionManager:
    """Manages MongoDB connections with support for SRV, LDAP, SCRAM auth."""

    def __init__(self, uri, username=None, password=None, auth_mechanism=None,
                 auth_source=None, tls=True, tls_insecure=False, timeout=10):
        if not uri.startswith("mongodb://") and not uri.startswith("mongodb+srv://"):
            self.uri = f"mongodb://{uri}"
        else:
            self.uri = uri
        self.username = username
        self.password = password
        self.auth_mechanism = auth_mechanism
        self.auth_source = auth_source or "admin"
        self.tls = tls
        self.tls_insecure = tls_insecure
        self.timeout = timeout

    def _build_client_kwargs(self, direct_connection=False, server_selection_timeout=None):
        """Build keyword arguments for MongoClient."""
        kwargs = {
            "serverSelectionTimeoutMS": (server_selection_timeout or self.timeout) * 1000,
            "connectTimeoutMS": self.timeout * 1000,
            "socketTimeoutMS": self.timeout * 1000,
        }
        if self.username:
            kwargs["username"] = self.username
        if self.password:
            kwargs["password"] = self.password
        if self.auth_mechanism:
            kwargs["authMechanism"] = self.auth_mechanism
        if self.auth_source:
            kwargs["authSource"] = self.auth_source
        if self.tls:
            kwargs["tls"] = True
            if self.tls_insecure:
                kwargs["tlsAllowInvalidCertificates"] = True
                kwargs["tlsAllowInvalidHostnames"] = True
        if direct_connection:
            kwargs["directConnection"] = True
        else:
            # Prefer primary, but allow secondary reading if primary is down
            kwargs["read_preference"] = ReadPreference.PRIMARY_PREFERRED
        return kwargs

    def connect(self, direct_connection=False):
        """Create a MongoClient using the configured URI."""
        kwargs = self._build_client_kwargs(direct_connection=direct_connection)
        return MongoClient(self.uri, **kwargs)

    def connect_to_node(self, host, port):
        """Create a direct MongoClient to a specific host:port."""
        node_uri = f"mongodb://{host}:{port}/"
        kwargs = self._build_client_kwargs(direct_connection=True)
        return MongoClient(node_uri, **kwargs)

    def parse_hosts_from_uri(self):
        """Extract host:port pairs from the connection URI.

        Returns a list of (host, port) tuples.
        For SRV URIs, performs DNS resolution via pymongo.
        """
        if self.uri.startswith("mongodb+srv://"):
            # Let pymongo resolve SRV; connect briefly to discover topology
            try:
                kwargs = self._build_client_kwargs(server_selection_timeout=self.timeout)
                client = MongoClient(self.uri, **kwargs)
                # Force topology discovery
                client.admin.command("ping")
                nodes = client.nodes
                client.close()
                return [(h, p) for h, p in nodes]
            except Exception:
                # Fallback: parse the SRV hostname for display purposes
                parsed = urlparse(self.uri)
                return [(parsed.hostname, 27017)]
        else:
            # Standard mongodb:// URI
            # Remove scheme and everything after '/'
            uri_part = self.uri
            if uri_part.startswith("mongodb://"):
                uri_part = uri_part[len("mongodb://"):]
            # Remove credentials if present
            if "@" in uri_part:
                uri_part = uri_part.split("@", 1)[1]
            # Remove database and options
            if "/" in uri_part:
                uri_part = uri_part.split("/", 1)[0]
            if "?" in uri_part:
                uri_part = uri_part.split("?", 1)[0]
            hosts = []
            for host_str in uri_part.split(","):
                host_str = host_str.strip()
                if ":" in host_str:
                    h, p = host_str.rsplit(":", 1)
                    hosts.append((h, int(p)))
                else:
                    hosts.append((host_str, 27017))
            return hosts

    def get_replicaset_from_uri(self):
        """Extract replicaSet name from the URI query parameters."""
        parsed = urlparse(self.uri)
        params = parse_qs(parsed.query)
        rs = params.get("replicaSet", [None])
        return rs[0] if rs and rs[0] else None


# ---------------------------------------------------------------------------
# TopologyDetector — detects MongoDB topology
# ---------------------------------------------------------------------------

class TopologyDetector:
    """Detects the topology of the MongoDB deployment."""

    STANDALONE = "standalone"
    REPLICASET = "replicaset"
    SHARDED = "sharded"

    @staticmethod
    def detect(client):
        """Detect topology type. Returns (topology_type, info_dict)."""
        try:
            # Check topology
            # Use 'hello' or 'isMaster' to get node status
            # Use PrimaryPreferred to ensure we can read from Secondary if Primary is down
            admin = client.get_database("admin", read_preference=ReadPreference.PRIMARY_PREFERRED)
            hello = admin.command("hello")
        except OperationFailure:
            # Fallback for older MongoDB
            admin = client.get_database("admin", read_preference=ReadPreference.PRIMARY_PREFERRED)
            hello = admin.command("isMaster")

        info = {
            "hello": hello,
            "hosts": hello.get("hosts", []),
            "passives": hello.get("passives", []),
            "arbiters": hello.get("arbiters", []),
            "setName": hello.get("setName"),
            "ismaster": hello.get("ismaster", hello.get("isWritablePrimary", False)),
            "secondary": hello.get("secondary", False),
            "msg": hello.get("msg", ""),
        }

        # Sharded cluster: mongos returns msg: "isdbgrid"
        if info["msg"] == "isdbgrid":
            return TopologyDetector.SHARDED, info

        # ReplicaSet: has setName
        if info["setName"]:
            return TopologyDetector.REPLICASET, info

        # Standalone
        return TopologyDetector.STANDALONE, info


# ---------------------------------------------------------------------------
# AvailabilityChecker
# ---------------------------------------------------------------------------

class AvailabilityChecker:
    """Checks availability of MongoDB nodes (direct + indirect)."""

    def __init__(self, conn_manager, output, expected_rs_name=None, verbose=False):
        self.conn_manager = conn_manager
        self.output = output
        self.expected_rs_name = expected_rs_name
        self.verbose = verbose

    def check(self):
        """Run the availability check."""
        try:
            client = self.conn_manager.connect()
            topology, info = TopologyDetector.detect(client)
        except (ConnectionFailure, ServerSelectionTimeoutError, ConfigurationError) as e:
            msg = f"Cannot connect to MongoDB: {e}"
            # Enhance error message for Quorum Loss scenarios
            if "No replica set members match selector" in str(e) and "Primary" in str(e):
                msg += " - Quorum Lost / No Primary available"
            
            self.output.add_message(NAGIOS_CRITICAL, msg)
            return
        except Exception as e:
            self.output.add_message(NAGIOS_UNKNOWN,
                                    f"Unexpected error connecting to MongoDB: {e}")
            return

        try:
            if topology == TopologyDetector.STANDALONE:
                self._check_standalone(client, info)
            elif topology == TopologyDetector.REPLICASET:
                self._check_replicaset(client, info)
            elif topology == TopologyDetector.SHARDED:
                self._check_sharded(client, info)
        finally:
            client.close()

    def _check_standalone(self, client, info):
        """Check a standalone MongoDB instance."""
        try:
            client.admin.command("ping")
            self.output.add_message(NAGIOS_OK, "Standalone node is reachable")
        except Exception as e:
            self.output.add_message(NAGIOS_CRITICAL,
                                    f"Standalone node unreachable: {e}")

    def _check_replicaset(self, client, info):
        """Check a ReplicaSet deployment."""
        # Get rsStatus from any connected member
        try:
            rs_status = client.admin.command("replSetGetStatus")
        except OperationFailure as e:
            self.output.add_message(NAGIOS_CRITICAL,
                                    f"Cannot get replSetGetStatus: {e}")
            return
        except Exception as e:
            self.output.add_message(NAGIOS_CRITICAL,
                                    f"Error retrieving replica set status: {e}")
            return

        rs_name = rs_status.get("set", "")
        # Validate RS name
        expected_name = self.expected_rs_name or self.conn_manager.get_replicaset_from_uri()
        if expected_name and rs_name != expected_name:
            self.output.add_message(
                NAGIOS_CRITICAL,
                f"ReplicaSet name mismatch: expected '{expected_name}', got '{rs_name}'. "
                f"Possible misconfiguration — connected to wrong replica set!"
            )
            return

        # Build member info from rsStatus (indirect view)
        indirect_members = {}
        for member in rs_status.get("members", []):
            member_name = member.get("name", "")
            indirect_members[member_name] = {
                "state": member.get("state", -1),
                "stateStr": member.get("stateStr", "UNKNOWN"),
                "health": member.get("health", 0),
                "uptime": member.get("uptime", 0),
            }

        # Determine which nodes to check (from the URI)
        uri_hosts = self.conn_manager.parse_hosts_from_uri()
        uri_host_set = {f"{h}:{p}" for h, p in uri_hosts}

        # Identify extra nodes in RS not in URI
        all_rs_members = set(indirect_members.keys())
        extra_nodes = all_rs_members - uri_host_set
        if extra_nodes and self.verbose:
            self.output.add_long_output(
                f"[INFO] Nodes in RS but not in URI (ignored): {', '.join(extra_nodes)}"
            )

        # Calculate voting members and quorum
        voting_members = 0
        healthy_voting = 0
        for member in rs_status.get("members", []):
            # In MongoDB, configVersion > 0 members are part of config
            # votes field indicates voting membership
            member_config = member.get("configVersion", 1)
            # Default: all members vote unless explicitly set to 0
            # We count from rsStatus members
            voting_members += 1
            if member.get("health", 0) == 1:
                # Count healthy voting members (Primary, Secondary, Arbiter, Recovering, Startup2)
                # 1=Primary, 2=Secondary, 7=Arbiter usually vote.
                # 3=Recovering, 5=Startup2 can also vote but might not be "healthy" for ops.
                # For Quorum check, we strictly want to know if we can elect.
                # We count Primary (1), Secondary (2), Arbiter (7) as healthy voters.
                if member.get("state") in (1, 2, 7):
                    healthy_voting += 1

        majority = (voting_members // 2) + 1
        quorum_ok = healthy_voting >= majority

        # Check each node from the URI
        nodes_ok = 0
        nodes_down = 0
        total_uri_nodes = len(uri_hosts)
        arbiter_issues = []

        for host, port in uri_hosts:
            node_name = f"{host}:{port}"
            indirect_info = indirect_members.get(node_name, None)
            is_arbiter = (indirect_info and indirect_info.get("state") == 7) if indirect_info else False

            # Direct connectivity check
            direct_ok = False
            direct_error = None
            try:
                node_client = self.conn_manager.connect_to_node(host, port)
                node_client.admin.command("ping")
                direct_ok = True
                node_client.close()
            except Exception as e:
                direct_error = str(e)

            except Exception as e:
                direct_error = str(e)

            safe_node = sanitize_metric_name(node_name)
            safe_rs_name = sanitize_metric_name(rs_name)

            if direct_ok:
                # Direct connection succeeded — verify consistency with indirect
                if indirect_info:
                    indirect_state = indirect_info.get("state", -1)
                    indirect_health = indirect_info.get("health", 0)

                    if indirect_health == 1 and indirect_state in (1, 2, 7):
                        # Both direct and indirect say OK
                        nodes_ok += 1
                        self.output.add_perfdata(f"rs_{safe_rs_name}_{safe_node}_state", 1, "", "", "", "0", "1")
                        if self.verbose:
                            self.output.add_long_output(
                                f"[OK] {node_name}: direct=OK, indirect={indirect_info['stateStr']}"
                            )
                    else:
                        # Direct OK but indirect says it's not healthy
                        self.output.add_message(
                            NAGIOS_CRITICAL,
                            f"Node {node_name}: reachable directly but RS reports state "
                            f"'{indirect_info['stateStr']}' — possible split-brain or recovery"
                        )
                        nodes_down += 1
                        self.output.add_perfdata(f"rs_{safe_rs_name}_{safe_node}_state", 0, "", "", "", "0", "1")
                else:
                    # Direct OK but not found in rsStatus — might be a misconfiguration
                    self.output.add_message(
                        NAGIOS_CRITICAL,
                        f"Node {node_name}: reachable but not found in replSetGetStatus — "
                        f"possible wrong node or misconfigured URI"
                    )
                    nodes_down += 1
                    self.output.add_perfdata(f"rs_{safe_rs_name}_{safe_node}_state", 0, "", "", "", "0", "1")
            else:
                # Direct connection failed
                if is_arbiter and indirect_info and indirect_info.get("health") == 1:
                    # Arbiter unreachable but RS says it's healthy — OK
                    # (arbiter may be on a segregated network)
                    nodes_ok += 1
                    arbiter_issues.append(node_name)
                    self.output.add_perfdata(f"rs_{safe_rs_name}_{safe_node}_state", 1, "", "", "", "0", "1")
                    if self.verbose:
                        self.output.add_long_output(
                            f"[INFO] Arbiter {node_name}: not reachable directly but RS reports "
                            f"healthy — likely on segregated network"
                        )
                elif indirect_info:
                    indirect_health = indirect_info.get("health", 0)
                    indirect_state_str = indirect_info.get("stateStr", "UNKNOWN")
                    if indirect_health == 1 and indirect_info.get("state") in (1, 2):
                        # Node is healthy per RS but we can't reach it — network issue
                        self.output.add_message(
                            NAGIOS_CRITICAL,
                            f"Node {node_name}: RS reports '{indirect_state_str}' but node is "
                            f"unreachable from monitoring — network issue detected"
                        )
                    else:
                        self.output.add_message(
                            NAGIOS_CRITICAL,
                            f"Node {node_name}: down (RS reports '{indirect_state_str}', "
                            f"direct connection failed: {direct_error})"
                        )
                    nodes_down += 1
                    self.output.add_perfdata(f"rs_{safe_rs_name}_{safe_node}_state", 0, "", "", "", "0", "1")
                else:
                    self.output.add_message(
                        NAGIOS_CRITICAL,
                        f"Node {node_name}: unreachable and not found in RS status "
                        f"(error: {direct_error})"
                    )
                    nodes_down += 1
                    self.output.add_perfdata(f"rs_{safe_rs_name}_{safe_node}_state", 0, "", "", "", "0", "1")

        # Summary
        if nodes_down > 0:
            self.output.add_message(
                NAGIOS_CRITICAL,
                f"RS '{rs_name}': {nodes_down}/{total_uri_nodes} node(s) down"
            )
        elif nodes_ok == total_uri_nodes:
            self.output.add_message(
                NAGIOS_OK,
                f"RS '{rs_name}': all {total_uri_nodes} monitored nodes healthy"
            )

        if arbiter_issues:
            self.output.add_long_output(
                f"[INFO] Arbiter(s) not directly reachable but RS healthy: "
                f"{', '.join(arbiter_issues)}"
            )

        if not quorum_ok:
            self.output.add_message(
                NAGIOS_CRITICAL,
                f"RS '{rs_name}': quorum LOST — only {healthy_voting}/{voting_members} "
                f"voting members healthy (need {majority})"
            )
        elif self.verbose:
            self.output.add_long_output(
                f"[OK] Quorum OK: {healthy_voting}/{voting_members} voting members "
                f"healthy (majority={majority})"
            )

        # Perfdata
        safe_rs_name = sanitize_metric_name(rs_name)
        self.output.add_perfdata(f"rs_{safe_rs_name}_nodes_ok", nodes_ok, "", "", "", "0", str(total_uri_nodes))
        self.output.add_perfdata(f"rs_{safe_rs_name}_nodes_down", nodes_down, "", "", "", "0", str(total_uri_nodes))
        self.output.add_perfdata(f"rs_{safe_rs_name}_quorum", "1" if quorum_ok else "0", "", "", "", "0", "1")

    def _check_sharded(self, client, info):
        """Check a Sharded Cluster deployment."""
        # 1. Check mongos connectivity (we're already connected to one)
        try:
            client.admin.command("ping")
        except Exception as e:
            self.output.add_message(NAGIOS_CRITICAL, f"mongos unreachable: {e}")
            return

        # 2. Get list of shards
        try:
            shards_info = client.admin.command("listShards")
        except OperationFailure as e:
            self.output.add_message(NAGIOS_CRITICAL, f"Cannot list shards: {e}")
            return

        shards = shards_info.get("shards", [])
        if not shards:
            self.output.add_message(NAGIOS_CRITICAL, "Sharded cluster has no shards configured")
            return

        # 3. Check each mongos from URI
        uri_hosts = self.conn_manager.parse_hosts_from_uri()
        mongos_ok = 0
        mongos_down = 0
        for host, port in uri_hosts:
            node_name = f"{host}:{port}"
            safe_mongos = sanitize_metric_name(node_name)
            try:
                node_client = self.conn_manager.connect_to_node(host, port)
                node_client.admin.command("ping")
                # Verify it's actually a mongos
                node_hello = node_client.admin.command("hello")
                if node_hello.get("msg") != "isdbgrid":
                    self.output.add_message(
                        NAGIOS_CRITICAL,
                        f"Node {node_name}: expected mongos but got a different topology type"
                    )
                    mongos_down += 1
                    self.output.add_perfdata(f"mongos_{safe_mongos}_state", 0, "", "", "", "0", "1")
                else:
                    mongos_ok += 1
                    self.output.add_perfdata(f"mongos_{safe_mongos}_state", 1, "", "", "", "0", "1")
                node_client.close()
            except Exception as e:
                self.output.add_message(NAGIOS_CRITICAL,
                                        f"mongos {node_name} unreachable: {e}")
                mongos_down += 1
                self.output.add_perfdata(f"mongos_{safe_mongos}_state", 0, "", "", "", "0", "1")

        if mongos_down == 0:
            self.output.add_message(NAGIOS_OK,
                                    f"All {mongos_ok} mongos nodes reachable")
        else:
            self.output.add_message(
                NAGIOS_CRITICAL,
                f"{mongos_down}/{mongos_ok + mongos_down} mongos node(s) unreachable"
            )

        # 4. Check each shard's replicaset health
        for shard in shards:
            shard_id = shard.get("_id", "unknown")
            safe_shard_id = sanitize_metric_name(shard_id)
            shard_host = shard.get("host", "")
            # shard host format: "rsName/host1:port1,host2:port2,..."
            if "/" in shard_host:
                shard_rs_name, shard_nodes_str = shard_host.split("/", 1)
            else:
                shard_rs_name = None
                shard_nodes_str = shard_host

            shard_nodes = []
            for node_str in shard_nodes_str.split(","):
                node_str = node_str.strip()
                if ":" in node_str:
                    h, p = node_str.rsplit(":", 1)
                    shard_nodes.append((h, int(p)))
                else:
                    shard_nodes.append((node_str, 27017))

            if not shard_nodes:
                self.output.add_message(NAGIOS_CRITICAL,
                                        f"Shard '{shard_id}': no nodes found")
                continue

            # Connect to the first reachable node of the shard
            shard_client = None
            for h, p in shard_nodes:
                try:
                    shard_client = self.conn_manager.connect_to_node(h, p)
                    shard_client.admin.command("ping")
                    break
                except Exception:
                    shard_client = None
                    continue

            if shard_client is None:
                self.output.add_message(
                    NAGIOS_CRITICAL,
                    f"Shard '{shard_id}' ({shard_rs_name}): no nodes reachable"
                )
                continue

            try:
                shard_rs_status = shard_client.admin.command("replSetGetStatus")
                actual_rs_name = shard_rs_status.get("set", "")

                if shard_rs_name and actual_rs_name != shard_rs_name:
                    self.output.add_message(
                        NAGIOS_CRITICAL,
                        f"Shard '{shard_id}': RS name mismatch — expected "
                        f"'{shard_rs_name}', got '{actual_rs_name}'"
                    )
                    continue

                # Check members
                shard_ok = 0
                shard_down = 0
                for member in shard_rs_status.get("members", []):
                    member_name = member.get("name", "")
                    state = member.get("state", -1)
                    health = member.get("health", 0)
                    state_str = member.get("stateStr", "UNKNOWN")

                    safe_member = sanitize_metric_name(member_name)
                    if health == 1 and state in (1, 2, 7):
                        shard_ok += 1
                        self.output.add_perfdata(f"shard_{safe_shard_id}_{safe_member}_state", 1, "", "", "", "0", "1")
                    else:
                        shard_down += 1
                        self.output.add_perfdata(f"shard_{safe_shard_id}_{safe_member}_state", 0, "", "", "", "0", "1")
                        self.output.add_message(
                            NAGIOS_CRITICAL,
                            f"Shard '{shard_id}' member {member_name}: {state_str}"
                        )

                total_shard_members = shard_ok + shard_down
                if shard_down == 0:
                    self.output.add_message(
                        NAGIOS_OK,
                        f"Shard '{shard_id}' ({actual_rs_name}): all {shard_ok} members healthy"
                    )
                self.output.add_perfdata(
                    f"shard_{safe_shard_id}_ok", shard_ok, "", "", "", "0", str(total_shard_members)
                )
                self.output.add_perfdata(
                    f"shard_{safe_shard_id}_down", shard_down, "", "", "", "0", str(total_shard_members)
                )
            except Exception as e:
                self.output.add_message(
                    NAGIOS_CRITICAL,
                    f"Shard '{shard_id}': error getting RS status: {e}"
                )
            finally:
                shard_client.close()

        # 5. Check config server RS
        try:
            config_info = client.admin.command("getShardMap")
            config_host = config_info.get("map", {}).get("config", "")
            if config_host:
                if "/" in config_host:
                    cfg_rs_name, cfg_nodes_str = config_host.split("/", 1)
                else:
                    cfg_rs_name = None
                    cfg_nodes_str = config_host

                cfg_nodes = []
                for node_str in cfg_nodes_str.split(","):
                    node_str = node_str.strip()
                    if ":" in node_str:
                        h, p = node_str.rsplit(":", 1)
                        cfg_nodes.append((h, int(p)))
                    else:
                        cfg_nodes.append((node_str, 27017))

                cfg_client = None
                for h, p in cfg_nodes:
                    try:
                        cfg_client = self.conn_manager.connect_to_node(h, p)
                        cfg_client.admin.command("ping")
                        break
                    except Exception:
                        cfg_client = None

                if cfg_client:
                    try:
                        cfg_status = cfg_client.admin.command("replSetGetStatus")
                        cfg_ok = 0
                        cfg_down = 0
                        for m in cfg_status.get("members", []):
                            m_name = m.get("name", "")
                            safe_m = sanitize_metric_name(m_name)
                            if m.get("health", 0) == 1 and m.get("state", -1) in (1, 2):
                                cfg_ok += 1
                                self.output.add_perfdata(f"config_{safe_m}_state", 1, "", "", "", "0", "1")
                            else:
                                cfg_down += 1
                                self.output.add_perfdata(f"config_{safe_m}_state", 0, "", "", "", "0", "1")
                        cfg_total = cfg_ok + cfg_down

                        if cfg_down > 0:
                            self.output.add_message(
                                NAGIOS_CRITICAL,
                                f"Config RS: {cfg_down}/{cfg_total} member(s) down"
                            )
                        else:
                            self.output.add_message(
                                NAGIOS_OK,
                                f"Config RS ({cfg_rs_name}): all {cfg_ok} members healthy"
                            )
                        self.output.add_perfdata("config_rs_ok", cfg_ok, "", "", "", "0", str(cfg_total))
                        self.output.add_perfdata("config_rs_down", cfg_down, "", "", "", "0", str(cfg_total))
                    except Exception as e:
                        self.output.add_message(NAGIOS_CRITICAL,
                                                f"Config RS status error: {e}")
                    finally:
                        cfg_client.close()
                else:
                    self.output.add_message(NAGIOS_CRITICAL,
                                            "Config RS: no config servers reachable")
        except Exception as e:
            if self.verbose:
                self.output.add_long_output(f"[WARN] Could not check config RS: {e}")

        # Perfdata for mongos
        self.output.add_perfdata("mongos_ok", mongos_ok, "", "", "", "0",
                                 str(mongos_ok + mongos_down))
        self.output.add_perfdata("mongos_down", mongos_down, "", "", "", "0",
                                 str(mongos_ok + mongos_down))


# ---------------------------------------------------------------------------
# ThresholdEngine
# ---------------------------------------------------------------------------

class ThresholdEngine:
    """Evaluate metrics against user-defined thresholds.

    Accepts a dict of thresholds in the format:
        {
            "metric_key": {"warning": <float>, "critical": <float>, "mode": "above"|"below"},
            ...
        }

    Mode 'above' (default): alert when value >= threshold.
    Mode 'below': alert when value <= threshold (e.g. oplog window, tickets).
    """

    def __init__(self, thresholds_dict=None):
        self.thresholds = thresholds_dict or {}

    @classmethod
    def from_json(cls, json_str):
        """Parse a JSON string into a ThresholdEngine."""
        if not json_str:
            return cls()
        try:
            data = json.loads(json_str)
            if not isinstance(data, dict):
                raise ValueError("--thresholds must be a JSON object")
            return cls(data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON for --thresholds: {e}")

    def check(self, metric_key, value, output, node_name, unit=""):
        """Evaluate a metric value against configured thresholds.

        Returns True if an alert was raised, False otherwise.
        """
        if metric_key not in self.thresholds:
            return False

        cfg = self.thresholds[metric_key]
        warn = cfg.get("warning")
        crit = cfg.get("critical")
        mode = cfg.get("mode", "above")

        if mode == "below":
            if crit is not None and value <= crit:
                output.add_message(
                    NAGIOS_CRITICAL,
                    f"Node {node_name}: {metric_key} {value:.1f}{unit} "
                    f"<= {crit}{unit}"
                )
                return True
            if warn is not None and value <= warn:
                output.add_message(
                    NAGIOS_WARNING,
                    f"Node {node_name}: {metric_key} {value:.1f}{unit} "
                    f"<= {warn}{unit}"
                )
                return True
        else:  # above
            if crit is not None and value >= crit:
                output.add_message(
                    NAGIOS_CRITICAL,
                    f"Node {node_name}: {metric_key} {value:.1f}{unit} "
                    f">= {crit}{unit}"
                )
                return True
            if warn is not None and value >= warn:
                output.add_message(
                    NAGIOS_WARNING,
                    f"Node {node_name}: {metric_key} {value:.1f}{unit} "
                    f">= {warn}{unit}"
                )
                return True
        return False

    def has(self, metric_key):
        """Check if a threshold is configured for a metric."""
        return metric_key in self.thresholds


# ---------------------------------------------------------------------------
# MetricsChecker
# ---------------------------------------------------------------------------

class MetricsChecker:
    """Extracts performance metrics from each MongoDB node."""

    def __init__(self, conn_manager, output, thresholds=None, verbose=False):
        self.conn_manager = conn_manager
        self.output = output
        self.thresholds = thresholds or ThresholdEngine()
        self.verbose = verbose

    def handle_permission_error(self, error, metric_keys):
        """Handle permission errors based on threshold configuration."""
        # check if any of the affected keys are in the threshold configuration
        needed = any(k in self.thresholds.thresholds for k in metric_keys)
        
        if needed:
            self.output.add_message(
                NAGIOS_CRITICAL,
                f"Permission denied for metrics {metric_keys} (required by thresholds): {error}"
            )
        else:
            # Downgrade to WARNING if not explicitly monitored
            self.output.add_message(
                NAGIOS_WARNING,
                f"Permission denied for metrics {metric_keys} (ignored): {error}"
            )

    def check(self):
        """Run metrics check on each node in the URI."""
        uri_hosts = self.conn_manager.parse_hosts_from_uri()

        if not uri_hosts:
            self.output.add_message(NAGIOS_UNKNOWN, "No hosts found in URI")
            return

        all_ok = True
        for host, port in uri_hosts:
            node_name = f"{host}:{port}"
            try:
                client = self.conn_manager.connect_to_node(host, port)
                self._collect_metrics(client, node_name)
                client.close()
            except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                self.output.add_message(
                    NAGIOS_CRITICAL,
                    f"Node {node_name} unreachable: {e}"
                )
                all_ok = False
            except Exception as e:
                self.output.add_message(
                    NAGIOS_CRITICAL,
                    f"Node {node_name} error collecting metrics: {e}"
                )
                all_ok = False

        if all_ok and not self.output.messages:
            self.output.add_message(NAGIOS_OK, "All nodes metrics collected successfully")

    def _collect_metrics(self, client, node_name):
        """Collect serverStatus metrics from a single node."""
        server_status = client.admin.command("serverStatus")
        safe_name = sanitize_metric_name(node_name)
        te = self.thresholds

        # --- Connections ---
        connections = server_status.get("connections", {})
        conn_current = connections.get("current", 0)
        conn_available = connections.get("available", 0)
        conn_active = connections.get("active", 0)
        conn_total_created = connections.get("totalCreated", 0)
        conn_total = conn_current + conn_available
        conn_usage_pct = (conn_current / conn_total * 100) if conn_total > 0 else 0

        self.output.add_perfdata(f"{safe_name}_conn_current", conn_current, "", "", "", "0", str(conn_total))
        self.output.add_perfdata(f"{safe_name}_conn_available", conn_available, "", "", "", "0", str(conn_total))
        self.output.add_perfdata(f"{safe_name}_conn_active", conn_active)
        self.output.add_perfdata(f"{safe_name}_conn_usage_pct", f"{conn_usage_pct:.1f}", "%")
        self.output.add_perfdata(f"{safe_name}_conn_total_created", conn_total_created, "c")

        te.check("conn_usage_pct", conn_usage_pct, self.output, node_name, "%")

        # --- Opcounters ---
        opcounters = server_status.get("opcounters", {})
        for op in ("insert", "query", "update", "delete", "getmore", "command"):
            val = opcounters.get(op, 0)
            self.output.add_perfdata(f"{safe_name}_ops_{op}", val, "c")

        # --- Global Lock / Queue ---
        global_lock = server_status.get("globalLock", {})
        current_queue = global_lock.get("currentQueue", {})
        queue_readers = current_queue.get("readers", 0)
        queue_writers = current_queue.get("writers", 0)
        queue_total = current_queue.get("total", 0)
        self.output.add_perfdata(f"{safe_name}_queue_readers", queue_readers)
        self.output.add_perfdata(f"{safe_name}_queue_writers", queue_writers)
        self.output.add_perfdata(f"{safe_name}_queue_total", queue_total)

        te.check("queue_total", queue_total, self.output, node_name)

        # --- WiredTiger Cache ---
        wt = server_status.get("wiredTiger", {})
        if wt:
            cache = wt.get("cache", {})
            cache_max = cache.get("maximum bytes configured", 0)
            cache_used = cache.get("bytes currently in the cache", 0)
            cache_dirty = cache.get("tracked dirty bytes in the cache", 0)
            cache_read = cache.get("bytes read into cache", 0)
            cache_written = cache.get("bytes written from cache", 0)
            cache_usage_pct = (cache_used / cache_max * 100) if cache_max > 0 else 0

            self.output.add_perfdata(f"{safe_name}_wt_cache_max", bytes_to_gb(cache_max), "GB")
            self.output.add_perfdata(f"{safe_name}_wt_cache_used", bytes_to_gb(cache_used), "GB")
            self.output.add_perfdata(f"{safe_name}_wt_cache_dirty", bytes_to_gb(cache_dirty), "GB")
            self.output.add_perfdata(f"{safe_name}_wt_cache_pct", f"{cache_usage_pct:.1f}", "%")
            self.output.add_perfdata(f"{safe_name}_wt_cache_read", bytes_to_gb(cache_read), "GB")
            self.output.add_perfdata(f"{safe_name}_wt_cache_written", bytes_to_gb(cache_written), "GB")

            # Cache eviction metrics
            evict_modified = cache.get("modified pages evicted", 0)
            evict_unmodified = cache.get("unmodified pages evicted", 0)
            pages_read = cache.get("pages read into cache", 0)
            pages_written = cache.get("pages written from cache", 0)
            self.output.add_perfdata(f"{safe_name}_wt_evict_modified", evict_modified, "c")
            self.output.add_perfdata(f"{safe_name}_wt_evict_unmodified", evict_unmodified, "c")
            self.output.add_perfdata(f"{safe_name}_wt_pages_read", pages_read, "c")
            self.output.add_perfdata(f"{safe_name}_wt_pages_written", pages_written, "c")

            te.check("cache_usage_pct", cache_usage_pct, self.output, node_name, "%")

            # --- WiredTiger Tickets (concurrency) ---
            ct = wt.get("concurrentTransactions", {})
            for rw in ("read", "write"):
                rw_data = ct.get(rw, {})
                tk_available = rw_data.get("available", 0)
                tk_out = rw_data.get("out", 0)
                tk_total = rw_data.get("totalTickets", 0)
                tk_usage_pct = (tk_out / tk_total * 100) if tk_total > 0 else 0

                self.output.add_perfdata(f"{safe_name}_tickets_{rw}_available", tk_available)
                self.output.add_perfdata(f"{safe_name}_tickets_{rw}_out", tk_out)
                self.output.add_perfdata(f"{safe_name}_tickets_{rw}_total", tk_total)
                self.output.add_perfdata(f"{safe_name}_tickets_{rw}_usage_pct", f"{tk_usage_pct:.1f}", "%")

                te.check(f"tickets_{rw}_pct", tk_usage_pct, self.output, node_name, "%")

        # --- Replication Lag (if replicaset member) ---
        # --- Replication Lag (if replicaset member) ---
        try:
            rs_status = client.admin.command("replSetGetStatus")
            primary_optime = None
            my_optime = None
            for member in rs_status.get("members", []):
                if member.get("state") == 1:  # PRIMARY
                    primary_optime = member.get("optimeDate")
                if member.get("self", False):
                    my_optime = member.get("optimeDate")
            if primary_optime and my_optime and primary_optime != my_optime:
                lag_seconds = abs((primary_optime - my_optime).total_seconds())
                self.output.add_perfdata(f"{safe_name}_repl_lag", f"{lag_seconds:.1f}", "s")
                te.check("repl_lag", lag_seconds, self.output, node_name, "s")
            elif primary_optime and my_optime:
                self.output.add_perfdata(f"{safe_name}_repl_lag", "0", "s")
        except OperationFailure as e:
            if e.code == 13:  # Unauthorized
                self.handle_permission_error(e, ["repl_lag"])
            # else: Not a replicaset member — skip replication lag
        except Exception:
            pass

        # --- Oplog Window (if replicaset member) ---
        try:
            oplog_coll = client.local["oplog.rs"]
            first_entry = oplog_coll.find().sort("$natural", pymongo.ASCENDING).limit(1)
            last_entry = oplog_coll.find().sort("$natural", pymongo.DESCENDING).limit(1)
            first_ts = None
            last_ts = None
            for doc in first_entry:
                first_ts = doc.get("ts")
            for doc in last_entry:
                last_ts = doc.get("ts")
            if first_ts and last_ts:
                oplog_window_secs = last_ts.time - first_ts.time
                oplog_window_hours = oplog_window_secs / 3600.0
                self.output.add_perfdata(
                    f"{safe_name}_oplog_window", f"{oplog_window_hours:.1f}", "h"
                )
                te.check("oplog_window", oplog_window_hours, self.output, node_name, "h")
        except OperationFailure as e:
            if e.code == 13:
                self.handle_permission_error(e, ["oplog_window"])
        except Exception:
            # Not a replicaset member or oplog not available
            pass

        # --- Oplog Size (if replicaset member) ---
        try:
            oplog_stats = client.local.command("collStats", "oplog.rs")
            oplog_max = oplog_stats.get("maxSize", oplog_stats.get("size", 0))
            oplog_used = oplog_stats.get("storageSize", oplog_stats.get("size", 0))
            self.output.add_perfdata(f"{safe_name}_oplog_max_size", bytes_to_gb(oplog_max), "GB")
            self.output.add_perfdata(f"{safe_name}_oplog_used_size", bytes_to_gb(oplog_used), "GB")
        except OperationFailure as e:
            if e.code == 13:
                self.handle_permission_error(e, ["oplog_max_size", "oplog_used_size"])
        except Exception:
            pass

        # --- Memory ---
        mem = server_status.get("mem", {})
        self.output.add_perfdata(f"{safe_name}_mem_resident", mem.get("resident", 0), "MB")
        self.output.add_perfdata(f"{safe_name}_mem_virtual", mem.get("virtual", 0), "MB")

        # --- Network ---
        network = server_status.get("network", {})
        self.output.add_perfdata(f"{safe_name}_net_in", bytes_to_gb(network.get("bytesIn", 0)), "GB")
        self.output.add_perfdata(f"{safe_name}_net_out", bytes_to_gb(network.get("bytesOut", 0)), "GB")
        self.output.add_perfdata(f"{safe_name}_net_requests", network.get("numRequests", 0), "c")

        # --- Document Operations ---
        metrics_data = server_status.get("metrics", {})
        document = metrics_data.get("document", {})
        for op in ("inserted", "updated", "deleted", "returned"):
            self.output.add_perfdata(f"{safe_name}_doc_{op}", document.get(op, 0), "c")

        # --- Cursors ---
        cursor_data = metrics_data.get("cursor", {})
        cursor_open = cursor_data.get("open", {})
        cursor_open_total = cursor_open.get("total", 0)
        cursor_open_no_timeout = cursor_open.get("noTimeout", 0)
        cursor_timed_out = cursor_data.get("timedOut", 0)
        self.output.add_perfdata(f"{safe_name}_cursor_open", cursor_open_total)
        self.output.add_perfdata(f"{safe_name}_cursor_open_no_timeout", cursor_open_no_timeout)
        self.output.add_perfdata(f"{safe_name}_cursor_timed_out", cursor_timed_out, "c")

        te.check("cursor_open", cursor_open_total, self.output, node_name)
        te.check("cursor_timed_out", cursor_timed_out, self.output, node_name)

        # --- Page Faults ---
        extra_info = server_status.get("extra_info", {})
        self.output.add_perfdata(f"{safe_name}_page_faults",
                                 extra_info.get("page_faults", 0), "c")

        # --- Active Clients ---
        active_clients = global_lock.get("activeClients", {})
        self.output.add_perfdata(f"{safe_name}_active_readers", active_clients.get("readers", 0))
        self.output.add_perfdata(f"{safe_name}_active_writers", active_clients.get("writers", 0))

        # --- Assertions ---
        asserts = server_status.get("asserts", {})
        for atype in ("regular", "warning", "msg", "user", "rollovers"):
            self.output.add_perfdata(f"{safe_name}_asserts_{atype}", asserts.get(atype, 0), "c")

        te.check("assertions_regular", asserts.get("regular", 0), self.output, node_name)
        te.check("assertions_warning", asserts.get("warning", 0), self.output, node_name)

        # --- Transactions (MongoDB 4.0+) ---
        txn = server_status.get("transactions", {})
        if txn:
            self.output.add_perfdata(f"{safe_name}_txn_current_active", txn.get("currentActive", 0))
            self.output.add_perfdata(f"{safe_name}_txn_current_open", txn.get("currentOpen", 0))
            self.output.add_perfdata(f"{safe_name}_txn_total_started", txn.get("totalStarted", 0), "c")
            self.output.add_perfdata(f"{safe_name}_txn_total_committed", txn.get("totalCommitted", 0), "c")
            self.output.add_perfdata(f"{safe_name}_txn_total_aborted", txn.get("totalAborted", 0), "c")

        # --- Database Stats (aggregated) ---
        try:
            db_names = client.list_database_names()
            total_data_size = 0
            total_storage_size = 0
            total_index_size = 0
            total_collections = 0
            total_objects = 0

            for db_name in db_names:
                try:
                    db_stats = client[db_name].command("dbStats")
                    total_data_size += db_stats.get("dataSize", 0)
                    total_storage_size += db_stats.get("storageSize", 0)
                    total_index_size += db_stats.get("indexSize", 0)
                    total_collections += db_stats.get("collections", 0)
                    total_objects += db_stats.get("objects", 0)
                except Exception:
                    pass

            self.output.add_perfdata(f"{safe_name}_total_data_size", bytes_to_gb(total_data_size), "GB")
            self.output.add_perfdata(f"{safe_name}_total_storage_size", bytes_to_gb(total_storage_size), "GB")
            self.output.add_perfdata(f"{safe_name}_total_index_size", bytes_to_gb(total_index_size), "GB")
            self.output.add_perfdata(f"{safe_name}_total_collections", total_collections, "c")
            self.output.add_perfdata(f"{safe_name}_total_objects", total_objects, "c")
        except OperationFailure as e:
            if e.code == 13:
                # Metrics derived from dbStats
                affected = ["total_data_size", "total_storage_size", "total_index_size"]
                self.handle_permission_error(e, affected)
            elif self.verbose:
                 self.output.add_long_output(f"[WARN] {node_name}: could not list databases: {e}")
        except Exception as e:
            if self.verbose:
                self.output.add_long_output(f"[WARN] {node_name}: could not list databases: {e}")

        # --- Filesystem ---
        try:
            admin_stats = client.admin.command("dbStats")
            fs_total = admin_stats.get("fsTotalSize", 0)
            fs_used = admin_stats.get("fsUsedSize", 0)
            if fs_total > 0:
                fs_free = fs_total - fs_used
                fs_usage_pct = (fs_used / fs_total) * 100
                self.output.add_perfdata(f"{safe_name}_fs_total", bytes_to_gb(fs_total), "GB")
                self.output.add_perfdata(f"{safe_name}_fs_used", bytes_to_gb(fs_used), "GB")
                self.output.add_perfdata(f"{safe_name}_fs_free", bytes_to_gb(fs_free), "GB")
                self.output.add_perfdata(f"{safe_name}_fs_used_pct", f"{fs_usage_pct:.1f}", "%")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# FilesystemChecker
# ---------------------------------------------------------------------------

class FilesystemChecker:
    """Checks filesystem usage on MongoDB nodes using dbStats."""

    def __init__(self, conn_manager, output, thresholds=None, verbose=False):
        self.conn_manager = conn_manager
        self.output = output
        self.verbose = verbose
        # Extract base warning/critical from ThresholdEngine, defaults 85/95
        if thresholds and thresholds.has("fs_usage_pct"):
            cfg = thresholds.thresholds["fs_usage_pct"]
            self.warning = cfg.get("warning", 85.0)
            self.critical = cfg.get("critical", 95.0)
        else:
            self.warning = 85.0
            self.critical = 95.0

    @staticmethod
    def dynamic_threshold(total_bytes, base_threshold_pct):
        """Calculate a dynamic threshold that adapts to volume size.

        For small volumes (<500GB): uses the threshold as-is.
        For large volumes: scales logarithmically so more absolute
        free space is required.

        Examples with base_threshold_pct=90:
            100GB  -> threshold 90% (10GB free)
            1TB    -> threshold ~93% (~70GB free)
            5TB    -> threshold ~95% (~250GB free)
            10TB   -> threshold ~96% (~400GB free)
        """
        total_gb = total_bytes / (1024 ** 3)
        if total_gb <= 500:
            return base_threshold_pct
        scale = math.log10(total_gb / 500) * 5
        return min(base_threshold_pct + scale, 99.0)

    def check(self):
        """Run filesystem check on each node."""
        uri_hosts = self.conn_manager.parse_hosts_from_uri()

        if not uri_hosts:
            self.output.add_message(NAGIOS_UNKNOWN, "No hosts found in URI")
            return

        all_ok = True
        for host, port in uri_hosts:
            node_name = f"{host}:{port}"
            try:
                client = self.conn_manager.connect_to_node(host, port)
                self._check_node_fs(client, node_name)
                client.close()
            except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                self.output.add_message(
                    NAGIOS_CRITICAL,
                    f"Node {node_name} unreachable: {e}"
                )
                all_ok = False
            except Exception as e:
                self.output.add_message(
                    NAGIOS_CRITICAL,
                    f"Node {node_name} error checking filesystem: {e}"
                )
                all_ok = False

        if all_ok and not self.output.messages:
            self.output.add_message(NAGIOS_OK,
                                    "Filesystem usage within thresholds on all nodes")

    def _check_node_fs(self, client, node_name):
        """Check filesystem usage on a single node."""
        db_stats = client.admin.command("dbStats")
        safe_name = sanitize_metric_name(node_name)

        fs_total = db_stats.get("fsTotalSize")
        fs_used = db_stats.get("fsUsedSize")

        if fs_total is None or fs_used is None:
            self.output.add_message(
                NAGIOS_UNKNOWN,
                f"Node {node_name}: fsTotalSize/fsUsedSize not available "
                f"(not supported on this deployment type)"
            )
            return

        if fs_total == 0:
            self.output.add_message(
                NAGIOS_UNKNOWN,
                f"Node {node_name}: fsTotalSize is 0 — cannot calculate usage"
            )
            return

        usage_pct = (fs_used / fs_total) * 100
        free_bytes = fs_total - fs_used
        free_gb = free_bytes / (1024 ** 3)
        total_gb = fs_total / (1024 ** 3)

        # Calculate dynamic thresholds
        effective_warn = self.dynamic_threshold(fs_total, self.warning)
        effective_crit = self.dynamic_threshold(fs_total, self.critical)

        # Perfdata
        self.output.add_perfdata(
            f"{safe_name}_fs_used_pct", f"{usage_pct:.1f}", "%",
            f"{effective_warn:.1f}", f"{effective_crit:.1f}", "0", "100"
        )
        self.output.add_perfdata(f"{safe_name}_fs_total", bytes_to_gb(fs_total), "GB")
        self.output.add_perfdata(f"{safe_name}_fs_used", bytes_to_gb(fs_used), "GB")
        self.output.add_perfdata(f"{safe_name}_fs_free", bytes_to_gb(free_bytes), "GB")

        # Evaluate thresholds
        if usage_pct >= effective_crit:
            self.output.add_message(
                NAGIOS_CRITICAL,
                f"Node {node_name}: filesystem {usage_pct:.1f}% used "
                f"({free_gb:.1f}GB free of {total_gb:.1f}GB, "
                f"dynamic critical threshold: {effective_crit:.1f}%)"
            )
        elif usage_pct >= effective_warn:
            self.output.add_message(
                NAGIOS_WARNING,
                f"Node {node_name}: filesystem {usage_pct:.1f}% used "
                f"({free_gb:.1f}GB free of {total_gb:.1f}GB, "
                f"dynamic warning threshold: {effective_warn:.1f}%)"
            )
        else:
            if self.verbose:
                self.output.add_long_output(
                    f"[OK] {node_name}: filesystem {usage_pct:.1f}% used "
                    f"({free_gb:.1f}GB free of {total_gb:.1f}GB)"
                )



# ---------------------------------------------------------------------------
# MongoDB version detection
# ---------------------------------------------------------------------------

def get_mongo_version(client):
    """Get MongoDB server version as a tuple (major, minor, patch)."""
    try:
        build_info = client.admin.command("buildInfo")
        version_str = build_info.get("version", "0.0.0")
        parts = version_str.split(".")
        return tuple(int(p) for p in parts[:3])
    except Exception:
        return (0, 0, 0)


# ---------------------------------------------------------------------------
# Argument Parsing
# ---------------------------------------------------------------------------

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Icinga/Nagios plugin for MongoDB monitoring",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Check modes (exactly one required):
  --availability    Check node/replica set availability and topology
  --metrics         Collect performance metrics per node
  --filesystem      Check filesystem usage with dynamic thresholds

Examples:
  # Check ReplicaSet availability
  %(prog)s --uri "mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=myRS" \\
           --availability --username admin --password secret

  # Check metrics with connection usage thresholds
  %(prog)s --uri "mongodb://host1:27017/" --metrics --warning 80 --critical 90

  # Check filesystem with dynamic thresholds
  %(prog)s --uri "mongodb://host1:27017,host2:27017/" --filesystem \\
           --warning 85 --critical 95

  # Collect all metrics (perfdata + thresholds)
  %(prog)s --uri "mongodb://host1:27017/" --metrics

  # Using SRV connection string with LDAP auth
  %(prog)s --uri "mongodb+srv://cluster.example.com/" --availability \\
           --username ldapuser --password secret --auth-mechanism PLAIN

Exit codes:
  0 = OK       - Everything is fine
  1 = WARNING  - Threshold exceeded (warning level)
  2 = CRITICAL - Node(s) down, threshold exceeded (critical), or error
  3 = UNKNOWN  - Plugin error or unsupported check
        """
    )

    parser.add_argument("--version", action="version",
                        version=f"%(prog)s {__version__}")

    # Connection
    parser.add_argument("--uri", required=True,
                        help="MongoDB connection string (mongodb:// or mongodb+srv://)")
    parser.add_argument("--username", "-u", default=None,
                        help="Username for authentication")
    parser.add_argument("--password", "-p", default=None,
                        help="Password for authentication")
    parser.add_argument("--auth-mechanism", default=None,
                        choices=["SCRAM-SHA-256", "SCRAM-SHA-1", "PLAIN"],
                        help="Authentication mechanism (default: auto, PLAIN for LDAP)")
    parser.add_argument("--auth-source", default="admin",
                        help="Authentication database (default: admin, use '$external' for LDAP)")
    parser.add_argument("--tls", action="store_true", default=False,
                        help="Enable TLS/SSL connection")
    parser.add_argument("--tls-insecure", action="store_true", default=False,
                        help="Disable TLS certificate verification")
    parser.add_argument("--timeout", type=int, default=10,
                        help="Connection timeout in seconds (default: 10)")

    # Check modes (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--availability", action="store_true",
                            help="Check availability of all nodes")
    mode_group.add_argument("--metrics", action="store_true",
                            help="Collect performance metrics per node")
    mode_group.add_argument("--filesystem", action="store_true",
                            help="Check filesystem usage with dynamic thresholds")

    # Thresholds
    parser.add_argument("--thresholds", type=str, default=None,
                        help='JSON thresholds, e.g. '
                             '\'{"\'conn_usage_pct\': {"warning": 80, "critical": 90}, '
                             '"oplog_window": {"warning": 48, "critical": 24, "mode": "below"}, '
                             '"fs_usage_pct": {"warning": 85, "critical": 95}}\''
                        )

    # Additional options
    parser.add_argument("--replicaset", default=None,
                        help="Expected replica set name (overrides URI parameter)")
    parser.add_argument("--verbose", "-v", action="store_true", default=False,
                        help="Enable verbose output for debugging")

    args = parser.parse_args()

    # Validate thresholds
    if args.thresholds:
        try:
            data = json.loads(args.thresholds)
            if not isinstance(data, dict):
                parser.error("--thresholds must be a JSON object")
        except json.JSONDecodeError as e:
            parser.error(f"Invalid JSON for --thresholds: {e}")

    # Auto-set auth source for LDAP
    if args.auth_mechanism == "PLAIN" and args.auth_source == "admin":
        args.auth_source = "$external"

    return args


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """Main entry point."""
    output = IcingaOutput()

    try:
        args = parse_arguments()

        conn_manager = MongoConnectionManager(
            uri=args.uri,
            username=args.username,
            password=args.password,
            auth_mechanism=args.auth_mechanism,
            auth_source=args.auth_source,
            tls=args.tls,
            tls_insecure=args.tls_insecure,
            timeout=args.timeout,
        )

        if args.availability:
            checker = AvailabilityChecker(
                conn_manager=conn_manager,
                output=output,
                expected_rs_name=args.replicaset,
                verbose=args.verbose,
            )
            checker.check()

        elif args.metrics:
            # Build ThresholdEngine from --thresholds JSON
            thresholds_dict = {}
            if args.thresholds:
                thresholds_dict = json.loads(args.thresholds)

            te = ThresholdEngine(thresholds_dict)

            checker = MetricsChecker(
                conn_manager=conn_manager,
                output=output,
                thresholds=te,
                verbose=args.verbose,
            )
            checker.check()

        elif args.filesystem:
            # Build ThresholdEngine from --thresholds JSON
            thresholds_dict = {}
            if args.thresholds:
                thresholds_dict = json.loads(args.thresholds)

            te = ThresholdEngine(thresholds_dict)

            checker = FilesystemChecker(
                conn_manager=conn_manager,
                output=output,
                thresholds=te,
                verbose=args.verbose,
            )
            checker.check()

    except SystemExit as e:
        # argparse calls sys.exit — re-raise to avoid catching it
        raise
    except Exception as e:
        output.add_message(NAGIOS_UNKNOWN, f"Plugin error: {e}")
        if "--verbose" in sys.argv or "-v" in sys.argv:
            output.add_long_output(traceback.format_exc())

    output.exit()


if __name__ == "__main__":
    main()
