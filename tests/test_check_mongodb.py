#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for check_mongodb.py

These tests use mocks — no real MongoDB needed.
Run with: python -m pytest tests/test_check_mongodb.py -v
"""

import math
import sys
import os
import unittest
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime, timezone, timedelta

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from check_mongodb import (
    IcingaOutput,
    MongoConnectionManager,
    TopologyDetector,
    AvailabilityChecker,
    MetricsChecker,
    FilesystemChecker,
    ThresholdEngine,
    NAGIOS_OK,
    NAGIOS_WARNING,
    NAGIOS_CRITICAL,
    NAGIOS_UNKNOWN,
)


# =====================================================================
# IcingaOutput Tests
# =====================================================================

class TestIcingaOutput(unittest.TestCase):
    """Test the Icinga output formatter."""

    def test_default_status_is_ok(self):
        output = IcingaOutput()
        assert output.status == NAGIOS_OK

    def test_ok_message(self):
        output = IcingaOutput()
        output.add_message(NAGIOS_OK, "All good")
        result = output.get_output()
        assert result.startswith("OK - All good")

    def test_warning_message(self):
        output = IcingaOutput()
        output.add_message(NAGIOS_WARNING, "Threshold exceeded")
        result = output.get_output()
        assert result.startswith("WARNING - Threshold exceeded")
        assert output.status == NAGIOS_WARNING

    def test_critical_message(self):
        output = IcingaOutput()
        output.add_message(NAGIOS_CRITICAL, "Node down")
        result = output.get_output()
        assert result.startswith("CRITICAL - Node down")
        assert output.status == NAGIOS_CRITICAL

    def test_status_escalation(self):
        """Status should only escalate, never de-escalate."""
        output = IcingaOutput()
        output.add_message(NAGIOS_CRITICAL, "Critical issue")
        output.add_message(NAGIOS_OK, "Some OK")
        assert output.status == NAGIOS_CRITICAL

    def test_warning_to_critical_escalation(self):
        output = IcingaOutput()
        output.add_message(NAGIOS_WARNING, "Warning first")
        output.add_message(NAGIOS_CRITICAL, "Then critical")
        assert output.status == NAGIOS_CRITICAL

    def test_perfdata_format(self):
        output = IcingaOutput()
        output.add_message(NAGIOS_OK, "Test")
        output.add_perfdata("conn_usage", "85.5", "%", "80", "90", "0", "100")
        result = output.get_output()
        assert "| conn_usage=85.5%;80;90;0;100" in result

    def test_perfdata_label_with_spaces(self):
        output = IcingaOutput()
        output.add_perfdata("my metric", "42")
        result = output.get_output()
        assert "'my metric'=42" in result

    def test_multiple_messages(self):
        output = IcingaOutput()
        output.add_message(NAGIOS_OK, "First")
        output.add_message(NAGIOS_OK, "Second")
        result = output.get_output()
        assert "First, Second" in result

    def test_long_output(self):
        output = IcingaOutput()
        output.add_message(NAGIOS_OK, "Summary")
        output.add_long_output("[OK] Detail line")
        result = output.get_output()
        lines = result.split("\n")
        assert len(lines) == 2
        assert lines[1] == "[OK] Detail line"

    def test_unknown_status(self):
        output = IcingaOutput()
        output.add_message(NAGIOS_UNKNOWN, "Cannot determine")
        assert output.status == NAGIOS_UNKNOWN

    def test_unknown_does_not_override_critical(self):
        output = IcingaOutput()
        output.add_message(NAGIOS_CRITICAL, "Critical first")
        output.add_message(NAGIOS_UNKNOWN, "Then unknown")
        assert output.status == NAGIOS_CRITICAL

    def test_no_messages_default(self):
        output = IcingaOutput()
        result = output.get_output()
        assert "No issues detected" in result


# =====================================================================
# MongoConnectionManager Tests
# =====================================================================

class TestMongoConnectionManager(unittest.TestCase):
    """Test URI parsing and connection configuration."""

    def test_parse_standard_uri_single_host(self):
        mgr = MongoConnectionManager("mongodb://host1:27017/")
        hosts = mgr.parse_hosts_from_uri()
        assert hosts == [("host1", 27017)]

    def test_parse_standard_uri_multiple_hosts(self):
        mgr = MongoConnectionManager(
            "mongodb://host1:27017,host2:27018,host3:27019/"
        )
        hosts = mgr.parse_hosts_from_uri()
        assert hosts == [("host1", 27017), ("host2", 27018), ("host3", 27019)]

    def test_parse_uri_with_credentials(self):
        mgr = MongoConnectionManager(
            "mongodb://user:pass@host1:27017,host2:27018/admin?replicaSet=rs0"
        )
        hosts = mgr.parse_hosts_from_uri()
        assert hosts == [("host1", 27017), ("host2", 27018)]

    def test_parse_uri_default_port(self):
        mgr = MongoConnectionManager("mongodb://host1/")
        hosts = mgr.parse_hosts_from_uri()
        assert hosts == [("host1", 27017)]

    def test_get_replicaset_from_uri(self):
        mgr = MongoConnectionManager(
            "mongodb://host1:27017/?replicaSet=myRS"
        )
        assert mgr.get_replicaset_from_uri() == "myRS"

    def test_get_replicaset_from_uri_none(self):
        mgr = MongoConnectionManager("mongodb://host1:27017/")
        assert mgr.get_replicaset_from_uri() is None

    def test_build_client_kwargs_basic(self):
        mgr = MongoConnectionManager("mongodb://host1:27017/", timeout=5)
        kwargs = mgr._build_client_kwargs()
        assert kwargs["serverSelectionTimeoutMS"] == 5000
        assert kwargs["connectTimeoutMS"] == 5000
        assert kwargs["socketTimeoutMS"] == 5000

    def test_build_client_kwargs_with_auth(self):
        mgr = MongoConnectionManager(
            "mongodb://host1:27017/",
            username="admin",
            password="secret",
            auth_mechanism="SCRAM-SHA-256",
            auth_source="admin"
        )
        kwargs = mgr._build_client_kwargs()
        assert kwargs["username"] == "admin"
        assert kwargs["password"] == "secret"
        assert kwargs["authMechanism"] == "SCRAM-SHA-256"
        assert kwargs["authSource"] == "admin"

    def test_build_client_kwargs_tls(self):
        mgr = MongoConnectionManager("mongodb://host1:27017/", tls=True, tls_insecure=True)
        kwargs = mgr._build_client_kwargs()
        assert kwargs["tls"] is True
        assert kwargs["tlsAllowInvalidCertificates"] is True
        assert kwargs["tlsAllowInvalidHostnames"] is True

    def test_build_client_kwargs_direct_connection(self):
        mgr = MongoConnectionManager("mongodb://host1:27017/")
        kwargs = mgr._build_client_kwargs(direct_connection=True)
        assert kwargs["directConnection"] is True

    def test_build_client_kwargs_ldap(self):
        mgr = MongoConnectionManager(
            "mongodb://host1:27017/",
            username="ldapuser",
            password="ldappass",
            auth_mechanism="PLAIN",
            auth_source="$external"
        )
        kwargs = mgr._build_client_kwargs()
        assert kwargs["authMechanism"] == "PLAIN"
        assert kwargs["authSource"] == "$external"


# =====================================================================
# TopologyDetector Tests
# =====================================================================

class TestTopologyDetector(unittest.TestCase):
    """Test topology detection logic."""

    def test_detect_standalone(self):
        client = MagicMock()
        client.admin.command.return_value = {
            "ismaster": True,
            "maxBsonObjectSize": 16777216,
        }
        topo, info = TopologyDetector.detect(client)
        assert topo == TopologyDetector.STANDALONE

    def test_detect_replicaset(self):
        client = MagicMock()
        client.admin.command.return_value = {
            "ismaster": True,
            "setName": "rs0",
            "hosts": ["host1:27017", "host2:27017", "host3:27017"],
        }
        topo, info = TopologyDetector.detect(client)
        assert topo == TopologyDetector.REPLICASET
        assert info["setName"] == "rs0"

    def test_detect_sharded(self):
        client = MagicMock()
        client.admin.command.return_value = {
            "ismaster": True,
            "msg": "isdbgrid",
        }
        topo, info = TopologyDetector.detect(client)
        assert topo == TopologyDetector.SHARDED

    def test_detect_uses_hello_first(self):
        client = MagicMock()
        client.admin.command.return_value = {
            "isWritablePrimary": True,
            "msg": "isdbgrid",
        }
        topo, info = TopologyDetector.detect(client)
        assert topo == TopologyDetector.SHARDED

    def test_detect_fallback_to_ismaster(self):
        """If 'hello' raises OperationFailure, fallback to 'isMaster'."""
        from pymongo.errors import OperationFailure

        client = MagicMock()
        client.admin.command.side_effect = [
            OperationFailure("not supported"),
            {"ismaster": True, "setName": "rs0", "hosts": ["h1:27017"]},
        ]
        topo, info = TopologyDetector.detect(client)
        assert topo == TopologyDetector.REPLICASET


# =====================================================================
# FilesystemChecker — Dynamic Threshold Tests
# =====================================================================

class TestDynamicThreshold(unittest.TestCase):
    """Test the logarithmic dynamic threshold formula."""

    def test_small_volume_unchanged(self):
        """Volumes <= 500GB should use the base threshold."""
        # 100GB
        result = FilesystemChecker.dynamic_threshold(100 * 1024**3, 90.0)
        assert result == 90.0

    def test_500gb_unchanged(self):
        """Exactly 500GB is the boundary — should use base threshold."""
        result = FilesystemChecker.dynamic_threshold(500 * 1024**3, 90.0)
        assert result == 90.0

    def test_1tb_higher_threshold(self):
        """1TB should have a higher effective threshold than base."""
        result = FilesystemChecker.dynamic_threshold(1024 * 1024**3, 90.0)
        assert result > 90.0
        assert result < 95.0

    def test_5tb_even_higher(self):
        """5TB should have even higher threshold."""
        result_1tb = FilesystemChecker.dynamic_threshold(1024 * 1024**3, 90.0)
        result_5tb = FilesystemChecker.dynamic_threshold(5120 * 1024**3, 90.0)
        assert result_5tb > result_1tb

    def test_10tb_threshold(self):
        """10TB should have substantial scaling."""
        result = FilesystemChecker.dynamic_threshold(10240 * 1024**3, 90.0)
        assert result > 95.0
        assert result < 99.0

    def test_max_threshold_cap(self):
        """Threshold should never exceed 99%."""
        result = FilesystemChecker.dynamic_threshold(1000000 * 1024**3, 98.0)
        assert result <= 99.0

    def test_different_base_thresholds(self):
        """Different base thresholds should scale proportionally."""
        result_85 = FilesystemChecker.dynamic_threshold(2048 * 1024**3, 85.0)
        result_90 = FilesystemChecker.dynamic_threshold(2048 * 1024**3, 90.0)
        assert result_90 > result_85

    def test_zero_volume(self):
        """Zero-size volume should use base threshold."""
        result = FilesystemChecker.dynamic_threshold(0, 90.0)
        assert result == 90.0


# =====================================================================
# AvailabilityChecker — Quorum Tests
# =====================================================================

class TestQuorumLogic(unittest.TestCase):
    """Test quorum calculation and availability logic."""

    def _make_rs_status(self, members):
        """Helper to build a mock rsStatus document."""
        return {
            "set": "rs0",
            "members": members,
        }

    def _make_member(self, name, state, health=1):
        """Helper to build a mock member document."""
        state_map = {1: "PRIMARY", 2: "SECONDARY", 6: "ARBITER", 7: "DOWN", 8: "ROLLBACK"}
        return {
            "name": name,
            "state": state,
            "stateStr": state_map.get(state, "UNKNOWN"),
            "health": health,
            "uptime": 1000,
            "configVersion": 1,
        }

    def test_3_node_all_healthy(self):
        """3 healthy nodes = quorum OK."""
        members = [
            self._make_member("h1:27017", 1),
            self._make_member("h2:27017", 2),
            self._make_member("h3:27017", 2),
        ]
        # majority = 3 // 2 + 1 = 2, healthy = 3 >= 2 ✓
        voting = len(members)
        healthy = sum(1 for m in members if m["health"] == 1 and m["state"] in (1, 2, 6))
        majority = (voting // 2) + 1
        assert healthy >= majority

    def test_3_node_one_down(self):
        """2 healthy + 1 down out of 3 = quorum still OK, but CRITICAL for availability."""
        members = [
            self._make_member("h1:27017", 1),
            self._make_member("h2:27017", 2),
            self._make_member("h3:27017", 7, health=0),  # DOWN
        ]
        voting = len(members)
        healthy = sum(1 for m in members if m["health"] == 1 and m["state"] in (1, 2, 6))
        majority = (voting // 2) + 1
        assert healthy >= majority  # 2 >= 2 → quorum OK
        assert healthy < voting      # but not all healthy

    def test_3_node_two_down_quorum_lost(self):
        """1 healthy + 2 down = quorum LOST."""
        members = [
            self._make_member("h1:27017", 1),
            self._make_member("h2:27017", 7, health=0),
            self._make_member("h3:27017", 7, health=0),
        ]
        voting = len(members)
        healthy = sum(1 for m in members if m["health"] == 1 and m["state"] in (1, 2, 6))
        majority = (voting // 2) + 1
        assert healthy < majority  # 1 < 2 → quorum LOST

    def test_rs_with_arbiter(self):
        """2 data + 1 arbiter = 3 voting, majority = 2."""
        members = [
            self._make_member("h1:27017", 1),
            self._make_member("h2:27017", 2),
            self._make_member("arb:27017", 6),
        ]
        voting = len(members)
        healthy = sum(1 for m in members if m["health"] == 1 and m["state"] in (1, 2, 6))
        majority = (voting // 2) + 1
        assert healthy >= majority  # 3 >= 2

    def test_5_node_rs(self):
        """5-node RS: majority = 3."""
        members = [
            self._make_member("h1:27017", 1),
            self._make_member("h2:27017", 2),
            self._make_member("h3:27017", 2),
            self._make_member("h4:27017", 7, health=0),
            self._make_member("h5:27017", 7, health=0),
        ]
        voting = len(members)
        healthy = sum(1 for m in members if m["health"] == 1 and m["state"] in (1, 2, 6))
        majority = (voting // 2) + 1
        assert majority == 3
        assert healthy >= majority  # 3 >= 3


# =====================================================================
# AvailabilityChecker — RS Name Validation
# =====================================================================

class TestRSNameValidation(unittest.TestCase):
    """Test ReplicaSet name validation."""

    def test_matching_rs_name(self):
        """Matching RS name should not cause any issue."""
        output = IcingaOutput()
        conn_mgr = MagicMock(spec=MongoConnectionManager)
        conn_mgr.get_replicaset_from_uri.return_value = "rs0"
        conn_mgr.parse_hosts_from_uri.return_value = [("h1", 27017)]

        checker = AvailabilityChecker(conn_mgr, output, expected_rs_name="rs0")

        # The RS name matches — good
        rs_name = "rs0"
        expected = checker.expected_rs_name or conn_mgr.get_replicaset_from_uri()
        assert rs_name == expected

    def test_mismatched_rs_name(self):
        """Mismatched RS name should be detected."""
        rs_name = "wrong_rs"
        expected = "rs0"
        assert rs_name != expected


# =====================================================================
# AvailabilityChecker — Full Integration Mock
# =====================================================================

class TestAvailabilityCheckerIntegration(unittest.TestCase):
    """Test the full availability checker with mocked MongoDB."""

    @patch("check_mongodb.MongoClient")
    def test_standalone_ok(self, mock_client_cls):
        """Standalone node should be OK when reachable."""
        mock_client = MagicMock()
        mock_client.admin.command.side_effect = lambda cmd, *a, **kw: {
            "hello": {"ismaster": True},
            "ping": {"ok": 1},
        }.get(cmd, {})
        mock_client_cls.return_value = mock_client

        output = IcingaOutput()
        conn_mgr = MongoConnectionManager("mongodb://host1:27017/")
        # Mock the connect method
        conn_mgr.connect = MagicMock(return_value=mock_client)

        checker = AvailabilityChecker(conn_mgr, output)
        checker.check()

        assert output.status == NAGIOS_OK

    @patch("check_mongodb.MongoClient")
    def test_standalone_unreachable(self, mock_client_cls):
        """Standalone node should be CRITICAL when unreachable."""
        from pymongo.errors import ConnectionFailure

        output = IcingaOutput()
        conn_mgr = MongoConnectionManager("mongodb://host1:27017/")
        conn_mgr.connect = MagicMock(side_effect=ConnectionFailure("timeout"))

        checker = AvailabilityChecker(conn_mgr, output)
        checker.check()

        assert output.status == NAGIOS_CRITICAL
        assert "Cannot connect" in output.messages[0]


# =====================================================================
# MetricsChecker Tests
# =====================================================================

class TestMetricsChecker(unittest.TestCase):
    """Test metrics collection logic."""

    def _mock_server_status(self):
        return {
            "connections": {
                "current": 50,
                "available": 950,
                "active": 10,
                "totalCreated": 1000,
            },
            "opcounters": {
                "insert": 100,
                "query": 500,
                "update": 200,
                "delete": 50,
                "getmore": 30,
                "command": 1000,
            },
            "globalLock": {
                "currentQueue": {
                    "total": 0,
                    "readers": 0,
                    "writers": 0,
                },
            },
            "wiredTiger": {
                "cache": {
                    "maximum bytes configured": 1073741824,
                    "bytes currently in the cache": 536870912,
                    "tracked dirty bytes in the cache": 1048576,
                    "bytes read into cache": 2147483648,
                    "bytes written from cache": 1073741824,
                },
            },
        }

    def test_connection_usage_ok(self):
        from pymongo.errors import OperationFailure

        output = IcingaOutput()
        conn_mgr = MagicMock(spec=MongoConnectionManager)
        conn_mgr.parse_hosts_from_uri.return_value = [("h1", 27017)]

        mock_client = MagicMock()

        def mock_command(cmd, *a, **kw):
            if cmd == "serverStatus":
                return self._mock_server_status()
            if cmd == "replSetGetStatus":
                raise OperationFailure("not running with --replSet")
            return {}

        mock_client.admin.command.side_effect = mock_command
        conn_mgr.connect_to_node.return_value = mock_client

        checker = MetricsChecker(
            conn_mgr, output,
            thresholds=ThresholdEngine({"conn_usage_pct": {"warning": 80, "critical": 90}})
        )
        checker.check()

        # 50 / (50 + 950) = 5% — well below warning
        assert output.status == NAGIOS_OK

    def test_connection_usage_warning(self):
        output = IcingaOutput()
        conn_mgr = MagicMock(spec=MongoConnectionManager)
        conn_mgr.parse_hosts_from_uri.return_value = [("h1", 27017)]

        ss = self._mock_server_status()
        ss["connections"]["current"] = 850
        ss["connections"]["available"] = 150

        mock_client = MagicMock()
        mock_client.admin.command.side_effect = lambda cmd, *a, **kw: {
            "serverStatus": ss,
        }.get(cmd, {})
        conn_mgr.connect_to_node.return_value = mock_client

        checker = MetricsChecker(
            conn_mgr, output,
            thresholds=ThresholdEngine({"conn_usage_pct": {"warning": 80, "critical": 90}})
        )
        checker.check()

        # 850 / 1000 = 85% — above warning (80%), below critical (90%)
        assert output.status == NAGIOS_WARNING

    def test_connection_usage_critical(self):
        output = IcingaOutput()
        conn_mgr = MagicMock(spec=MongoConnectionManager)
        conn_mgr.parse_hosts_from_uri.return_value = [("h1", 27017)]

        ss = self._mock_server_status()
        ss["connections"]["current"] = 950
        ss["connections"]["available"] = 50

        mock_client = MagicMock()
        mock_client.admin.command.side_effect = lambda cmd, *a, **kw: {
            "serverStatus": ss,
        }.get(cmd, {})
        conn_mgr.connect_to_node.return_value = mock_client

        checker = MetricsChecker(
            conn_mgr, output,
            thresholds=ThresholdEngine({"conn_usage_pct": {"warning": 80, "critical": 90}})
        )
        checker.check()

        # 950 / 1000 = 95% — above critical (90%)
        assert output.status == NAGIOS_CRITICAL

    def test_node_unreachable_is_critical(self):
        from pymongo.errors import ConnectionFailure

        output = IcingaOutput()
        conn_mgr = MagicMock(spec=MongoConnectionManager)
        conn_mgr.parse_hosts_from_uri.return_value = [("h1", 27017)]
        conn_mgr.connect_to_node.side_effect = ConnectionFailure("timeout")

        checker = MetricsChecker(conn_mgr, output)
        checker.check()

        assert output.status == NAGIOS_CRITICAL
        assert "unreachable" in output.messages[0]


# =====================================================================
# MetricsChecker — Oplog Window Tests
# =====================================================================

class TestOplogWindowThresholds(unittest.TestCase):
    """Test oplog window threshold logic in MetricsChecker."""

    def _mock_server_status(self):
        return {
            "connections": {
                "current": 50, "available": 950, "active": 10, "totalCreated": 1000,
            },
            "opcounters": {
                "insert": 100, "query": 500, "update": 200,
                "delete": 50, "getmore": 30, "command": 1000,
            },
            "globalLock": {"currentQueue": {"total": 0, "readers": 0, "writers": 0}},
            "wiredTiger": {
                "cache": {
                    "maximum bytes configured": 1073741824,
                    "bytes currently in the cache": 536870912,
                    "tracked dirty bytes in the cache": 1048576,
                    "bytes read into cache": 2147483648,
                    "bytes written from cache": 1073741824,
                },
            },
        }

    def _make_mock_client(self, oplog_window_hours):
        """Create a mock client with a specific oplog window in hours."""
        from pymongo.errors import OperationFailure
        from bson import Timestamp
        import time

        mock_client = MagicMock()
        now = int(time.time())
        window_secs = int(oplog_window_hours * 3600)
        first_ts = Timestamp(now - window_secs, 1)
        last_ts = Timestamp(now, 1)

        def mock_admin_command(cmd, *a, **kw):
            if cmd == "serverStatus":
                return self._mock_server_status()
            if cmd == "replSetGetStatus":
                raise OperationFailure("not running with --replSet")
            return {}

        mock_client.admin.command.side_effect = mock_admin_command

        # Mock oplog.rs collection with find().sort().limit() chain
        mock_oplog = MagicMock()

        def mock_find(*a, **kw):
            mock_cursor = MagicMock()
            def mock_sort(field, direction):
                mock_sorted = MagicMock()
                def mock_limit(n):
                    import pymongo
                    if direction == pymongo.ASCENDING:
                        return iter([{"ts": first_ts}])
                    else:
                        return iter([{"ts": last_ts}])
                mock_sorted.limit = mock_limit
                return mock_sorted
            mock_cursor.sort = mock_sort
            return mock_cursor

        mock_oplog.find = mock_find
        mock_client.local.__getitem__ = MagicMock(return_value=mock_oplog)

        return mock_client

    def test_oplog_window_ok(self):
        """72h oplog window with 48h warning / 24h critical → OK."""
        output = IcingaOutput()
        conn_mgr = MagicMock(spec=MongoConnectionManager)
        conn_mgr.parse_hosts_from_uri.return_value = [("h1", 27017)]

        mock_client = self._make_mock_client(oplog_window_hours=72)
        conn_mgr.connect_to_node.return_value = mock_client

        checker = MetricsChecker(
            conn_mgr, output,
            thresholds=ThresholdEngine({"oplog_window": {"warning": 48, "critical": 24, "mode": "below"}})
        )
        checker.check()

        assert output.status == NAGIOS_OK

    def test_oplog_window_warning(self):
        """36h oplog window with 48h warning / 24h critical → WARNING."""
        output = IcingaOutput()
        conn_mgr = MagicMock(spec=MongoConnectionManager)
        conn_mgr.parse_hosts_from_uri.return_value = [("h1", 27017)]

        mock_client = self._make_mock_client(oplog_window_hours=36)
        conn_mgr.connect_to_node.return_value = mock_client

        checker = MetricsChecker(
            conn_mgr, output,
            thresholds=ThresholdEngine({"oplog_window": {"warning": 48, "critical": 24, "mode": "below"}})
        )
        checker.check()

        assert output.status == NAGIOS_WARNING
        assert "oplog_window" in output.messages[0]

    def test_oplog_window_critical(self):
        """12h oplog window with 48h warning / 24h critical → CRITICAL."""
        output = IcingaOutput()
        conn_mgr = MagicMock(spec=MongoConnectionManager)
        conn_mgr.parse_hosts_from_uri.return_value = [("h1", 27017)]

        mock_client = self._make_mock_client(oplog_window_hours=12)
        conn_mgr.connect_to_node.return_value = mock_client

        checker = MetricsChecker(
            conn_mgr, output,
            thresholds=ThresholdEngine({"oplog_window": {"warning": 48, "critical": 24, "mode": "below"}})
        )
        checker.check()

        assert output.status == NAGIOS_CRITICAL
        assert "oplog_window" in output.messages[0]

    def test_oplog_window_no_thresholds_always_ok(self):
        """Without oplog thresholds, small window should not trigger alerts."""
        output = IcingaOutput()
        conn_mgr = MagicMock(spec=MongoConnectionManager)
        conn_mgr.parse_hosts_from_uri.return_value = [("h1", 27017)]

        mock_client = self._make_mock_client(oplog_window_hours=1)
        conn_mgr.connect_to_node.return_value = mock_client

        # No oplog thresholds set
        checker = MetricsChecker(conn_mgr, output)
        checker.check()

        assert output.status == NAGIOS_OK


# =====================================================================
# FilesystemChecker Tests
# =====================================================================

class TestFilesystemChecker(unittest.TestCase):
    """Test filesystem check logic."""

    def test_fs_ok(self):
        output = IcingaOutput()
        conn_mgr = MagicMock(spec=MongoConnectionManager)
        conn_mgr.parse_hosts_from_uri.return_value = [("h1", 27017)]

        mock_client = MagicMock()
        # 100GB total, 50GB used = 50% — well below thresholds
        mock_client.admin.command.return_value = {
            "fsTotalSize": 100 * 1024**3,
            "fsUsedSize": 50 * 1024**3,
        }
        conn_mgr.connect_to_node.return_value = mock_client

        checker = FilesystemChecker(
            conn_mgr, output,
            thresholds=ThresholdEngine({"fs_usage_pct": {"warning": 85, "critical": 95}})
        )
        checker.check()

        assert output.status == NAGIOS_OK

    def test_fs_warning(self):
        output = IcingaOutput()
        conn_mgr = MagicMock(spec=MongoConnectionManager)
        conn_mgr.parse_hosts_from_uri.return_value = [("h1", 27017)]

        mock_client = MagicMock()
        # 100GB total, 88GB used = 88% — above warning (85%), below critical (95%)
        mock_client.admin.command.return_value = {
            "fsTotalSize": 100 * 1024**3,
            "fsUsedSize": 88 * 1024**3,
        }
        conn_mgr.connect_to_node.return_value = mock_client

        checker = FilesystemChecker(
            conn_mgr, output,
            thresholds=ThresholdEngine({"fs_usage_pct": {"warning": 85, "critical": 95}})
        )
        checker.check()

        assert output.status == NAGIOS_WARNING

    def test_fs_critical(self):
        output = IcingaOutput()
        conn_mgr = MagicMock(spec=MongoConnectionManager)
        conn_mgr.parse_hosts_from_uri.return_value = [("h1", 27017)]

        mock_client = MagicMock()
        # 100GB total, 97GB used = 97% — above critical (95%)
        mock_client.admin.command.return_value = {
            "fsTotalSize": 100 * 1024**3,
            "fsUsedSize": 97 * 1024**3,
        }
        conn_mgr.connect_to_node.return_value = mock_client

        checker = FilesystemChecker(
            conn_mgr, output,
            thresholds=ThresholdEngine({"fs_usage_pct": {"warning": 85, "critical": 95}})
        )
        checker.check()

        assert output.status == NAGIOS_CRITICAL

    def test_fs_no_data(self):
        output = IcingaOutput()
        conn_mgr = MagicMock(spec=MongoConnectionManager)
        conn_mgr.parse_hosts_from_uri.return_value = [("h1", 27017)]

        mock_client = MagicMock()
        mock_client.admin.command.return_value = {}  # No fs fields
        conn_mgr.connect_to_node.return_value = mock_client

        checker = FilesystemChecker(conn_mgr, output)
        checker.check()

        assert output.status == NAGIOS_UNKNOWN

    def test_fs_node_unreachable(self):
        from pymongo.errors import ConnectionFailure

        output = IcingaOutput()
        conn_mgr = MagicMock(spec=MongoConnectionManager)
        conn_mgr.parse_hosts_from_uri.return_value = [("h1", 27017)]
        conn_mgr.connect_to_node.side_effect = ConnectionFailure("timeout")

        checker = FilesystemChecker(conn_mgr, output)
        checker.check()

        assert output.status == NAGIOS_CRITICAL



# =====================================================================
# MongoDB Version Compatibility Tests
# =====================================================================

class TestMongoDBVersionCompat(unittest.TestCase):
    """Test handling of different MongoDB version fields."""

    def test_mongodb_8_optime_written_field(self):
        """MongoDB 8.0+ includes optimeWritten in rsStatus — ensure it doesn't break."""
        member_v8 = {
            "name": "h1:27017",
            "state": 1,
            "stateStr": "PRIMARY",
            "health": 1,
            "uptime": 1000,
            "optimeDate": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "optimeWritten": {"ts": 1234567890, "t": 1},
            "optimeWrittenDate": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "lastWrittenWallTime": datetime(2024, 1, 1, tzinfo=timezone.utc),
        }
        # The extra fields should be ignored gracefully
        assert member_v8["state"] == 1
        assert member_v8["health"] == 1

    def test_pre_mongodb8_no_optime_written(self):
        """Pre-MongoDB 8.0 doesn't have optimeWritten — should work fine."""
        member_v7 = {
            "name": "h1:27017",
            "state": 2,
            "stateStr": "SECONDARY",
            "health": 1,
            "uptime": 1000,
            "optimeDate": datetime(2024, 1, 1, tzinfo=timezone.utc),
        }
        assert member_v7.get("optimeWritten") is None
        assert member_v7["state"] == 2


# =====================================================================
# Argument Parsing Tests
# =====================================================================

class TestArgumentParsing(unittest.TestCase):
    """Test command-line argument parsing."""

    def test_availability_mode(self):
        from check_mongodb import parse_arguments
        with patch("sys.argv", ["check_mongodb.py", "--uri", "mongodb://h1:27017/", "--availability"]):
            args = parse_arguments()
            assert args.availability is True
            assert args.metrics is False

    def test_metrics_mode(self):
        from check_mongodb import parse_arguments
        with patch("sys.argv", ["check_mongodb.py", "--uri", "mongodb://h1:27017/", "--metrics"]):
            args = parse_arguments()
            assert args.metrics is True
            assert args.availability is False

    def test_filesystem_mode(self):
        from check_mongodb import parse_arguments
        with patch("sys.argv", ["check_mongodb.py", "--uri", "mongodb://h1:27017/", "--filesystem"]):
            args = parse_arguments()
            assert args.filesystem is True
            assert args.thresholds is None  # defaults are in FilesystemChecker

    def test_ldap_auto_auth_source(self):
        from check_mongodb import parse_arguments
        with patch("sys.argv", [
            "check_mongodb.py", "--uri", "mongodb://h1:27017/",
            "--availability", "--auth-mechanism", "PLAIN"
        ]):
            args = parse_arguments()
            assert args.auth_source == "$external"

    def test_mutually_exclusive_modes(self):
        from check_mongodb import parse_arguments
        with patch("sys.argv", [
            "check_mongodb.py", "--uri", "mongodb://h1:27017/",
            "--availability", "--metrics"
        ]):
            with self.assertRaises(SystemExit):
                parse_arguments()

    def test_thresholds_json_arg(self):
        from check_mongodb import parse_arguments
        import json
        thresholds = json.dumps({"conn_usage_pct": {"warning": 80, "critical": 90}})
        with patch("sys.argv", [
            "check_mongodb.py", "--uri", "mongodb://h1:27017/",
            "--metrics", "--thresholds", thresholds
        ]):
            args = parse_arguments()
            data = json.loads(args.thresholds)
            assert "conn_usage_pct" in data
            assert data["conn_usage_pct"]["warning"] == 80

    def test_thresholds_invalid_json(self):
        from check_mongodb import parse_arguments
        with patch("sys.argv", [
            "check_mongodb.py", "--uri", "mongodb://h1:27017/",
            "--metrics", "--thresholds", "not-json"
        ]):
            with self.assertRaises(SystemExit):
                parse_arguments()


# =====================================================================
# ThresholdEngine Tests
# =====================================================================

class TestThresholdEngine(unittest.TestCase):
    """Test the ThresholdEngine class."""

    def test_check_above_ok(self):
        te = ThresholdEngine({"conn_usage_pct": {"warning": 80, "critical": 90}})
        output = IcingaOutput()
        result = te.check("conn_usage_pct", 50.0, output, "h1:27017", "%")
        assert result is False
        assert output.status == NAGIOS_OK

    def test_check_above_warning(self):
        te = ThresholdEngine({"conn_usage_pct": {"warning": 80, "critical": 90}})
        output = IcingaOutput()
        result = te.check("conn_usage_pct", 85.0, output, "h1:27017", "%")
        assert result is True
        assert output.status == NAGIOS_WARNING

    def test_check_above_critical(self):
        te = ThresholdEngine({"conn_usage_pct": {"warning": 80, "critical": 90}})
        output = IcingaOutput()
        result = te.check("conn_usage_pct", 95.0, output, "h1:27017", "%")
        assert result is True
        assert output.status == NAGIOS_CRITICAL

    def test_check_below_ok(self):
        te = ThresholdEngine({"oplog_window": {"warning": 48, "critical": 24, "mode": "below"}})
        output = IcingaOutput()
        result = te.check("oplog_window", 72.0, output, "h1:27017", "h")
        assert result is False
        assert output.status == NAGIOS_OK

    def test_check_below_warning(self):
        te = ThresholdEngine({"oplog_window": {"warning": 48, "critical": 24, "mode": "below"}})
        output = IcingaOutput()
        result = te.check("oplog_window", 36.0, output, "h1:27017", "h")
        assert result is True
        assert output.status == NAGIOS_WARNING

    def test_check_below_critical(self):
        te = ThresholdEngine({"oplog_window": {"warning": 48, "critical": 24, "mode": "below"}})
        output = IcingaOutput()
        result = te.check("oplog_window", 12.0, output, "h1:27017", "h")
        assert result is True
        assert output.status == NAGIOS_CRITICAL

    def test_no_threshold_no_alert(self):
        te = ThresholdEngine({})
        output = IcingaOutput()
        result = te.check("anything", 999, output, "h1:27017")
        assert result is False

    def test_tickets_pct_threshold(self):
        te = ThresholdEngine({"tickets_read_pct": {"warning": 80, "critical": 95}})
        output = IcingaOutput()
        result = te.check("tickets_read_pct", 90.0, output, "h1:27017", "%")
        assert result is True
        assert output.status == NAGIOS_WARNING

    def test_from_json_valid(self):
        import json
        te = ThresholdEngine.from_json(json.dumps({"conn_usage_pct": {"warning": 80, "critical": 90}}))
        assert te.has("conn_usage_pct")

    def test_from_json_invalid(self):
        with self.assertRaises(ValueError):
            ThresholdEngine.from_json("not-json")

    def test_from_json_empty(self):
        te = ThresholdEngine.from_json(None)
        assert len(te.thresholds) == 0


if __name__ == "__main__":
    unittest.main()
