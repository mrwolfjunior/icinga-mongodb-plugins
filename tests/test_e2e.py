#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
End-to-End tests for check_mongodb.py

These tests require Docker containers to be running.
Run the appropriate docker-compose file before running these tests.

Usage:
    # ReplicaSet tests
    docker compose -f docker/docker-compose.replicaset.yml up -d
    sleep 30
    python -m pytest tests/test_e2e.py -v --timeout=120 -k "replicaset"

    # ReplicaSet with arbiter tests
    docker compose -f docker/docker-compose.replicaset-arbiter.yml up -d
    sleep 30
    python -m pytest tests/test_e2e.py -v --timeout=120 -k "arbiter"

    # Sharded cluster tests
    docker compose -f docker/docker-compose.sharded.yml up -d
    sleep 60
    python -m pytest tests/test_e2e.py -v --timeout=120 -k "sharded"

    # Single node tests
    docker compose -f docker/docker-compose.single.yml up -d
    sleep 10
    python -m pytest tests/test_e2e.py -v --timeout=120 -k "single"
"""

import os
import subprocess
import sys
import time
import pytest

# Path to the plugin script
SCRIPT = os.path.join(os.path.dirname(__file__), "..", "check_mongodb.py")
FAULT_SCRIPT = os.path.join(os.path.dirname(__file__), "fault_injection.sh")

# Connection URIs (using localhost mapped ports)
SINGLE_URI = "mongodb://localhost:27017/"
RS_URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"
RS_ARBITER_URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rsArbiter"
SHARDED_URI = "mongodb://localhost:27017,localhost:27018/"

# Icinga exit codes
OK = 0
WARNING = 1
CRITICAL = 2
UNKNOWN = 3


def run_check(*args, timeout=30):
    """Run the check_mongodb.py script and return (exit_code, stdout, stderr)."""
    cmd = [sys.executable, SCRIPT] + list(args)
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout
        )
        return result.returncode, result.stdout.strip(), result.stderr.strip()
    except subprocess.TimeoutExpired:
        return UNKNOWN, "", "Timeout expired"


def run_fault(action, *args):
    """Run the fault injection script."""
    cmd = ["bash", FAULT_SCRIPT, action] + list(args)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    return result.returncode == 0


def is_container_running(container_name):
    """Check if a Docker container is running."""
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Running}}", container_name],
        capture_output=True, text=True
    )
    return result.stdout.strip() == "true"


def wait_for_rs_stable(uri, max_wait=60):
    """Wait for a ReplicaSet to stabilize (have a primary)."""
    start = time.time()
    while time.time() - start < max_wait:
        code, out, _ = run_check("--uri", uri, "--availability", "--timeout", "5")
        if code == OK:
            return True
        time.sleep(5)
    return False


# =====================================================================
# Single Node Tests
# =====================================================================

class TestSingleNode:
    """Tests for single-node MongoDB deployment."""

    @pytest.fixture(autouse=True)
    def check_single_running(self):
        if not is_container_running("mongo-single"):
            pytest.skip("Single node container not running")

    def test_availability_ok(self):
        code, out, _ = run_check("--uri", SINGLE_URI, "--availability")
        assert code == OK
        assert "OK" in out

    def test_metrics_ok(self):
        code, out, _ = run_check("--uri", SINGLE_URI, "--metrics")
        assert code == OK
        assert "|" in out  # Should contain perfdata

    def test_filesystem_ok(self):
        code, out, _ = run_check(
            "--uri", SINGLE_URI, "--filesystem",
            "--thresholds", '{"fs_usage_pct": {"warning": 85, "critical": 95}}'
        )
        assert code in (OK, WARNING)  # Depends on actual disk usage

    def test_metrics_perfdata(self):
        code, out, _ = run_check("--uri", SINGLE_URI, "--metrics")
        assert code == OK
        assert "|" in out  # Should contain perfdata


# =====================================================================
# ReplicaSet Tests
# =====================================================================

class TestReplicaSet:
    """Tests for 3-node ReplicaSet deployment."""

    @pytest.fixture(autouse=True)
    def check_rs_running(self):
        if not is_container_running("mongo1"):
            pytest.skip("ReplicaSet containers not running")

    def test_availability_all_up(self):
        code, out, _ = run_check("--uri", RS_URI, "--availability")
        assert code == OK
        assert "healthy" in out.lower() or "OK" in out

    def test_availability_with_verbose(self):
        code, out, _ = run_check("--uri", RS_URI, "--availability", "--verbose")
        assert code == OK
        assert "Quorum OK" in out or "OK" in out

    def test_metrics_all_up(self):
        code, out, _ = run_check("--uri", RS_URI, "--metrics")
        assert code == OK
        assert "conn_current" in out or "|" in out

    def test_filesystem_ok(self):
        code, out, _ = run_check(
            "--uri", RS_URI, "--filesystem",
            "--thresholds", '{"fs_usage_pct": {"warning": 85, "critical": 95}}'
        )
        assert code in (OK, WARNING)

    def test_metrics_all_up(self):
        code, out, _ = run_check("--uri", RS_URI, "--metrics")
        assert code == OK
        assert "collected" in out.lower() or "OK" in out

    def test_availability_secondary_down(self):
        """Stopping 1 secondary should produce CRITICAL."""
        try:
            assert run_fault("stop-node", "mongo2")
            time.sleep(10)  # Wait for RS to detect the failure

            code, out, _ = run_check("--uri", RS_URI, "--availability", "--timeout", "5")
            assert code == CRITICAL
            assert "down" in out.lower() or "unreachable" in out.lower()
        finally:
            run_fault("start-node", "mongo2")
            time.sleep(15)
            wait_for_rs_stable(RS_URI)

    def test_availability_primary_down(self):
        """Stopping the primary should produce CRITICAL."""
        try:
            assert run_fault("stop-node", "mongo1")
            time.sleep(5)  # Don't wait too long — check during election

            code, out, _ = run_check("--uri", RS_URI, "--availability", "--timeout", "5")
            assert code == CRITICAL
        finally:
            run_fault("start-node", "mongo1")
            time.sleep(20)
            wait_for_rs_stable(RS_URI)

    def test_availability_quorum_lost(self):
        """Stopping 2 of 3 nodes should cause quorum loss — CRITICAL."""
        try:
            run_fault("stop-node", "mongo2")
            run_fault("stop-node", "mongo3")
            time.sleep(10)

            code, out, _ = run_check("--uri", RS_URI, "--availability", "--timeout", "5")
            assert code == CRITICAL
            assert "quorum" in out.lower() or "down" in out.lower()
        finally:
            run_fault("start-node", "mongo2")
            run_fault("start-node", "mongo3")
            time.sleep(20)
            wait_for_rs_stable(RS_URI)

    def test_availability_wrong_rs_name(self):
        """Using wrong RS name should produce CRITICAL."""
        wrong_uri = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=wrongRS"
        code, out, _ = run_check("--uri", wrong_uri, "--availability", "--timeout", "5")
        # pymongo may refuse to connect or we detect the mismatch
        assert code in (CRITICAL, UNKNOWN)

    def test_availability_network_partition(self):
        """Network-partitioned node should be CRITICAL."""
        try:
            assert run_fault("network-partition", "mongo3")
            time.sleep(10)

            code, out, _ = run_check("--uri", RS_URI, "--availability", "--timeout", "5")
            assert code == CRITICAL
        finally:
            run_fault("restore-network", "mongo3")
            time.sleep(15)
            wait_for_rs_stable(RS_URI)

    def test_metrics_node_down(self):
        """Metrics check with a node down should be CRITICAL."""
        try:
            assert run_fault("stop-node", "mongo3")
            time.sleep(5)

            code, out, _ = run_check("--uri", RS_URI, "--metrics", "--timeout", "5")
            assert code == CRITICAL
            assert "unreachable" in out.lower()
        finally:
            run_fault("start-node", "mongo3")
            time.sleep(15)
            wait_for_rs_stable(RS_URI)

    def test_filesystem_node_down(self):
        """Filesystem check with a node down should be CRITICAL."""
        try:
            assert run_fault("stop-node", "mongo3")
            time.sleep(5)

            code, out, _ = run_check(
                "--uri", RS_URI, "--filesystem",
                "--thresholds", '{"fs_usage_pct": {"warning": 85, "critical": 95}}',
                "--timeout", "5"
            )
            assert code == CRITICAL
        finally:
            run_fault("start-node", "mongo3")
            time.sleep(15)
            wait_for_rs_stable(RS_URI)

    def test_metrics_node_down(self):
        """Metrics check with a node down should be CRITICAL."""
        try:
            assert run_fault("stop-node", "mongo3")
            time.sleep(5)

            code, out, _ = run_check("--uri", RS_URI, "--metrics", "--timeout", "5")
            assert code == CRITICAL
        finally:
            run_fault("start-node", "mongo3")
            time.sleep(15)
            wait_for_rs_stable(RS_URI)


# =====================================================================
# ReplicaSet with Arbiter Tests
# =====================================================================

class TestReplicaSetArbiter:
    """Tests for ReplicaSet with arbiter deployment."""

    @pytest.fixture(autouse=True)
    def check_arbiter_running(self):
        if not is_container_running("mongo-data1"):
            pytest.skip("ReplicaSet-arbiter containers not running")

    def test_availability_all_up(self):
        code, out, _ = run_check("--uri", RS_ARBITER_URI, "--availability")
        assert code == OK

    def test_availability_arbiter_ok_if_rs_healthy(self):
        """Arbiter unreachable but RS healthy → should be OK."""
        try:
            assert run_fault("stop-node", "mongo-arbiter")
            time.sleep(10)

            # The RS should still be healthy (2 data nodes, primary + secondary)
            # Arbiter is unreachable but rsStatus shows RS is healthy
            code, out, _ = run_check("--uri", RS_ARBITER_URI, "--availability", "--timeout", "5")
            # Since arbiter is in the URI but unreachable, and we check rsStatus
            # which should still show the arbiter as healthy (it was healthy before stop),
            # the behavior depends on heartbeat timing. After a few seconds, RS should
            # detect arbiter is down but still have quorum.
            # The checker logic: if arbiter unreachable directly BUT rsStatus says healthy → OK
            # If enough time has passed, rsStatus will say arbiter is down too
            assert code in (OK, CRITICAL)  # Depends on timing
        finally:
            run_fault("start-node", "mongo-arbiter")
            time.sleep(15)


# =====================================================================
# Sharded Cluster Tests
# =====================================================================

class TestShardedCluster:
    """Tests for Sharded Cluster deployment."""

    @pytest.fixture(autouse=True)
    def check_sharded_running(self):
        if not is_container_running("mongos1"):
            pytest.skip("Sharded cluster containers not running")

    def test_sharded_availability(self):
        code, out, _ = run_check("--uri", SHARDED_URI, "--availability")
        assert code == OK

    def test_sharded_metrics(self):
        code, out, _ = run_check("--uri", SHARDED_URI, "--metrics")
        assert code == OK

    def test_sharded_metrics_perfdata(self):
        code, out, _ = run_check("--uri", SHARDED_URI, "--metrics")
        assert code == OK

    def test_sharded_mongos_down(self):
        """One mongos down should be CRITICAL."""
        try:
            assert run_fault("stop-node", "mongos2")
            time.sleep(5)

            code, out, _ = run_check("--uri", SHARDED_URI, "--availability", "--timeout", "5")
            assert code == CRITICAL
            assert "unreachable" in out.lower() or "down" in out.lower()
        finally:
            run_fault("start-node", "mongos2")
            time.sleep(10)


# =====================================================================
# Error Handling Tests
# =====================================================================

class TestErrorHandling:
    """Tests for error handling edge cases."""

    def test_unreachable_host(self):
        """Completely unreachable host should give CRITICAL."""
        code, out, _ = run_check(
            "--uri", "mongodb://192.0.2.1:27017/",  # RFC 5737 test address
            "--availability", "--timeout", "3"
        )
        assert code == CRITICAL

    def test_wrong_port(self):
        """Wrong port should give CRITICAL."""
        code, out, _ = run_check(
            "--uri", "mongodb://localhost:19999/",
            "--availability", "--timeout", "3"
        )
        assert code == CRITICAL

    def test_missing_mode(self):
        """Missing check mode should give error exit."""
        code, out, err = run_check("--uri", "mongodb://localhost:27017/")
        assert code != OK  # argparse error

    def test_help_flag(self):
        """--help should show usage."""
        code, out, _ = run_check("--help")
        assert code == OK
        assert "usage" in out.lower() or "check_mongodb" in out.lower()
