#!/bin/bash
# fault_injection.sh — Simulate fault scenarios for MongoDB E2E testing
#
# Usage:
#   ./fault_injection.sh <action> [args...]
#
# Actions:
#   stop-node <container>          Stop a MongoDB container
#   start-node <container>         Start a stopped container
#   network-partition <container>  Disconnect container from Docker network
#   restore-network <container>    Reconnect container to Docker network
#   stop-primary <compose-file>    Stop the current primary (forces election)
#   stop-majority <compose-file>   Stop majority of nodes (loses quorum)
#   fill-disk <container> <mb>     Create a large file to fill disk
#   clear-disk <container>         Remove the fill file
#   pause-node <container>         Pause container (simulates freeze)
#   unpause-node <container>       Unpause container

set -e

ACTION="${1:-help}"
shift || true

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

get_network_name() {
    local container="$1"
    docker inspect "$container" --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' 2>/dev/null | head -1
}

find_primary() {
    local compose_file="$1"
    local containers
    containers=$(docker compose -f "$compose_file" ps -q 2>/dev/null)
    for cid in $containers; do
        local name
        name=$(docker inspect --format '{{.Name}}' "$cid" | sed 's/^\///')
        local is_primary
        is_primary=$(docker exec "$name" mongosh --quiet --eval "rs.isMaster().ismaster" 2>/dev/null || echo "false")
        if [ "$is_primary" = "true" ]; then
            echo "$name"
            return
        fi
    done
    echo ""
}

case "$ACTION" in
    stop-node)
        CONTAINER="${1:?Container name required}"
        log_info "Stopping container: $CONTAINER"
        docker stop "$CONTAINER"
        log_info "Container $CONTAINER stopped."
        ;;

    start-node)
        CONTAINER="${1:?Container name required}"
        log_info "Starting container: $CONTAINER"
        docker start "$CONTAINER"
        log_info "Container $CONTAINER started."
        ;;

    network-partition)
        CONTAINER="${1:?Container name required}"
        NETWORK=$(get_network_name "$CONTAINER")
        if [ -z "$NETWORK" ]; then
            log_error "Could not determine network for $CONTAINER"
            exit 1
        fi
        log_info "Disconnecting $CONTAINER from network $NETWORK"
        docker network disconnect "$NETWORK" "$CONTAINER"
        log_info "Container $CONTAINER disconnected from $NETWORK"
        ;;

    restore-network)
        CONTAINER="${1:?Container name required}"
        # Find the project network (try common patterns)
        NETWORK="${2:-}"
        if [ -z "$NETWORK" ]; then
            # Try to find the network from other running containers in the same project
            NETWORK=$(docker network ls --format '{{.Name}}' | grep -E "mongonet|mongo" | head -1)
        fi
        if [ -z "$NETWORK" ]; then
            log_error "Could not determine network. Pass it as second argument."
            exit 1
        fi
        log_info "Reconnecting $CONTAINER to network $NETWORK"
        docker network connect "$NETWORK" "$CONTAINER"
        log_info "Container $CONTAINER reconnected to $NETWORK"
        ;;

    stop-primary)
        COMPOSE_FILE="${1:?Compose file required}"
        PRIMARY=$(find_primary "$COMPOSE_FILE")
        if [ -z "$PRIMARY" ]; then
            log_error "No primary found!"
            exit 1
        fi
        log_info "Found primary: $PRIMARY — stopping it..."
        docker stop "$PRIMARY"
        log_info "Primary $PRIMARY stopped. New election should occur."
        ;;

    stop-majority)
        COMPOSE_FILE="${1:?Compose file required}"
        CONTAINERS=$(docker compose -f "$COMPOSE_FILE" ps --format '{{.Name}}' 2>/dev/null | grep -v init)
        COUNT=$(echo "$CONTAINERS" | wc -l | xargs)
        MAJORITY=$(( (COUNT / 2) + 1 ))
        log_info "Total nodes: $COUNT, stopping majority ($MAJORITY)..."
        STOPPED=0
        for name in $CONTAINERS; do
            if [ $STOPPED -ge $MAJORITY ]; then
                break
            fi
            log_info "Stopping $name..."
            docker stop "$name" 2>/dev/null || true
            STOPPED=$((STOPPED + 1))
        done
        log_info "Stopped $STOPPED nodes. Quorum should be lost."
        ;;

    fill-disk)
        CONTAINER="${1:?Container name required}"
        SIZE_MB="${2:-1024}"
        log_info "Creating ${SIZE_MB}MB file on $CONTAINER to fill disk..."
        docker exec "$CONTAINER" dd if=/dev/zero of=/data/db/fillfile bs=1M count="$SIZE_MB" 2>/dev/null
        log_info "Disk filled with ${SIZE_MB}MB on $CONTAINER"
        ;;

    clear-disk)
        CONTAINER="${1:?Container name required}"
        log_info "Removing fill file from $CONTAINER..."
        docker exec "$CONTAINER" rm -f /data/db/fillfile
        log_info "Fill file removed from $CONTAINER"
        ;;

    pause-node)
        CONTAINER="${1:?Container name required}"
        log_info "Pausing container: $CONTAINER"
        docker pause "$CONTAINER"
        log_info "Container $CONTAINER paused."
        ;;

    unpause-node)
        CONTAINER="${1:?Container name required}"
        log_info "Unpausing container: $CONTAINER"
        docker unpause "$CONTAINER"
        log_info "Container $CONTAINER unpaused."
        ;;

    help|*)
        cat <<EOF
Usage: $0 <action> [args...]

Actions:
  stop-node <container>              Stop a MongoDB container
  start-node <container>             Start a stopped container
  network-partition <container>      Disconnect container from Docker network
  restore-network <container> [net]  Reconnect container to Docker network
  stop-primary <compose-file>        Stop the current primary (forces election)
  stop-majority <compose-file>       Stop majority of nodes (loses quorum)
  fill-disk <container> <size_mb>    Create a large file to fill disk (default: 1024MB)
  clear-disk <container>             Remove the fill file
  pause-node <container>             Pause container (simulates freeze)
  unpause-node <container>           Unpause container

Examples:
  $0 stop-node mongo2
  $0 network-partition mongo3
  $0 stop-primary docker/docker-compose.replicaset.yml
  $0 fill-disk mongo1 2048
EOF
        ;;
esac
