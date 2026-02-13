#!/bin/bash
# init-replicaset.sh — Initialize a MongoDB ReplicaSet from environment variables
# Environment:
#   RS_NAME  — ReplicaSet name (e.g. "rs0")
#   MEMBERS  — Comma-separated list of host:port (e.g. "mongo1:27017,mongo2:27017,mongo3:27017")


# Detect Shell
if command -v mongosh &> /dev/null; then
  MONGO_SHELL="mongosh"
elif command -v mongo &> /dev/null; then
  MONGO_SHELL="mongo"
else
  echo "Error: Neither 'mongosh' nor 'mongo' shell found."
  exit 1
fi

echo "Using shell: $MONGO_SHELL"

RS_NAME="${RS_NAME:-rs0}"
MEMBERS="${MEMBERS:-mongo1:27017,mongo2:27017,mongo3:27017}"

echo "=== Initializing ReplicaSet '${RS_NAME}' ==="
echo "Members: ${MEMBERS}"

# Build the members array for rs.initiate()
IFS=',' read -ra MEMBER_ARRAY <<< "$MEMBERS"
MEMBERS_JSON=""
ID=0
for member in "${MEMBER_ARRAY[@]}"; do
    member=$(echo "$member" | xargs)  # trim whitespace
    if [ -n "$MEMBERS_JSON" ]; then
        MEMBERS_JSON="${MEMBERS_JSON},"
    fi
    MEMBERS_JSON="${MEMBERS_JSON}{_id:${ID},host:\"${member}\"}"
    ID=$((ID + 1))
done

FIRST_MEMBER=$(echo "${MEMBER_ARRAY[0]}" | xargs)

echo "Connecting to ${FIRST_MEMBER}..."

# Wait for the first member to be ready
for i in $(seq 1 30); do
    if $MONGO_SHELL --host "${FIRST_MEMBER}" --eval "db.adminCommand('ping')" &>/dev/null; then
        echo "First member is ready."
        break
    fi
    echo "Waiting for ${FIRST_MEMBER} to be ready... (attempt ${i}/30)"
    sleep 2
done

# Initiate the ReplicaSet
echo "Running rs.initiate()..."
$MONGO_SHELL --host "${FIRST_MEMBER}" --eval "
  try {
    rs.initiate({
      _id: '${RS_NAME}',
      members: [${MEMBERS_JSON}]
    });
  } catch (e) {
    if (e.code === 23 || e.message.includes('already initialized')) {
      print('ReplicaSet already initialized.');
    } else {
      throw e;
    }
  }
"

# Wait for the RS to elect a primary
echo "Waiting for primary election..."
for i in $(seq 1 30); do
    IS_PRIMARY=$($MONGO_SHELL --host "${FIRST_MEMBER}" --quiet --eval "rs.isMaster().ismaster" 2>/dev/null || echo "false")
    if [ "$IS_PRIMARY" = "true" ]; then
        echo "Primary elected successfully."
        break
    fi
    echo "Waiting for primary... (attempt ${i}/30)"
    sleep 2
done

echo "=== ReplicaSet '${RS_NAME}' initialized ==="
$MONGO_SHELL --host "${FIRST_MEMBER}" --eval "rs.status()"

