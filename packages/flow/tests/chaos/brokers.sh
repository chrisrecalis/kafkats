#!/usr/bin/env bash
# Starts the Kafka brokers of the chaos harness (Docker, KRaft, confluentinc/cp-kafka:7.6.0).
#
#   brokers.sh single [name] [port]   one broker at 127.0.0.1:<port> (default: kafka-chaos, 29092)
#   brokers.sh cluster                three brokers kafka-mb1..3 at 127.0.0.1:39091-39093 (RF 3, min ISR 2)
#
# Never removes or replaces a container: a missing one is created, a stopped one started, a paused one unpaused, a
# running one left alone. Prints the containers it started (created or restarted) on stdout, one per line.
set -euo pipefail
shopt -s inherit_errexit

IMAGE=confluentinc/cp-kafka:7.6.0
CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk

status() {
	docker inspect -f '{{if .State.Paused}}paused{{else}}{{.State.Status}}{{end}}' "$1" 2>/dev/null || echo missing
}

# ensure <name> <docker run options...>: brings the container up; prints its name if it had to boot.
ensure() {
	local name=$1
	shift
	case $(status "$name") in
	running) ;;
	paused) docker unpause "$name" >/dev/null ;;
	missing)
		docker run -d --name "$name" "$@" -e CLUSTER_ID=$CLUSTER_ID $IMAGE >/dev/null
		echo "$name"
		;;
	*)
		docker start "$name" >/dev/null
		echo "$name"
		;;
	esac
}

# wait_ready <since> <name...>: waits until each broker logged its start after <since> (unix seconds).
wait_ready() {
	local since=$1 name
	shift
	for name in "$@"; do
		for _ in $(seq 1 90); do
			docker logs --since "$since" "$name" 2>&1 | grep -q 'Kafka Server started' && continue 2
			sleep 1
		done
		echo "$name did not start within 90s" >&2
		docker logs --tail 20 "$name" >&2
		exit 1
	done
}

single() {
	local name=${1:-kafka-chaos} port=${2:-29092}
	ensure "$name" -p "$port:$port" \
		-e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
		-e KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:$port,CONTROLLER://0.0.0.0:29093" \
		-e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://127.0.0.1:$port" \
		-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
		-e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:29093 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
		-e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
		-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
		-e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0
}

cluster() {
	local i
	for i in 1 2 3; do
		ensure "kafka-mb$i" --network host \
			-e KAFKA_NODE_ID=$i -e KAFKA_PROCESS_ROLES=broker,controller \
			-e KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:3909$i,CONTROLLER://0.0.0.0:3919$i" \
			-e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://127.0.0.1:3909$i" \
			-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
			-e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:39191,2@localhost:39192,3@localhost:39193 \
			-e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
			-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3 \
			-e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2 -e KAFKA_MIN_INSYNC_REPLICAS=2 -e KAFKA_DEFAULT_REPLICATION_FACTOR=3 \
			-e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 -e KAFKA_JMX_PORT= -e KAFKA_HEAP_OPTS="-Xmx512m -Xms256m"
	done
}

since=$(($(date +%s) - 1))
case ${1:-} in
single) started=$(single "${@:2}") ;;
cluster) started=$(cluster) ;;
*)
	sed -n '2,8p' "$0" >&2
	exit 2
	;;
esac
# shellcheck disable=SC2086 # one container name per word
[ -z "$started" ] || wait_ready "$since" $started
[ -z "$started" ] || echo "$started"
