#!/usr/bin/env bash
# The chaos suite (spec 9.4): every scenario below, against Docker brokers that brokers.sh brings up.
#
#   packages/flow/tests/chaos/run-all.sh [pattern...]    patterns are shell globs on scenario names (default: all)
#
# Env: CHAOS_LOG_DIR       one log per scenario (default: a new temporary directory)
#      CHAOS_LOCK          lock file held for the whole run, so that chaos, integration and perf runs on one machine
#                          never overlap (default /tmp/kafkats-brokers.lock)
#      CHAOS_MIN_FREE_MB   available memory the three-broker cluster waits for, up to 10 minutes (default 5000)
# Single-broker scenarios (1b-*) use kafka-chaos at 127.0.0.1:29092. Three-broker scenarios (3b-*) use kafka-mb1..3
# at 127.0.0.1:39091-39093; the run stops again the cluster containers it had to start.
# Exit 0 when every selected scenario passed.
set -uo pipefail

here=$(cd "$(dirname "$0")" && pwd)
cd "$here/../.." || exit 2
logs=${CHAOS_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/kafkats-chaos.XXXXXX")}
mkdir -p "$logs"
lock=${CHAOS_LOCK:-/tmp/kafkats-brokers.lock}

# kind name [VAR=value...] flags...   kc = kill-chaos.ts, bc = broker-chaos.ts, cc = close-chaos.ts
SCENARIOS=(
	'kc 1b-eos-stateless-2i --guarantee eos --mode stateless --instances 2 --kills 8 --drain-ms 60000'
	'kc 1b-eos-count-2i --guarantee eos --mode count --instances 2 --kills 8 --drain-ms 60000'
	'kc 1b-eos-regroup-2i --guarantee eos --mode regroup --instances 2 --kills 6 --drain-ms 60000'
	'kc 1b-eos-regroup-1i-12p --guarantee eos --mode regroup --instances 1 --partitions 12 --kills 6 --drain-ms 60000'
	'kc 1b-alo-stateless-2i --guarantee alo --mode stateless --instances 2 --kills 8 --max-dupes 2718'
	'kc 1b-alo-count-2i --guarantee alo --mode count --instances 2 --kills 8 --max-count 316'
	'bc 1b-eos-count-pause --guarantee eos --mode count --fault pause --pause-s 20'
	'bc 1b-eos-stateless-restart --guarantee eos --mode stateless --fault restart'
	'bc 1b-alo-count-restart --guarantee alo --mode count --fault restart'
	'cc 1b-close-drain-eos-count --scenario drain --guarantee eos --mode count'
	'cc 1b-close-drain-alo-stateless --scenario drain --guarantee alo --mode stateless'
	'cc 1b-close-during-start --scenario start --delay-ms 300'
	'cc 1b-close-broker-down --scenario broker-down --down pause'
	'kc 1b-eos-tablejoin-2i --guarantee eos --mode table-join --instances 2 --kills 8 --drain-ms 60000'
	'bc 1b-eos-global-pause --guarantee eos --mode global-join --fault pause --pause-s 20'
	'kc 1b-eos-count-bp-2i CHAOS_BUFFERED_RECORDS=8 --guarantee eos --mode count --instances 2 --kills 8 --drain-ms 60000'
	'kc 1b-eos-window-2i --guarantee eos --mode window --instances 2 --kills 8 --drain-ms 60000'
	'kc 1b-eos-join-2i --guarantee eos --mode join --instances 2 --kills 8 --drain-ms 60000'
	'bc 3b-eos-count-kill --guarantee eos --mode count --fault kill'
	'bc 3b-eos-count-rolling --guarantee eos --mode count --fault rolling'
	'bc 3b-eos-stateless-rolling --guarantee eos --mode stateless --fault rolling'
	'bc 3b-alo-count-kill --guarantee alo --mode count --fault kill'
	'bc 3b-alo-stateless-rolling --guarantee alo --mode stateless --fault rolling'
	'bc 3b-alo-count-pause --guarantee alo --mode count --fault pause --pause-s 25'
	'kc 3b-eos-count-2i --guarantee eos --mode count --instances 2 --kills 6 --drain-ms 60000'
	'kc 3b-eos-regroup-2i --guarantee eos --mode regroup --instances 2 --kills 6 --drain-ms 60000'
	'kc 3b-alo-count-2i --guarantee alo --mode count --instances 2 --kills 6 --max-count 257'
)
CLUSTER_ENV=(
	CHAOS_BROKERS=127.0.0.1:39091,127.0.0.1:39092,127.0.0.1:39093 CHAOS_RF=3
	CHAOS_CONTAINER=kafka-mb2 CHAOS_CONTAINERS=kafka-mb1,kafka-mb2,kafka-mb3
)

selected() {
	local pattern
	[ ${#patterns[@]} -eq 0 ] && return 0
	for pattern in "${patterns[@]}"; do
		# shellcheck disable=SC2053 # glob match on purpose
		[[ $1 == $pattern ]] && return 0
	done
	return 1
}

# scenario <line> [VAR=value...]: runs one scenario line with the extra env, prints its verdict and evidence.
scenario() {
	local kind name rest script word start code result verdict
	local -a env=("${@:2}") args=()
	read -r kind name rest <<<"$1"
	for word in $rest; do
		if [[ $word == [A-Z]*=* ]]; then env+=("$word"); else args+=("$word"); fi
	done
	case $kind in
	kc) script=kill-chaos ;;
	bc)
		script=broker-chaos
		args+=(--recover-ms 120000)
		;;
	cc) script=close-chaos ;;
	esac
	start=$(date +%s)
	# timeout runs in a process group of its own with the harness and its workers, and signals all of them; fd 9
	# (the lock) stays with this shell. In the background, so that an interrupt reaches the traps at once.
	env "${env[@]}" timeout -k 30 600 pnpm exec tsx "tests/chaos/$script.ts" "${args[@]}" >"$logs/$name.log" 2>&1 9>&- &
	child=$!
	wait $child
	code=$?
	child=
	result=$(grep -m1 '^RESULT' "$logs/$name.log" | cut -c1-110)
	if [ $code -eq 0 ] && [[ $result == *': PASS'* ]]; then
		verdict=PASS
		passed+=("$name")
	else
		verdict=FAIL
		[ $code -eq 124 ] && verdict='FAIL (timeout 600s)'
		failed+=("$name")
	fi
	printf '%-28s %-5s %4ss  %s\n' "$name" "$verdict" $(($(date +%s) - start)) "$result"
	grep -E '^  (summary|unhandled|workers in ERROR|ALO duplicate gate|fault->broker)|\] close: ' "$logs/$name.log" |
		cut -c1-240 | sed 's/^ */    /'
}

# section <prefix> <brokers.sh args> [VAR=value...]: brings the brokers up before every selected scenario of the
# section (a failed scenario may leave one paused or stopped).
section() {
	local prefix=$1 brokers=$2 line name
	shift 2
	for line in "${SCENARIOS[@]}"; do
		read -r _ name _ <<<"$line"
		[[ $name == "$prefix"-* ]] && selected "$name" || continue
		# shellcheck disable=SC2086 # brokers.sh arguments
		if ! "$here/brokers.sh" $brokers >>"$logs/brokers.log" 2>&1 9>&-; then
			printf '%-28s FAIL  brokers did not start (%s)\n' "$name" "$logs/brokers.log"
			failed+=("$name")
			continue
		fi
		scenario "$line" "$@"
	done
}

wanted() {
	local line name
	for line in "${SCENARIOS[@]}"; do
		read -r _ name _ <<<"$line"
		[[ $name == "$1"-* ]] && selected "$name" && return 0
	done
	return 1
}

running() {
	[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = true ]
}

available_mb() {
	awk '/^MemAvailable:/ { print int($2 / 1024) }' /proc/meminfo
}

patterns=("$@")
passed=()
failed=()
child=
# An interrupt ends the running scenario's process group too.
trap '[ -z "$child" ] || kill -TERM -- -"$child" 2>/dev/null; exit 130' INT TERM
docker info >/dev/null 2>&1 || {
	echo 'docker is not available'
	exit 2
}
exec 9>"$lock"
if ! flock -n 9; then
	echo "waiting for $lock (another broker run is in progress)..."
	flock 9
fi
echo "logs: $logs"

if wanted 1b; then
	echo '## single broker (kafka-chaos, 127.0.0.1:29092)'
	section 1b single
fi

if wanted 3b; then
	echo '## three brokers, replication factor 3 (kafka-mb1..3, 127.0.0.1:39091-39093)'
	stopped=()
	for container in kafka-mb1 kafka-mb2 kafka-mb3; do
		running "$container" || stopped+=("$container")
	done
	need=${CHAOS_MIN_FREE_MB:-5000}
	for _ in $(seq 1 60); do
		[ ${#stopped[@]} -eq 0 ] || [ "$(available_mb)" -ge "$need" ] && break
		sleep 10
	done
	if [ ${#stopped[@]} -gt 0 ] && [ "$(available_mb)" -lt "$need" ]; then
		echo "only $(available_mb) MB available, the cluster needs $need MB: three-broker scenarios not run"
		for line in "${SCENARIOS[@]}"; do
			read -r _ name _ <<<"$line"
			[[ $name == 3b-* ]] && selected "$name" && failed+=("$name")
		done
	else
		# Stops again the containers this run starts, also when the run is interrupted.
		trap '[ ${#stopped[@]} -eq 0 ] || docker stop "${stopped[@]}" >/dev/null' EXIT
		section 3b cluster "${CLUSTER_ENV[@]}"
	fi
fi

echo "## ${#passed[@]}/$((${#passed[@]} + ${#failed[@]})) passed${failed[*]:+; failed: ${failed[*]}}"
echo "logs: $logs"
[ ${#failed[@]} -eq 0 ]
