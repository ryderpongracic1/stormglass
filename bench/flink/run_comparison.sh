#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_DIR=$(CDPATH= cd -- "${SCRIPT_DIR}/../.." && pwd)

CYCLES=${CYCLES:-100}
REPS=${REPS:-3}
PARALLELISMS=${PARALLELISMS:-"1 2 4 8"}
PROFILE=${PROFILE:-bounded}
LATENESS_MS=${LATENESS_MS:-0}
if [ "${LATENESS_MS}" != 0 ]; then
  echo "Only zero allowed lateness has matched output semantics." >&2
  exit 1
fi
mkdir -p "${REPO_DIR}/bench/results"
RESULT_DIR=$(mktemp -d "${REPO_DIR}/bench/results/run-XXXXXXXX")
FIXTURE="${RESULT_DIR}/comparison.sgfx"

if [ -x "${REPO_DIR}/build-release/app/stormglass_compare" ]; then
  STORMGLASS_COMPARE="${REPO_DIR}/build-release/app/stormglass_compare"
elif [ -x "${REPO_DIR}/build/app/stormglass_compare" ]; then
  STORMGLASS_COMPARE="${REPO_DIR}/build/app/stormglass_compare"
else
  STORMGLASS_COMPARE=""
fi

if [ -z "${JAVA_HOME:-}" ]; then
  if ! command -v brew >/dev/null 2>&1; then
    echo "Set JAVA_HOME to Java 17, or install Homebrew OpenJDK 17." >&2
    exit 1
  fi
  export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
fi
export PATH="${JAVA_HOME}/bin:${PATH}"
# Fixed heap and GC; each measured job starts in a fresh JVM.
JAVA_ARGS=(-Xms1g -Xmx4g -XX:+UseG1GC)
if ! "${JAVA_HOME}/bin/java" -version 2>&1 | grep -q 'version "17\.'; then
  echo "The reference comparison requires Java 17." >&2
  exit 1
fi

if [ ! -f "${SCRIPT_DIR}/target/stormglass-flink-bench-1.0.0.jar" ] || \
   [ ! -f "${SCRIPT_DIR}/target/classpath.txt" ]; then
  echo "Run bench/flink/setup_macos.sh first." >&2
  exit 1
fi
if [ -z "${STORMGLASS_COMPARE}" ]; then
  echo "Build with -DSTORMGLASS_BENCH=ON before running the comparison." >&2
  exit 1
fi
# Rebuild rather than silently measuring stale binaries after a source edit.
BUILD_DIR=$(dirname "$(dirname "${STORMGLASS_COMPARE}")")
python3 - "${BUILD_DIR}/CMakeCache.txt" <<'BUILD_CHECK'
import pathlib, sys
cache = pathlib.Path(sys.argv[1]).read_text().splitlines()
settings = dict(line.split('=', 1) for line in cache if '=' in line and not line.startswith('//'))
if settings.get('CMAKE_BUILD_TYPE:STRING') != 'Release':
    raise SystemExit('benchmark requires a Release build')
for flag in ('STORMGLASS_SANITIZERS', 'STORMGLASS_TSAN'):
    if settings.get(flag + ':BOOL', 'OFF') != 'OFF':
        raise SystemExit('benchmark must not use sanitizers')
BUILD_CHECK
cmake --build "${BUILD_DIR}" --parallel 4 --target stormglass_compare
mvn -f "${SCRIPT_DIR}/pom.xml" --batch-mode --no-transfer-progress package
mvn -f "${SCRIPT_DIR}/pom.xml" --batch-mode --no-transfer-progress \
  dependency:build-classpath -DincludeScope=compile \
  -Dmdep.outputFile="${SCRIPT_DIR}/target/classpath.txt"

python3 "${REPO_DIR}/bench/fixture/generate_fixture.py" \
  --output "${FIXTURE}" --records 1000000 --keys 1000 \
  --watermark-interval 500 --max-disorder-ms 5000 --profile "${PROFILE}"

python3 - "${RESULT_DIR}" "${CYCLES}" "${REPS}" "${PARALLELISMS}" <<'MANIFEST'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
cycles, reps = int(sys.argv[2]), int(sys.argv[3])
ns = [int(n) for n in sys.argv[4].split()]
if cycles <= 0 or reps <= 0 or not ns or min(ns) <= 0 or len(set(ns)) != len(ns):
    raise SystemExit("cycles/reps/parallelisms must be positive, parallelisms unique")
(root / 'manifest.json').write_text(json.dumps(dict(
    cycles=cycles, reps=reps, parallelisms=ns, expected_records=1_000_000 * cycles,
    window_ms=1000, lateness_ms=0, timing='execute',
    fixture_sha256=hashlib.sha256((root / 'comparison.sgfx').read_bytes()).hexdigest(),
    java_args=['-Xms1g', '-Xmx4g', '-XX:+UseG1GC'],
    warmup='separate process, two cycles; does not warm measured JVM JIT'), indent=2) + '\n')
MANIFEST
{
  date -u
  git -C "${REPO_DIR}" status --short
  git -C "${REPO_DIR}" diff --binary | shasum -a 256
  shasum -a 256 "${STORMGLASS_COMPARE}" "${SCRIPT_DIR}/target/stormglass-flink-bench-1.0.0.jar"
  python3 - "${SCRIPT_DIR}/target/classpath.txt" <<'DEPENDENCIES'
import hashlib, pathlib, os, sys
for item in pathlib.Path(sys.argv[1]).read_text().strip().split(os.pathsep):
    path = pathlib.Path(item)
    print(hashlib.sha256(path.read_bytes()).hexdigest(), path)
DEPENDENCIES
  cat "${BUILD_DIR}/CMakeCache.txt"
  git -C "${REPO_DIR}" rev-parse HEAD
  sw_vers
  uname -m
  sysctl -n machdep.cpu.brand_string hw.logicalcpu hw.physicalcpu hw.memsize
  "${JAVA_HOME}/bin/java" -version 2>&1
  mvn -version
} > "${RESULT_DIR}/environment.txt"

for parallelism in ${PARALLELISMS}; do
  "${STORMGLASS_COMPARE}" \
    --fixture "${FIXTURE}" --cycles 2 --parallelism "${parallelism}" \
    --lateness-ms "${LATENESS_MS}" >"${RESULT_DIR}/warmup-stormglass-n${parallelism}.txt" 2>&1
  "${JAVA_HOME}/bin/java" "${JAVA_ARGS[@]}" \
    -cp "${SCRIPT_DIR}/target/stormglass-flink-bench-1.0.0.jar:$(cat "${SCRIPT_DIR}/target/classpath.txt")" \
    io.stormglass.bench.FlinkComparison --fixture "${FIXTURE}" \
    --cycles 2 --parallelism "${parallelism}" --lateness-ms "${LATENESS_MS}" >"${RESULT_DIR}/warmup-flink-n${parallelism}.txt" 2>&1

  run_job() {
    local engine=$1
    if [ "$engine" = stormglass ]; then
      "${STORMGLASS_COMPARE}" --fixture "${FIXTURE}" --cycles "${CYCLES}" \
        --parallelism "${parallelism}" --lateness-ms "${LATENESS_MS}" \
        2>&1 | tee "${RESULT_DIR}/stormglass-n${parallelism}-run${run}.txt"
    else
      "${JAVA_HOME}/bin/java" "${JAVA_ARGS[@]}" \
        -cp "${SCRIPT_DIR}/target/stormglass-flink-bench-1.0.0.jar:$(cat "${SCRIPT_DIR}/target/classpath.txt")" \
        io.stormglass.bench.FlinkComparison --fixture "${FIXTURE}" \
        --cycles "${CYCLES}" --parallelism "${parallelism}" --lateness-ms "${LATENESS_MS}" \
        2>&1 | tee "${RESULT_DIR}/flink-n${parallelism}-run${run}.txt"
    fi
  }
  run=1
  while [ "${run}" -le "${REPS}" ]; do
    # Alternate engine order to reduce systematic first/second-run bias.
    if (( run % 2 )); then
      run_job stormglass
      run_job flink
    else
      run_job flink
      run_job stormglass
    fi
    run=$((run + 1))
  done
done

python3 "${SCRIPT_DIR}/summarize_results.py" "${RESULT_DIR}" \
  | tee "${RESULT_DIR}/summary.txt"
echo "Raw results: ${RESULT_DIR}"
