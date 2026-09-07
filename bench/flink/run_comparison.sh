#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_DIR=$(CDPATH= cd -- "${SCRIPT_DIR}/../.." && pwd)
FIXTURE="${REPO_DIR}/bench/data/comparison.sgfx"
CYCLES=${CYCLES:-100}
REPS=${REPS:-3}
PARALLELISMS=${PARALLELISMS:-"1 2 4 8"}
PROFILE=${PROFILE:-bounded}
LATENESS_MS=${LATENESS_MS:-0}
RESULT_DIR="${REPO_DIR}/bench/results/$(date -u +%Y%m%dT%H%M%SZ)"

if [ -x "${REPO_DIR}/build-release/app/stormglass_compare" ]; then
  STORMGLASS_COMPARE="${REPO_DIR}/build-release/app/stormglass_compare"
elif [ -x "${REPO_DIR}/build/app/stormglass_compare" ]; then
  STORMGLASS_COMPARE="${REPO_DIR}/build/app/stormglass_compare"
else
  STORMGLASS_COMPARE=""
fi

if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew is required to locate Java 17." >&2
  exit 1
fi
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PATH="${JAVA_HOME}/bin:${PATH}"

if [ ! -f "${SCRIPT_DIR}/target/stormglass-flink-bench-1.0.0.jar" ] || \
   [ ! -f "${SCRIPT_DIR}/target/classpath.txt" ]; then
  echo "Run bench/flink/setup_macos.sh first." >&2
  exit 1
fi
if [ -z "${STORMGLASS_COMPARE}" ]; then
  echo "Build with -DSTORMGLASS_BENCH=ON before running the comparison." >&2
  exit 1
fi
python3 "${REPO_DIR}/bench/fixture/generate_fixture.py" \
  --output "${FIXTURE}" --records 1000000 --keys 1000 \
  --watermark-interval 500 --max-disorder-ms 5000 --profile "${PROFILE}"

mkdir -p "${RESULT_DIR}"
{
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
    --lateness-ms "${LATENESS_MS}" >/dev/null
  "${JAVA_HOME}/bin/java" \
    -cp "${SCRIPT_DIR}/target/classes:$(cat "${SCRIPT_DIR}/target/classpath.txt")" \
    io.stormglass.bench.FlinkComparison --fixture "${FIXTURE}" \
    --cycles 2 --parallelism "${parallelism}" --lateness-ms "${LATENESS_MS}" >/dev/null

  run=1
  while [ "${run}" -le "${REPS}" ]; do
    "${STORMGLASS_COMPARE}" \
      --fixture "${FIXTURE}" --cycles "${CYCLES}" --parallelism "${parallelism}" \
      --lateness-ms "${LATENESS_MS}" \
      | tee "${RESULT_DIR}/stormglass-n${parallelism}-run${run}.txt"
    "${JAVA_HOME}/bin/java" \
      -cp "${SCRIPT_DIR}/target/classes:$(cat "${SCRIPT_DIR}/target/classpath.txt")" \
      io.stormglass.bench.FlinkComparison --fixture "${FIXTURE}" \
      --cycles "${CYCLES}" --parallelism "${parallelism}" \
      --lateness-ms "${LATENESS_MS}" \
      | tee "${RESULT_DIR}/flink-n${parallelism}-run${run}.txt"
    run=$((run + 1))
  done
done

python3 "${SCRIPT_DIR}/summarize_results.py" "${RESULT_DIR}" \
  | tee "${RESULT_DIR}/summary.txt"
echo "Raw results: ${RESULT_DIR}"
