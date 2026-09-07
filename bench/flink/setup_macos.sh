#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew is required to locate OpenJDK 17 and Maven." >&2
  exit 1
fi
if ! brew --prefix openjdk@17 >/dev/null 2>&1; then
  echo "Install Java 17 with: brew install openjdk@17" >&2
  exit 1
fi
if ! command -v mvn >/dev/null 2>&1; then
  echo "Install Maven with: brew install maven" >&2
  exit 1
fi

export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PATH="${JAVA_HOME}/bin:${PATH}"

mvn -f "${SCRIPT_DIR}/pom.xml" --batch-mode --no-transfer-progress clean package
mvn -f "${SCRIPT_DIR}/pom.xml" --batch-mode --no-transfer-progress \
  dependency:build-classpath -DincludeScope=compile \
  -Dmdep.outputFile="${SCRIPT_DIR}/target/classpath.txt"
echo "JAVA_HOME=${JAVA_HOME}"
"${JAVA_HOME}/bin/java" -version
echo "Flink 2.3.0 local execution dependencies and the comparison job are ready."
