#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
mvn -q -B package

jar=target/sql-workloads-1.0.jar
test -s "$jar"
shasum -a 256 "$jar"
