#!/bin/bash

# ./scripts/prepare-experiment.sh test 8081 001 002
# or to just run lenticular lenses:
# ./scripts/prepare-experiment.sh test 8081


set -o nounset
set -o errexit

stardog_database_name="$1"
shift
server_port="$1"
shift

while (( "$#" )); do
  name=`basename input/rdf/SAA-ID-${1}*`
  echo "Uploading $name to stardog"
  /opt/stardog/bin/stardog data add http://localhost:8120/${stardog_database_name} -g "http://goldenagents.org/datasets/${name}" input/rdf/${name}/*.ttl
  shift
done

LL_STARDOG_DATABASE="${stardog_database_name}" LL_PORT="${server_port}" python ~/alignments/src/run.py
