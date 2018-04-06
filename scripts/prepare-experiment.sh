#!/bin/bash

# ./prepare-experiment.sh saaMariageTest 001 002

set -o nounset
set -o errexit

stardog_database_name="$1"
shift
server_port="$1"
shift

while (( "$#" )); do
  name="basename input/rdf/SAA-ID-$#*"
  /opt/stardog/bin/stardog data add http://localhost:8120/${stardog_database_name} -g "http://goldenagents.org/datasets/${name}" input/rdf/${name}/*.ttl  
  shift
done

LL_STARDOG_DATABASE="${stardog_database_name}" LL_PORT="${server_port}" ~/alignments/src/run.py