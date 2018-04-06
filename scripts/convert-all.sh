#!/bin/bash

for dir in `cd ./input/xml/ && ls`; do
  echo "converting $dir"
  rm -r ./input/rdf/$dir/*
  gzcat ./input/xml/$dir/*.xml.gz | python -m xmltodict 2 | python -u ./scripts/$dir/xml2rdf.py
  mv ./scripts/$dir/*.ttl ./input/rdf/$dir
  git add -A ./input/rdf/$dir/*
done

git commit -m "Re-executed conversion"