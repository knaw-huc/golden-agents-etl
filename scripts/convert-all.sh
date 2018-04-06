#!/bin/bash


set -o nounset
set -o errexit

cd -P -- "$(dirname -- "$0")" #go to dir of script even if it was called as a symbolic link
cd ..

for dir in `cd ./input/xml/ && ls`; do
  echo "converting $dir"
  rm -fr ./input/rdf/$dir/*
  zcat ./input/xml/$dir/*.xml.gz | python -m xmltodict 2 | python -u ./scripts/$dir/xml2rdf.py
  mkdir -p ./input/rdf/$dir
  mv ./scripts/$dir/*.ttl ./input/rdf/$dir
  git add -A ./input/rdf/$dir/*
done

git commit -m "Re-executed conversion"
git push origin master
