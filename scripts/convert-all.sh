#!/bin/bash


set -o nounset
set -o errexit

cd -P -- "$(dirname -- "$0")" #go to dir of script even if it was called as a symbolic link
cd ..

for dir in `cd ./input/xml/ && ls`; do
  if shasum -s -c ./input/xml/$dir/checksums; then
    echo "files in $dir are up to date"
  else 
    echo "converting $dir"
    mkdir -p ./input/rdf/$dir
    rm -fr ./input/rdf/$dir/*
    gzip -c -d ./input/xml/$dir/*.xml.gz | python -m xmltodict 2 | python -u ./scripts/$dir/xml2rdf.py
    mv ./scripts/$dir/*.ttl ./input/rdf/$dir
    shasum ./input/xml/$dir/*.xml.gz > input/xml/$dir/checksums
    git add ./input/xml/$dir/checksums
    git add -A ./input/rdf/$dir/*
  fi
done

git commit -m "Re-executed conversion"
git push origin master
