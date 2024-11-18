#!/usr/bin/env bash

CURL='curl -su a:a'
COUCH='http://127.0.0.1:15984'
DATA='dev/lib/node1/data/shards/00000000-ffffffff'

show-files () {
  echo $'\n----[ files ]----'
  ls -lah "$DATA"
  echo $'\n'
}

do-compact () {
  local gen="$1"
  $CURL -X POST -Hcontent-type:application/json "$COUCH/asd/_compact?gen=$gen"
  sleep 3
}

$CURL $COUCH/asd -X DELETE
rm "$DATA"/*

$CURL $COUCH/asd?q=1 -X PUT
$CURL $COUCH/asd/foo -X PUT -d '{"huhu":"0xb00bfart"}'
$CURL $COUCH/asd/bar/zomg -X PUT -d '\0xbarf00d' -Hcontent-type:binary/octet-stream

show-files

for n in {1..5} ; do
  echo $'\n' "----[ compaction round: $n ]----"

  let gen=n-1
  do-compact "$gen"
  show-files

  $CURL $COUCH/asd/foo | jq
  $CURL $COUCH/asd/bar | jq
  $CURL -i $COUCH/asd/bar/zomg
done
