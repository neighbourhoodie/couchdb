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
  $CURL -X POST -Hcontent-type:application/json $COUCH/asd/_compact
  sleep 5
}

$CURL $COUCH/asd -X DELETE
rm "$DATA"/*

$CURL $COUCH/asd?q=1 -X PUT
$CURL $COUCH/asd/foo -X PUT -d '{"huhu":"0xb00bfart"}'
$CURL $COUCH/asd/bar/zomg -X PUT -d '\0xbarf00d' -Hcontent-type:binary/octet-stream

show-files

echo $'\n----[ first compaction ]----'

do-compact
show-files

$CURL $COUCH/asd/foo | jq
$CURL $COUCH/asd/bar | jq
$CURL -i $COUCH/asd/bar/zomg

echo $'\n----[ second compaction ]----'

do-compact
show-files

$CURL $COUCH/asd/foo | jq
$CURL $COUCH/asd/bar | jq
$CURL -i $COUCH/asd/bar/zomg
