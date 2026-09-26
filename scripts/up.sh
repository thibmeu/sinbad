#!/bin/sh
set -eu

JANUS_SOURCE=${JANUS_SOURCE:-/tmp/sinbad-janus-64e258152448c3fd6cae5976f6063d683ba1cbc3}
export JANUS_SOURCE
if [ ! -d "$JANUS_SOURCE/.git" ]; then
  mkdir -p "$JANUS_SOURCE"
  git -C "$JANUS_SOURCE" init
  git -C "$JANUS_SOURCE" fetch --depth 1 https://github.com/divviup/janus.git 64e258152448c3fd6cae5976f6063d683ba1cbc3
  git -C "$JANUS_SOURCE" checkout --detach FETCH_HEAD
fi
docker compose up -d --build leader helper
