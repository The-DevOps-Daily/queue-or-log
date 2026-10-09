#!/usr/bin/env bash
cd "$(dirname "$0")"; source ./env.sh
$DOCKER rm -f qol-kafka qol-rabbit qol-nats >/dev/null 2>&1 || true
echo "down"
