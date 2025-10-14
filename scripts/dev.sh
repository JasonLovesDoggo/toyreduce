#!/usr/bin/env bash

set -e

cd "$(dirname "$0")/../ui"

pnpm run publish

cd ..

go run ./cmd/toyreduce master
