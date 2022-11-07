#!/bin/sh

export PGHOST=/var/run/postgresql
export PGUSER=postgres
export PGDATABASE=pg2kv2spacetimedb

mkdir -p ./count.d/cache.d/fs.d

./target/release/pg2kv2spacetimedb
