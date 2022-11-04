#!/bin/sh

export PGHOST=/var/run/postgresql
export PGUSER=postgres
export PGDATABASE=pg2kv2spacetimedb

./target/release/pg2kv2spacetimedb
