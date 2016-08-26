#!/usr/bin/env bash
mkdir -p "data/$1"
scp -C linkdepth:*.jl "data/$1"
scp -C linkdepth:*.log "data/$1"
