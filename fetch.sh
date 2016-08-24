#!/usr/bin/env bash
mkdir -p "data/$1"
scp linkdepth:*.jl "data/$1"
scp linkdepth:*.log "data/$1"
