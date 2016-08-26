#!/usr/bin/env bash
mkdir -p "data/$1"
scp -C linkdepth:*.jl "data/$1"
scp -C linkdepth:*.jl.gz "data/$1"
scp -C linkdepth:*.log "data/$1"
ssh linkdepth './frontier-size.py .scrapy/*' > "data/$1/frontier-`date +%s`.txt"
