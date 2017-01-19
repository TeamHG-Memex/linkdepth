#!/usr/bin/env bash
scp -C *.py *.csv ${1:-linkdepth:}
