#!/usr/bin/env python3
import sys
import argparse
from pathlib import Path
from scrapy.squeues import PickleLifoDiskQueue


if __name__ == '__main__':
    p = argparse.ArgumentParser(
        description="Print total number of requests in Scrapy queues. "
                    "Pass one or more JOBDIRs to this script, "
                    "e.g. 'frontier-size.py .scrapy/*'")
    p.add_argument("jobdir", nargs='+')
    args = p.parse_args()

    for folder in args.jobdir:
        total = 0
        for path in Path(folder).joinpath('requests.queue').glob("*"):
            if path.suffix == '.json':
                continue
            if not path.stat().st_size:
                continue
            try:
                queue = PickleLifoDiskQueue(str(path))
                size = len(queue)
                total += size
            except Exception as e:
                sys.stderr.write(repr(e) + "\n")

        print("%s    " % folder, total)
