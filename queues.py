# -*- coding: utf-8 -*-
from collections import deque
from itertools import count
from queuelib import PriorityQueue
from scrapy.squeues import PickleLifoDiskQueue


class RoundRobinPriorityQueue:
    """
    This queue chooses a concrete queue among other priority queues
    in a round-robin fashion.

    XXX: standard scrapy disk queues don't work with it; use queues.DiskQueue.
    """
    def __init__(self, qfactory, startprios=()):
        self._slots = deque()
        self.pqueues = dict()     # slot -> priority queue
        self.qfactory = qfactory  # factory for creating new internal queues

    def push(self, request, priority=0):
        slot = request.get('meta', {}).get('scheduler_slot', None)
        if slot not in self.pqueues:
            self.pqueues[slot] = PriorityQueue(self.qfactory)
            self._slots.append(slot)
        self.pqueues[slot].push(request, priority)

    def pop(self):
        if not self._slots:
            return
        slot = self._slots.popleft()
        queue = self.pqueues[slot]
        request = queue.pop()

        if len(queue):
            self._slots.append(slot)
        else:
            del self.pqueues[slot]
        return request

    def close(self):
        for queue in self.pqueues.values():
            queue.close()
        self.pqueues.clear()
        self._slots.clear()

    def __len__(self):
        return sum(len(x) for x in self.pqueues.values()) if self.pqueues else 0


class DiskQueue(PickleLifoDiskQueue):
    """
    Hack: this disk queue supports RoundRobinPriorityQueue,
    but it doesn't support crawl resuming. It allows to save RAM
    by keeping requests on disk.
    """
    ids = count()

    def __init__(self, path):
        path = path + "-" + str(next(self.ids))
        super().__init__(path)
