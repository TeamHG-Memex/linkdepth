# -*- coding: utf-8 -*-
import logging
from collections import defaultdict
from urllib.parse import urlsplit

from scrapy.exceptions import NotConfigured, IgnoreRequest


logger = logging.getLogger(__name__)


def get_netloc(url):
    return urlsplit(url).netloc


class MaxRequestsMiddleware:
    """
    Downloader middleware for limiting a number of
    requests to a domain (netloc).
    """

    def __init__(self, max_requests, stats):
        self.stats = stats
        self.max_requests = max_requests
        self.requests_num = defaultdict(int)

    @classmethod
    def from_crawler(cls, crawler):
        max_requests = crawler.settings.getint('MAX_REQUESTS_PER_NETLOC', 0)
        if not max_requests:
            raise NotConfigured()
        return cls(max_requests=max_requests, stats=crawler.stats)

    def process_request(self, request, spider):
        netloc = request.meta.get('netloc', get_netloc(request.url))
        self.requests_num[netloc] += 1

        if self.requests_num[netloc] == self.max_requests:
            logger.info("Max requests limit reached for %s" % netloc)

        if self.requests_num[netloc] >= self.max_requests:
            self.stats.inc_value("MaxRequestsMiddleware/dropped")
            raise IgnoreRequest()


class DropRequestMiddleware:
    """
    Downloader middleware to drop a requests if a certain condition is met.
    It calls ``spider.should_drop(request)`` method to check if a request
    should be downloaded or dropped; spider must implement this method.
    """
    def __init__(self, stats):
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        return cls(stats=crawler.stats)

    def process_request(self, request, spider):
        if spider.should_drop(request):
            self.stats.inc_value("DropRequestMiddleware/dropped")
            raise IgnoreRequest()

