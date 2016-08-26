#!/usr/bin/env python3
"""
Usage: linkdepth.py urls.txt items.jl
"""
import csv
import time
import argparse
from urllib.parse import urlsplit
from collections import defaultdict

import tldextract
import autopager
import scrapy
from w3lib.url import canonicalize_url
from scrapy.crawler import CrawlerProcess
from scrapy.utils.url import add_http_if_no_scheme
from scrapy.http.response import Response
from scrapy.linkextractors import LinkExtractor


HOUR = 60 * 60


def get_netloc(url):
    return urlsplit(url).netloc


def get_domain(url):
    return tldextract.extract(url).registered_domain.lower()


def normalize_url(url):
    url = add_http_if_no_scheme(url)
    return canonicalize_url(url).replace('https://', 'http://', 1)


def read_urls(fp):
    """ Read a csv file with urls """
    reader = csv.DictReader(fp)
    for row in reader:
        url = add_http_if_no_scheme(row['url'])
        start_url = row.get('start', "http://" + get_netloc(url))
        start_depth = int(row.get('start_depth', 0))
        yield url, start_url, start_depth


class DepthSpider(scrapy.Spider):
    """
    A spider to monitor URL depth.

    1. Check all URLs, discard 404.
    2. When all URLs for a given netloc are checked, start a crawl for
       this netloc, looking for links.

    There are two crawling algorithms:

    a) prioritize pagination links - it may help because we're looking for
       detail pages, and list pages usually have multiple links to them;
    b) standard BFS crawl.

    Crawl for a netloc is stopped either when all URLs are found, or when
    an URL limit for this domain is reached.
    """
    name = 'linkdepth'
    urls = None  # path to a file with urls to monitor

    PRIO_INIT = 10000
    PRIO_DOMAIN = 1000
    PRIO_PAGE = 10

    bfs = False
    crawl_id = None

    def start_requests(self):
        self.le = LinkExtractor(canonicalize=False)

        self._urls_to_check = defaultdict(set)  # domain -> url
        self._urls_to_find = defaultdict(set)   # domain -> url
        self._urls_found = set()
        self._starts = {}

        with open(self.urls, 'rt') as f:
            for url, start_depth, start_url in read_urls(f):
                netloc = get_netloc(url)
                self._urls_to_check[netloc].add(url)
                self._starts[normalize_url(url)] = start_depth, start_url
                req = scrapy.Request(url,
                    callback=self.parse_seed,
                    errback=self.parse_seed_error,
                    meta={
                        'url': url,
                        'netloc': netloc,
                        'scheduler_slot': netloc,
                    },
                    dont_filter=True,
                    priority=self.PRIO_INIT,
                )
                yield from self._force_crawl(req)

    def _force_crawl(self, request):
        try:
            # force crawling of this request
            self.crawler.engine.crawl(request, self)
        except AssertionError:  # spider is not opened
            yield request

    def parse_seed(self, response: Response):
        url = response.meta['url']
        netloc = response.meta['netloc']
        self._urls_to_check[netloc].discard(url)
        to_find = self._urls_to_find[netloc]
        to_find.add(normalize_url(url))
        yield from self.maybe_start_domain_crawl(netloc)

    def parse_seed_error(self, failure):
        meta = failure.request.meta
        url = meta['url']
        netloc = meta['netloc']
        self._urls_to_check[netloc].discard(meta['url'])
        self.logger.info("%s is unavailable: %r" % (url, failure))
        yield from self.maybe_start_domain_crawl(netloc)

    def maybe_start_domain_crawl(self, netloc: str):
        if self._urls_to_check[netloc]:
            return

        to_find = self._urls_to_find[netloc]
        if not to_find:
            self.logger.warning("No good URLs for domain %s." % netloc)
            return

        self.logger.info(
            "Urls for %s are checked, %s of them are OK. "
            "Starting domain crawl." % (netloc, len(to_find))
        )
        for start_url, start_depth in {self._starts[url] for url in to_find}:
            yield scrapy.Request(start_url, self.parse_domain,
                priority=self.PRIO_DOMAIN,
                meta={
                    'request_depth': start_depth,
                    'netloc': netloc,
                    'scheduler_slot': netloc,
                }
            )

    def parse_domain(self, response: Response):
        info = self._request_info(response, response.url, visited=True)
        yield from self._handle_ground_truth(info)

        netloc = response.meta['netloc']
        if get_netloc(response.url) != netloc:
            self.logger.debug(
                "Filtering off-domain response %s: "
                "netloc is not %s" % (response, netloc)
            )
            return

        if not self._urls_to_find[netloc]:
            # all GT urls are found on this domain
            return

        next_depth = response.meta['request_depth'] + 1
        for url, prio in self._get_links(response):
            info = self._request_info(response, url, visited=False)
            if get_netloc(url) == netloc:
                yield scrapy.Request(url, self.parse_domain, priority=prio, meta={
                    'netloc': response.meta['netloc'],
                    'request_depth': next_depth,
                    'scheduler_slot': netloc,
                    'request_info': info,
                })
            yield from self._handle_ground_truth(info)

    def _get_links(self, response: Response):
        """ Return (link, priority) tuples for a response """

        if not self.bfs:
            page_hrefs = autopager.select(response).xpath("@href").extract()
            if page_hrefs:
                self.logger.info("Pagination detected at %s" % response.url)

            for href in page_hrefs:
                url = response.urljoin(href)
                yield url, self.PRIO_PAGE

        for link in self.le.extract_links(response):
            yield link.url, 0

    def _handle_ground_truth(self, request_info):
        if not request_info['ground_truth']:
            return

        url = request_info['url']
        url_norm = normalize_url(url)
        netloc = get_netloc(url)
        to_find = self._urls_to_find[netloc]

        self.logger.info("ground truth URL found: %s" % url)
        to_find.discard(url_norm)
        if not to_find:
            self.logger.info("All pages for %s are found!" % netloc)
        yield request_info

    def _request_info(self, response: Response, url: str, visited: bool):
        request = response.request  # type: scrapy.Request
        netloc = get_netloc(url)
        url_norm = normalize_url(url)
        ground_truth = url_norm in self._urls_to_find[netloc]

        return {
            'url': url,
            'ground_truth': ground_truth,
            'found_at': response.url,
            'sent_at': time.time(),
            'crawl': self.crawl_id,
            'autopager': not self.bfs,
            'depth': response.meta['request_depth'] + (0 if visited else 1),
            'priority': request.priority,
            '_visited': visited,
            '_respone_depth': response.meta['depth'],
        }

    def should_drop(self, request: scrapy.Request):
        netloc = request.meta.get('netloc')
        if not netloc or self._urls_to_check[netloc]:
            return

        if not self._urls_to_find[netloc]:
            # stop crawling a domain once all URLs are found
            return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Figire out depth of the urls. "
    )
    parser.add_argument("urls",
                        help="A CSV file with urls to check. It should have "
                             "'url' column; 'start' and 'start_depth' columns "
                             "are also supported."
                        )
    parser.add_argument("result", help="Result file, e.g. depths.jl")
    parser.add_argument("--bfs", action="store_true",
                        help="Run a BFS crawl instead of using autopager")
    parser.add_argument("--limit", type=int, default=1000000,
                        help="Max requests per netloc (defualt: %(default)s)")
    args = parser.parse_args()

    crawl_id = time.time()
    crawl_name = '%s-%s' % ('bfs' if args.bfs else 'pager', crawl_id)

    cp = CrawlerProcess(dict(
        AUTOTHROTTLE_ENABLED=True,
        AUTOTHROTTLE_START_DELAY=1,
        AUTOTHROTTLE_MAX_DELAY=10,
        # DOWNLOAD_DELAY=1,
        ROBOTSTXT_OBEY=False,
        CONCURRENT_REQUESTS=48,
        MEMUSAGE_ENABLED=True,
        FEED_FORMAT='jsonlines',
        FEED_URI=args.result,
        LOG_FILE='linkdepth-%s.log' % crawl_name,
        LOG_LEVEL='DEBUG',
        DEPTH_PRIORITY=1,
        JOBDIR='.scrapy/%s' % crawl_id,
        MAX_REQUESTS_PER_NETLOC=args.limit,
        DOWNLOADER_MIDDLEWARES={
            'middleware.MaxRequestsMiddleware': 650,  # after redirects
            'middleware.DropRequestMiddleware': 651,
        },
        SCHEDULER='scheduler.LoggingScheduler',
        SCHEDULER_PUSH_LOG='linkdepth-scheduler-%s.jl.gz' % crawl_name,
        SCHEDULER_PRIORITY_QUEUE='queues.RoundRobinPriorityQueue',
        SCHEDULER_DISK_QUEUE='queues.DiskQueue',
        CLOSESPIDER_TIMEOUT=HOUR * 18,
    ))
    cp.crawl(DepthSpider, urls=args.urls, bfs=args.bfs, crawl_id=crawl_id)
    cp.start()
