# -*- coding: utf-8 -*-
import json
import gzip
from scrapy.core.scheduler import Scheduler


class LoggingScheduler(Scheduler):
    """
    This scheduler stores pushed requests in a jsonlines file.
    Information to store must be in ``request.meta['request_info']``.

    It has to be done in a scheduler because there is no hook
    in Scrapy to execute code after request is deduplicated.
    """
    @classmethod
    def from_crawler(cls, crawler):
        cls = super().from_crawler(crawler)
        cls.log_path = crawler.settings.get('SCHEDULER_PUSH_LOG')
        cls.log_fp = None
        return cls

    def open(self, spider):
        if self.log_path:
            self.log_fp = gzip.open(self.log_path, 'at', encoding='utf8',
                                    compresslevel=3)
        return super().open(spider)

    def close(self, reason):
        if self.log_fp:
            self.log_fp.close()
        return super().close(reason)

    def log_request(self, request):
        """ Store request in a log """
        if not self.log_fp:
            return

        # request_info is removed to save disk space
        info = request.meta.pop('request_info', None)
        if not info:
            return

        self.log_fp.write(json.dumps(info))
        self.log_fp.write("\n")

    def enqueue_request(self, request):
        # XXX: the method is copy-pasted because dupefilter is hardcoded
        if not request.dont_filter and self.df.request_seen(request):
            self.df.log(request, self.spider)
            return False

        # ============  this is the only changed line  ============
        self.log_request(request)
        # =========================================================

        dqok = self._dqpush(request)
        if dqok:
            self.stats.inc_value('scheduler/enqueued/disk', spider=self.spider)
        else:
            self._mqpush(request)
            self.stats.inc_value('scheduler/enqueued/memory', spider=self.spider)
        self.stats.inc_value('scheduler/enqueued', spider=self.spider)
        return True
