import logging
import time
from datetime import datetime

from .models import HTTP_TYPES, PROXY_RESULT_TYPE, Proxy, AbstractProxyProcessor
from .utils import get_response_speed, repr_response


logger = logging.getLogger(__name__)

MOCKBIN_COM = 'mockbin.com'  # slightly faster
HTTPBIN_ORG = 'httpbin.org'

CHECK_URLS = {
    MOCKBIN_COM: {
        'http': 'http://mockbin.com/request',
        'https': 'https://mockbin.com/request',
    },
    HTTPBIN_ORG: {
        'http': 'http://httpbin.org/get?show_env=1',
        'https': 'https://httpbin.org/get?show_env=1',
    },
}


class ProxyChecker(AbstractProxyProcessor):
    def __init__(self, proxy=None, pool=None, pool_size=None, blacklist=None,
                 timeout=10, retry_count=0, retry_wait=0,
                 http_check=True, https_check=True, https_force_check=False,
                 target=MOCKBIN_COM, history=0):
        super().__init__(proxy, pool, pool_size, blacklist)

        if target not in (HTTPBIN_ORG, MOCKBIN_COM):
            raise ValueError('Unknown checker target: %S' % target)
        self.target = target

        self.timeout = timeout
        self.retry_count = retry_count
        self.retry_wait = retry_wait

        self.http_check = http_check
        self.https_check = https_check
        # Checks even if only http is supported
        # Needed for some fetchers that not reporting that proxy supports https
        self.https_force_check = https_force_check

        self.history = history

        # To avoid parallel processing of same proxy from different fetchers
        self._processing = set()

    def __call__(self, *proxies, join=False):
        for proxy in proxies:
            if proxy.addr not in self._processing and proxy.addr not in self.blacklist:
                self._processing.add(proxy.addr)
                self.spawn(self.worker, proxy)
        if join:
            self.workers.join()

    def create_session(self):
        # Lazy import requests because of gevent.monkey_patch
        from .requests import ConfigurableSession
        return ConfigurableSession(
            forgetful_cookies=True,
            allow_redirects=False,
            timeout=self.timeout,
            # TODO: merge retry_count and HTTPAdapter.max_retries?
            retry_count=self.retry_count,
            retry_wait=self.retry_wait,
            adapter={'pool_connections': 1, 'pool_maxsize': 1}
        )

    def worker(self, proxy):
        if proxy.addr in self.blacklist:
            # because blacklist may be changed after __call__
            logger.debug('Check skipped: %s', proxy.addr)
            self._processing.remove(proxy.addr)
            return
        session = self.create_session()
        https_support = proxy.types.intersection([Proxy.TYPE.HTTPS, Proxy.TYPE.SOCKS4,
                                                  Proxy.TYPE.SOCKS5])

        success = None
        if ((self.https_check and https_support) or self.https_force_check):
            success = self.check(session, 'https', proxy)
            if proxy.types.intersection(HTTP_TYPES):
                if success:
                    proxy.types.add(Proxy.TYPE.HTTPS)
                elif Proxy.TYPE.HTTPS in proxy.types:
                    proxy.types.remove(Proxy.TYPE.HTTPS)

        # Assuming that if we have https working, http working also
        # TODO: we can't test anonymity then
        if (self.http_check and success is None):
            success = self.check(session, 'http', proxy)

        # TODO: maybe add logging that proxy is skipped?
        # don't make this an error? Сынк эбаут ит!
        assert success is not None, 'proxy not checked'
        self._processing.remove(proxy.addr)
        self.process_proxy(proxy)

    def check(self, session, protocol, proxy):
        proxies = {'http': proxy.url, 'https': proxy.url}
        try:
            start_at = time.time()
            resp = session.get(CHECK_URLS[self.target][protocol], proxies=proxies)
            resp.raise_for_status()

            # TODO: anonymity check for http and fail proxy instead of assert
            if self.target == HTTPBIN_ORG:
                assert 'origin' in resp.json(), 'Checker wrong response'
            elif self.target == MOCKBIN_COM:
                assert 'clientIPAddress' in resp.json(), 'Checker wrong response'

        except Exception as exc:
            logger.debug('Check %s fail: %s: %s', protocol, proxy.addr, exc)
            proxy.fail_at = datetime.utcnow()
            proxy.fail += 1
            if self.history:
                proxy.set_history(proxy.fail_at, PROXY_RESULT_TYPE.FAIL,
                                  repr(exc), 'checker', self.history)
            return False
        else:
            logger.debug('Check %s success: %s', protocol, proxy.addr)
            proxy.success_at = datetime.utcnow()
            proxy.fail = 0
            proxy.speed = get_response_speed(resp, start_at)
            if self.history:
                proxy.set_history(proxy.success_at, PROXY_RESULT_TYPE.SUCCESS,
                                  repr_response(resp), 'checker', self.history)
            return True
        finally:
            session.close()
