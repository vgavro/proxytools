# import sys
from datetime import datetime

import gevent.pool


HTTP_URL = 'http://httpbin.org/get?show_env=1'
HTTPS_URL = 'https://httpbin.org/get?show_env=1'


class ProxyChecker:
    def __init__(self, pool=None, add=None, pool_size=10, timeout=5, session=None,
                 max_retries=0, retry_timeout=0, https_force_check=True):

        # TODO: make abstract class for this routine
        self.pool = pool or gevent.pool.Pool(pool_size)
        self.workers = gevent.pool.Group()
        self.add = add

        # TODO: not implemented
        self.max_retries = max_retries
        self.retry_timeout = retry_timeout

        self.https_force_check = https_force_check

        self.timeout = timeout

    def __call__(self, *proxies, join=False):
        for proxy in proxies:
            self.workers.add(self.pool.spawn(self.worker, proxy))
        if join:
            self.workers.join()

    @property
    def ready(self):
        return not len(self.workers)

    def create_session(self):
        # Lazy import requests because of gevent.monkey_patch
        from .requests import ConfigurableSession, ForgetfulCookieJar
        return ConfigurableSession(cookies=ForgetfulCookieJar(), timeout=self.timeout)

    def worker(self, proxy):
        if not self.https_force_check:
            # TODO: think about arguments for all check cases!
            raise NotImplementedError('Only https_force_check is implemented yet')

        # Creating session each time not to hit [Errno 24] Too many open files
        session = self.create_session()

        proxies = {'http': proxy.url, 'https': proxy.url}
        try:
            resp = session.get(HTTPS_URL, timeout=self.timeout, proxies=proxies)
            resp.raise_for_status()
            assert 'origin' in resp.json()
        except Exception as exc:
            # TODO: add logging
            # print(f'Checked fail: {proxy.url}: {exc}', file=sys.stderr)
            return False
        else:
            # TODO: add logging
            # print(f'Checker success: {proxy.url}: {resp.text}', file=sys.stderr)
            proxy.checked_at = datetime.utcnow()
            if proxy.TYPE.HTTP in proxy.types and proxy.TYPE.HTTPS not in proxy.types:
                proxy.types.add(proxy.TYPE.HTTPS)
            self.add(proxy)
            return True  # TODO: maybe more smart callback for fail also?

# TODO: add cli
# def main():
#     from .utils import gevent_monkey_patch
#     gevent_monkey_patch()
#     import pickle
#
#     proxies = pickle.load(open('proxies', 'rb'))
#     checker = ProxyChecker()
#     checker([p for p in proxies if p.TYPE.HTTP in p.types], join=True)
