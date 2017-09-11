import time

from gevent.pool import Pool

from proxytools.proxylist import ProxyList
from proxytools.proxychecker import ProxyChecker
from proxytools.proxyfetcher import ProxyFetcher
from proxytools.requests import ProxyListSession


def test_proxylist_session():
    checker = ProxyChecker(http_check=False, https_force_check=True)
    proxyfetcher = ProxyFetcher(ProxyFetcher.registry, checker=checker)
    proxylist = ProxyList(proxyfetcher, filename='./proxies.json', atexit_save=True)
    session = ProxyListSession(proxylist)

    def worker(x):
        started = time.time()
        print('Fetch start', x)
        resp = session.get('https://httpbin.org/get')
        assert 'origin' in resp.json(), resp.json()
        print('Fetch succeed', x, time.time() - started, resp._proxy.addr, resp._proxy.speed)
        # except Exception as exc:
        #     print('Fetch failed', x, time.time() - started, repr(exc))
    pool = Pool(5)

    [pool.spawn(worker, x) for x in range(20)]
    pool.join()
