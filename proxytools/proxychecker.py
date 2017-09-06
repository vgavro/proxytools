import gevent.pool


class ProxyChecker:
    def __init__(self, pool=None, pool_size=10, timeout=5, session=None):
        self.pool = pool or gevent.pool.Pool(pool_size)
        self.session = session or self.create_session()
        self.workers = gevent.pool.Group()

        self.timeout = timeout

    def __call__(self, proxies, join=False):
        for proxy in proxies:
            self.workers.add(self.pool.spawn(self.worker, proxy))
        if join:
            self.workers.join()

    def create_session(self):
        import requests
        return requests.Session()

    def worker(self, proxy):
        print('checking', proxy.url)
        proxies = {'http': proxy.url, 'https': proxy.url}
        try:
            print('http', proxy, self.session.get('http://httpbin.org/get', timeout=self.timeout,
                                                  proxies=proxies).text)
        except Exception as exc:
            print('http', proxy, exc)
        try:
            print('https', proxy, self.session.get('https://httpbin.org/get', timeout=self.timeout,
                                                   proxies=proxies).text)
        except Exception as exc:
            print('https', proxy, exc)


def main():
    from .utils import gevent_monkey_patch
    gevent_monkey_patch()
    import pickle

    proxies = pickle.load(open('proxies', 'rb'))
    checker = ProxyChecker()
    checker([p for p in proxies if p.TYPE.HTTP in p.types], join=True)
