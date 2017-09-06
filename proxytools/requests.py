from requests.adapters import HTTPAdapter
from requests.sessions import Session

from .proxylist import ProxyMaxRetriesExceeded


PROXY_MAX_RETRIES_DEFAULT = 3


class ResponseValidator:
    def __init__(self, status=None, content=None, callback=None):
        if content and not isinstance(content, (tuple, list)):
            content = [content]
        if status and not isinstance(status, (tuple, list)):
            status = [status]
        self.status, self.content, self.callback = status, content, callback

    def __call__(self, resp):
        if self.content and not any(x in resp.content for x in self.content):
            return False
        if self.status and resp.status_code not in self.status:
            return False
        if self.callback:
            return self.callback(resp)
        return True


def _call_with_proxylist(proxylist, func, *args, **kwargs):
    proxy_max_retries = kwargs.pop('proxy_max_retries', PROXY_MAX_RETRIES_DEFAULT)
    proxy_response_validator = kwargs.pop('proxy_response_validator', None)
    exclude = []
    for _ in range(proxy_max_retries):
        proxy = proxylist.get_random(exclude=exclude)
        if kwargs.get('proxies'):
            raise ValueError('proxies argument is not empty, '
                             'but should be populated from proxylist')
        kwargs['proxies'] = {'http': proxy.url, 'https': proxy.url}
        try:
            resp = func(*args, **kwargs)
            # TODO: clear self.proxy_manager dict to prevent overflow on many proxies?
            # TODO: do not keep connections to proxy on initialization
            # TODO: keep proxy_managers in proxy_list maybe? for connection caching?
        except Exception as exc:
            proxylist.failed(proxy, exc=exc)
            exclude.append(proxy.url)
        else:
            if not proxy_response_validator or proxy_response_validator(resp):
                proxylist.succeed(proxy)
                return resp
            else:
                proxylist.failed(proxy, resp=resp)
                exclude.append(proxy.url)
    raise ProxyMaxRetriesExceeded('Max retries exceeded: ' + str(proxy_max_retries))


class SharedProxyManagerHTTPAdapter(HTTPAdapter):
    def __init__(self, proxy_manager, **kwargs):
        super().__init__(**kwargs)
        self.proxy_manager = proxy_manager


class ProxyListHTTPAdapter(SharedProxyManagerHTTPAdapter):
    def __init__(self, proxylist, proxy_max_retries=PROXY_MAX_RETRIES_DEFAULT,
                 proxy_response_validator=None, **kwargs):
        self.proxylist = proxylist
        self.proxy_max_retries = proxy_max_retries
        self.proxy_response_validator = proxy_response_validator
        super().__init__(proxylist.proxy_pool_manager, **kwargs)

    def send(self, *args, **kwargs):
        kwargs.setdefault('proxy_max_retries', self.proxy_max_retries)
        kwargs.setdefault('proxy_response_validator', self.proxy_response_validator)
        return _call_with_proxylist(self.proxylist, super().send, *args, **kwargs)


class ProxyListSession(Session):
    # TODO: we must implement session to allow custom max_retries and ResponseValidator
    # per request, not per adapter, for example for SuperProxy requests
    def __init__(self, proxylist, proxy_max_retries=PROXY_MAX_RETRIES_DEFAULT,
                 proxy_response_validator=None, **kwargs):
        self.proxylist = proxylist
        self.proxy_max_retries = proxy_max_retries
        self.proxy_response_validator = proxy_response_validator
        super().__init__(**kwargs)
        self.mount('http://', SharedProxyManagerHTTPAdapter(proxylist.proxy_pool_manager))
        self.mount('https://', SharedProxyManagerHTTPAdapter(proxylist.proxy_pool_manager))

    def request(self, *args, **kwargs):
        kwargs.setdefault('proxy_max_retries', self.proxy_max_retries)
        kwargs.setdefault('proxy_response_validator', self.proxy_response_validator)
        return _call_with_proxylist(self.proxylist, super().request, *args, **kwargs)
