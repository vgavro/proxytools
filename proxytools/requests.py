import re
from datetime import datetime
from collections import OrderedDict

from urllib3.poolmanager import ProxyManager
from requests.cookies import RequestsCookieJar
from requests.adapters import HTTPAdapter
from requests.sessions import Session
from requests.utils import select_proxy, urldefragauth
from gevent import Timeout, sleep

from .proxylist import ProxyMaxRetriesExceeded, InsufficientProxiesError
from .superproxy import SUPERPROXY_HEADERS
from .utils import repr_response, get_random_user_agent


PROXY_MAX_RETRIES_DEFAULT = 3
TIMEOUT_DEFAULT = 10


class ForgetfulCookieJar(RequestsCookieJar):
    # from https://github.com/requests/toolbelt/blob/master/requests_toolbelt/cookies/forgetful.py
    def set_cookie(self, *args, **kwargs):
        return


class SharedProxyManagerHTTPAdapter(HTTPAdapter):
    """
    ProxyManager holds connection pool, so if we're using different sessions,
    which connects to same proxies, it's useful to share proxy managers between sessions.
    """
    def __init__(self, proxy_manager, **kwargs):
        super().__init__(**kwargs)
        self.proxy_manager = proxy_manager


class ConfigurableSession(Session):
    """
    Helper class that allows to pass some parameters to __init__
    instead of settings them later.
    Extends with request_wait, forgetful_cookies and random_user_agent params.
    Allows to set default timeout for each request and
    override allow_redirects.
    """
    def __init__(self, request_wait=0, forgetful_cookies=False,
                 random_user_agent=False, **kwargs):
        super().__init__()

        # to specify ordering this may be OrderedDict
        mount = kwargs.pop('mount', {})
        for prefix, adapter in mount.items():
            self.mount(prefix, adapter)

        _configurable_attrs = [
            'headers', 'auth', 'proxies', 'hooks',
            'params', 'stream', 'verify', 'cert', 'max_redirects',
            'trust_env', 'cookies',
            'timeout', 'allow_redirects'
        ]
        for k, v in kwargs.items():
            if k in _configurable_attrs:
                setattr(self, k, v)
            else:
                raise TypeError('Unknown keyword argument: %s', k)

        self.request_wait = request_wait
        self.request_at = None

        if forgetful_cookies:
            assert 'cookies' not in kwargs
            self.cookies = ForgetfulCookieJar()

        if random_user_agent:
            self.headers['User-Agent'] = get_random_user_agent(random_user_agent)

    def request(self, *args, **kwargs):
        kwargs.setdefault('timeout', getattr(self, 'timeout', None))
        if hasattr(self, 'allow_redirects'):
            kwargs['allow_redirects'] = self.allow_redirects
        if self.request_wait and self.request_at:
            delta = (datetime.utcnow() - self.request_at).total_seconds()
            if 0 < delta < self.request_wait:
                sleep(self.request_wait - delta)
        try:
            return super().request(*args, **kwargs)
        finally:
            self.request_at = datetime.utcnow()


class RegexpMountSession(Session):
    """
    Allows to mount HTTPAdapter by regular expression.
    Useful if you want to mount custom HTTPAdapter (for example ProxyListHTTPAdapter)
    only to specific urls, but you haven't proper url hierarchy.
    """
    def __init__(self, regexp_adapters={}, **kwargs):
        self.regexp_adapters = OrderedDict()
        for pattern, adapter in regexp_adapters.items():
            self.regexp_mount(pattern, adapter)

        super().__init__(**kwargs)

    def regexp_mount(self, pattern, adapter):
        if not isinstance(pattern, re._pattern_type):
            # compat for python < 3.6
            # see https://stackoverflow.com/a/34178375/450103
            # and https://stackoverflow.com/a/30943547/450103
            pattern = re.compile(pattern)
        self.regexp_adapters[pattern] = adapter

    def get_adapter(self, url):
        for pattern, adapter in self.regexp_adapters.items():
            if re.match(url):
                return adapter
        return super().get_adapter(url)


class ProxyListMixin:
    def __init__(self, proxylist, allow_no_proxy=False, **kwargs):
        self.proxylist = proxylist
        self.allow_no_proxy = allow_no_proxy
        self.proxy_kwargs = {k: kwargs.pop(k) for k in tuple(kwargs.keys())
                             if k.startswith('proxy_')}
        self._preserve_addr = None
        super().__init__(**kwargs)

    def _proxylist_call(self, func, *args, **kwargs):
        if kwargs.get('proxies'):
            raise ValueError('proxies argument is not empty, '
                             'but should be populated from proxylist')

        for k, v in self.proxy_kwargs.items():
            kwargs.setdefault(k, v)

        strategy = kwargs.pop('proxy_strategy', self.proxylist._get_fastest)
        max_retries = kwargs.pop('proxy_max_retries', PROXY_MAX_RETRIES_DEFAULT)
        response_validator = kwargs.pop('proxy_response_validator', None)
        preserve = kwargs.pop('proxy_preserve', False)
        preserve_addr = self._preserve_addr if preserve is True else preserve
        proxy_kwargs = {k[6:]: kwargs.pop(k) for k in tuple(kwargs.keys())
                        if k.startswith('proxy_')}
        allow_no_proxy = kwargs.pop('allow_no_proxy', self.allow_no_proxy)
        if allow_no_proxy:
            proxy_kwargs.setdefault('wait', False)

        exclude = []
        for _ in range(max_retries):

            try:
                proxy = self.proxylist.get(strategy, exclude=exclude, preserve=preserve_addr,
                                           **proxy_kwargs)
            except (ProxyMaxRetriesExceeded, InsufficientProxiesError, Timeout) as exc:
                if allow_no_proxy:
                    proxy = None
                    kwargs['proxies'] = None
                else:
                    raise
            else:
                kwargs['proxies'] = {'http': proxy.url, 'https': proxy.url}

            exc_ = None  # workaround for "smart" python3 variable clearing
            try:
                resp = func(*args, **kwargs)
            except Exception as exc:
                # NOTE: timeout extends BaseException and should not match
                if not proxy:
                    raise
                self.proxylist.fail(proxy, exc=exc)
                exclude.append(proxy.addr)
                exc_ = exc  # workaround for "smart" python3 variable clearing
            else:
                if not proxy:
                    resp._proxy = None
                    # NOTE: no content validator if no proxy was used
                    return resp
                elif not response_validator or response_validator(resp):
                    self.proxylist.success(proxy)
                    if preserve is True:
                        self._preserve_addr = proxy.addr
                    # NOTE: maybe remove it, test purpose only (also used in superproxy)
                    resp._proxy = proxy
                    return resp
                else:
                    self.proxylist.fail(proxy, resp=resp)
                    if preserve is True:
                        self._preserve_addr = None
                    exclude.append(proxy.addr)
        reason_repr = exc_ and repr(exc_) or repr_response(resp)
        raise ProxyMaxRetriesExceeded('Max retries exceeded: {} {}'
                                      .format(max_retries, reason_repr))


class ProxyListHTTPAdapter(ProxyListMixin, SharedProxyManagerHTTPAdapter):
    """
    Adapter that is using proxies from ProxyList.
    Useful if you want only specific urls to serve through proxies,
    and other urls you want to serve directly.
    """
    def __init__(self, proxylist, **kwargs):
        super().__init__(proxylist, proxy_manager=proxylist.proxy_pool_manager, **kwargs)

    def send(self, *args, **kwargs):
        return self._proxylist_call(super().send, *args, **kwargs)


class ProxyListSession(ProxyListMixin, ConfigurableSession):
    """
    Session that is using proxies from ProxyList.
    """
    # Never work with proxies without timeout!
    # NOTE: this timeout applies to each request,
    # so total timeout would be proxy_max_retries * timeout
    timeout = TIMEOUT_DEFAULT

    def __init__(self, proxylist, **kwargs):
        adapter = SharedProxyManagerHTTPAdapter(proxylist.proxy_pool_manager)
        kwargs['mount'] = {'http://': adapter, 'https://': adapter}
        super().__init__(proxylist, **kwargs)

    def request(self, *args, **kwargs):
        # TODO: for now redirects are done without proxy,
        # because it uses self.send method directly, and we
        # can't easily pass proxy_kwargs there
        kwargs['allow_redirects'] = False
        return self._proxylist_call(super().request, *args, **kwargs)


class SuperProxySession(ConfigurableSession):
    """
    Session that is using SuperProxy daemon as proxy.
    Accepts arguments same as ProxyListSession and transfer
    parameters as X-Superproxy-* headers.
    NOTE: We're connecting to SuperProxy as plain proxy,
    even with https urls, this allows to validate responses on SuperProxy
    and transfer required X-Superproxy-* headers.
    Responses validation is highly recommended, as proxies may return
    "NOT WORKING" with 200 status for example, and we must fail
    such proxies and add them to blacklist.
    """
    timeout = TIMEOUT_DEFAULT * PROXY_MAX_RETRIES_DEFAULT

    def __init__(self, superproxy_url, **kwargs):
        self.proxy_kwargs = {k: kwargs.pop(k) for k in tuple(kwargs)
                             if k.startswith('proxy_')}
        self._preserve_addr = None
        adapter = SimpleHTTPSProxyManagerHTTPAdapter()
        kwargs['mount'] = {'http://': adapter, 'https://': adapter}
        kwargs['proxies'] = {'http': superproxy_url, 'https': superproxy_url}
        super().__init__(**kwargs)

    def request(self, method, url, headers={}, **kwargs):
        # TODO: for now redirects are done without proxy,
        # because it uses self.send method directly, and we
        # can't easily pass proxy_kwargs there
        kwargs['allow_redirects'] = False

        for k, v in self.proxy_kwargs.items():
            kwargs.setdefault(k, v)
        preserve = kwargs.pop('proxy_preserve', False)
        if preserve is True and self._preserve_addr:
            kwargs['proxy_preserve'] = self._preserve_addr

        proxy_kwargs = {k: kwargs.pop(k) for k in tuple(kwargs)
                        if k.startswith('proxy_')}
        for key, value in proxy_kwargs.items():
            headers['X-Superproxy-' + key.replace('_', '-').title()] = \
                SUPERPROXY_HEADERS[key][1](value)

        resp = super().request(method, url, headers=headers, **kwargs)

        if preserve:
            self._preserve_addr = resp.headers.get('X-Superproxy-Addr')

        return resp


class SimpleHTTPSProxyManager(ProxyManager):
    """
    Proxy manager that is NOT USING "CONNECT" for https urls.
    """
    def connection_from_host(self, host, port=None, scheme='http', pool_kwargs=None):
        return super(ProxyManager, self).connection_from_host(
            self.proxy.host, self.proxy.port, self.proxy.scheme, pool_kwargs=pool_kwargs)

    def urlopen(self, method, url, redirect=True, **kw):
        # Seems requests is not using urlopen method anyway
        headers = kw.get('headers', self.headers)
        kw['headers'] = self._set_proxy_headers(url, headers)
        return super(ProxyManager, self).urlopen(method, url, redirect=redirect, **kw)


class SimpleHTTPSProxyManagerHTTPAdapter(HTTPAdapter):
    """
    HTTP adapter that is NOT USING "CONNECT" for https urls.
    """
    def proxy_manager_for(self, proxy, **proxy_kwargs):
        if (proxy not in self.proxy_manager and
           not proxy.lower().startswith('socks')):
            manager = self.proxy_manager[proxy] = SimpleHTTPSProxyManager(
                proxy,
                proxy_headers=self.proxy_headers(proxy),
                num_pools=self._pool_connections,
                maxsize=self._pool_maxsize,
                block=self._pool_block,
                **proxy_kwargs)
            return manager
        return super().proxy_manager_for(proxy, **proxy_kwargs)

    def request_url(self, request, proxies):
        proxy = select_proxy(request.url, proxies)
        if (not proxy.lower().startswith('socks') and
           request.url.lower().startswith('https://')):
            return urldefragauth(request.url)
        return super().request_url(request, proxies)
