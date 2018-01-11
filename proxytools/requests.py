import re
from datetime import datetime
from collections import OrderedDict

from urllib3.poolmanager import ProxyManager
from urllib3.exceptions import IncompleteRead
from requests.cookies import RequestsCookieJar
from requests.adapters import HTTPAdapter
from requests.sessions import Session
from requests.utils import select_proxy, urldefragauth
from gevent import sleep

from .exceptions import ProxyListError, ProxyMaxRetriesExceeded
from .superproxy import SUPERPROXY_REQUEST_HEADERS
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
    instead of setting them later and extends with common functionality.
    """
    def __init__(self, request_wait=0, retry_response=None, retry_exception=None,
                 retry_count=0, retry_wait=0, forgetful_cookies=False,
                 enforce_content_length=False, random_user_agent=False, **kwargs):
        super().__init__()

        # to specify ordering this may be OrderedDict
        mount = kwargs.pop('mount', {})
        for prefix, adapter in mount.items():
            self.mount(prefix, adapter)

        for k, v in kwargs.items():
            if k in ('headers', 'auth', 'proxies', 'hooks', 'params', 'stream',
                     'verify', 'cert', 'max_redirects', 'trust_env', 'cookies',
                     'timeout', 'allow_redirects'):
                setattr(self, k, v)
            else:
                raise TypeError('Unknown keyword argument: %s', k)

        self.request_wait = request_wait
        self.request_at = None
        self.retry_response = retry_response
        self.retry_exception = retry_exception
        self.retry_count = retry_count
        self.retry_wait = retry_wait

        if forgetful_cookies:
            assert 'cookies' not in kwargs
            self.cookies = ForgetfulCookieJar()

        self.enforce_content_length = enforce_content_length

        if random_user_agent:
            self.headers['User-Agent'] = get_random_user_agent(random_user_agent)

    def request(self, *args, **kwargs):
        kwargs.setdefault('timeout', getattr(self, 'timeout', None))
        if hasattr(self, 'allow_redirects'):
            kwargs['allow_redirects'] = self.allow_redirects

        request_wait = kwargs.pop('request_wait', self.request_wait)
        retry_response = kwargs.pop('retry_response', self.retry_response)
        retry_exception = kwargs.pop('retry_exception', self.retry_exception)
        retry_count = kwargs.get('retry_count', self.retry_count)
        retry_wait = retry_default_wait = kwargs.get('retry_wait', self.retry_wait)

        for retry in range(retry_count + 1):
            wait = retry and (retry_wait if isinstance(retry_wait, (int, float))
                              else retry_default_wait) or request_wait
            while wait and self.request_at:
                # checking continuous for simultaneous use
                delta = (datetime.utcnow() - self.request_at).total_seconds()
                if 0 < delta < wait:
                    sleep(wait - delta)
                else:
                    break
            self.request_at = datetime.utcnow()

            try:
                resp = super().request(*args, **kwargs)
                if self.enforce_content_length and resp.raw.length_remaining not in (0, None):
                    # NOTE: This param may be set in urllib3.response.HTTPResponse,
                    # but there is no possibility to pass it to requests.adapters.HTTPAdapter.
                    # Also we couldn't reproduce success response if Content-Length is greater
                    # than real content body (requests fails with ReadTimeout error), but
                    # somehow we manage to get such responses from proxies.
                    read, remaining = resp.raw._fp_bytes_read, resp.raw.length_remaining
                    resp.close()
                    raise IncompleteRead(read, remaining)
            except Exception as exc:
                retry_wait = retry_exception and retry_exception(exc)
                if retry_wait and retry < retry_count:
                    continue
                raise
            retry_wait = retry_response and retry_response(resp)
            if retry_wait:
                continue
            break
        return resp


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
        self._persist_addr = None
        super().__init__(**kwargs)

    @staticmethod
    def _pop_response_match(key, params):
        # TODO: allow superproxy to pass serveral headers for this mechanics
        try:
            match = params.pop(key)
        except KeyError:
            return None
        if isinstance(match, (tuple, list)):
            return lambda resp: any(cb(resp) for cb in match)
        # assert callable(match)
        return match

    def _proxylist_call(self, func, *args, **kwargs):
        if kwargs.get('proxies'):
            raise ValueError('proxies argument is not empty, '
                             'but should be populated from proxylist')

        for k, v in self.proxy_kwargs.items():
            kwargs.setdefault(k, v)

        strategy = kwargs.pop('proxy_strategy', self.proxylist._get_fastest)
        max_retries = kwargs.pop('proxy_max_retries', PROXY_MAX_RETRIES_DEFAULT)
        success_response = self._pop_response_match('proxy_success_response', kwargs)
        success_timeout = kwargs.pop('proxy_success_timeout', None)
        fail_response = self._pop_response_match('proxy_fail_response', kwargs)
        fail_timeout = kwargs.pop('proxy_fail_timeout', None)
        rest_response = self._pop_response_match('proxy_rest_response', kwargs)
        rest_timeout = kwargs.pop('proxy_rest_timeout', None)
        request_ident = kwargs.pop('proxy_request_ident', None)
        debug = kwargs.pop('proxy_debug', False)
        if rest_response and not rest_timeout:
            raise ValueError('rest_response must be used with rest_timeout > 0')
        persist = kwargs.pop('proxy_persist', False)
        persist_addr = self._persist_addr if persist is True else persist
        proxy_kwargs = {k[6:]: kwargs.pop(k) for k in tuple(kwargs.keys())
                        if k.startswith('proxy_')}
        allow_no_proxy = kwargs.pop('allow_no_proxy', self.allow_no_proxy)
        if allow_no_proxy:
            proxy_kwargs.setdefault('wait', False)

        exclude = []
        for _ in range(max_retries):

            try:
                proxy = self.proxylist.get(strategy, exclude=exclude, persist=persist_addr,
                                           request_ident=request_ident, **proxy_kwargs)
            except ProxyListError as exc:
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
                self.proxylist.fail(proxy, timeout=fail_timeout, exc=exc,
                                    request_ident=request_ident, debug=debug)
                if persist is True:
                    self._persist_addr = None
                exclude.append(proxy.addr)
                exc_ = exc  # workaround for "smart" python3 variable clearing
            else:
                if not proxy:
                    resp._proxy = None
                    # NOTE: no content validation if no proxy was used
                    return resp
                if rest_response and rest_response(resp):
                    self.proxylist.rest(proxy, timeout=rest_timeout, resp=resp,
                                        request_ident=request_ident, debug=debug)
                    if persist is True:
                        self._persist_addr = None
                    exclude.append(proxy.addr)

                elif ((not fail_response or not fail_response(resp)) and
                      (not success_response or success_response(resp))):
                    self.proxylist.success(proxy, timeout=success_timeout, resp=resp,
                                           request_ident=request_ident)
                    if persist is True:
                        self._persist_addr = proxy.addr
                    # NOTE: maybe remove it, test purpose only (also used in superproxy)
                    resp._proxy = proxy
                    return resp
                else:
                    self.proxylist.fail(proxy, timeout=fail_timeout, resp=resp,
                                        request_ident=request_ident)
                    if persist is True:
                        self._persist_addr = None
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
    Accepts arguments same as ProxyListSession and transfer parameters as X-Superproxy-* headers.
    NOTE: We're connecting to SuperProxy as plain proxy, even with https urls,
    this allows to validate responses on SuperProxy and transfer meta headers for each request.
    With response vaildation we may set proxies to rest state for some timeout
    (for example on rate limiting) or fail state.
    Success responses validation is highly recommended for http requests (not https),
    as plain proxy may return "NOT WORKING" with 200 status for example,
    and we must fail such proxies and add them to blacklist.
    As SuperProxy connects to proxies using CONNECT method for https urls,
    you may not validate success responses for https (certificate validation enabled by default).
    """
    timeout = TIMEOUT_DEFAULT * PROXY_MAX_RETRIES_DEFAULT

    def __init__(self, superproxy_url, **kwargs):
        self.proxy_kwargs = {k: kwargs.pop(k) for k in tuple(kwargs)
                             if k.startswith('proxy_')}
        self._persist_addr = None
        adapter = PlainHTTPSProxyManagerHTTPAdapter()
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
        persist = kwargs.pop('proxy_persist', False)
        if persist is True and self._persist_addr:
            kwargs['proxy_persist'] = self._persist_addr

        proxy_kwargs = {k: kwargs.pop(k) for k in tuple(kwargs)
                        if k.startswith('proxy_')}
        for key, value in proxy_kwargs.items():
            if key in ['proxy_timeout', 'proxy_allow_no_proxy']:
                key = key[6:]
            headers['X-Superproxy-' + key.replace('_', '-').title()] = \
                SUPERPROXY_REQUEST_HEADERS[key][1](value)

        resp = super().request(method, url, headers=headers, **kwargs)
        error_cls_name = resp.headers.get('X-Superproxy-Error')
        if error_cls_name:
            if error_cls_name in ProxyListError.cls_map:
                error_arg = resp.text[len(error_cls_name) + 2:len(resp.text) - 3]
                raise ProxyListError.cls_map[error_cls_name](error_arg)
            raise Exception(error_cls_name)

        if persist:
            self._persist_addr = resp.headers.get('X-Superproxy-Addr') or None

        return resp


class PlainHTTPSProxyManager(ProxyManager):
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


class PlainHTTPSProxyManagerHTTPAdapter(HTTPAdapter):
    """
    HTTP adapter that is NOT USING "CONNECT" for https urls.
    """
    def proxy_manager_for(self, proxy, **proxy_kwargs):
        if (proxy not in self.proxy_manager and
           not proxy.lower().startswith('socks')):
            manager = self.proxy_manager[proxy] = PlainHTTPSProxyManager(
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
