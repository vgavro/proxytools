import re
from collections import OrderedDict

from requests.cookies import RequestsCookieJar
from requests.adapters import HTTPAdapter
from requests.sessions import Session

from .proxylist import ProxyMaxRetriesExceeded
from .superproxy import SUPERPROXY_HEADERS


PROXY_MAX_RETRIES_DEFAULT = 3
TIMEOUT_DEFAULT = 10


def repr_response(resp, full=False):
    if not full and len(resp.content) > 128:
        content = '{}...{}b'.format(resp.content[:128], len(resp.content))
    else:
        content = resp.response.content
    return '{} {} {}: {}'.format(resp.request.method, resp.status_code,
                                 resp.url, content)


class ResponseMatch:
    """
    Helper class to be used instead callback to match response.
    For proxy_response_validator for example.
    """
    def __init__(self, status=None, content=None, header=None):
        if content and not isinstance(content, (tuple, list)):
            content = [content]
        if status and not isinstance(status, (tuple, list)):
            status = [status]
        if header:
            if not isinstance(header, (tuple, list)):
                header = ((header, None),)
            elif len(header) == 2 and not isinstance(header[0], (tuple, list)):
                header = (header,)
        self.status, self.content, self.header = status, content, header

    def __call__(self, resp):
        if self.content and not any(x in resp.content for x in self.content):
            return False
        if self.status and resp.status_code not in self.status:
            return False
        if self.header:
            matched = False
            for header, content in self.header:
                has_header = header in resp.headers
                matched = ((has_header and not content) or
                           (has_header and content and content in resp.headers[header]))
            if not matched:
                return False
        return True


class ForgetfulCookieJar(RequestsCookieJar):
    # from https://github.com/requests/toolbelt/blob/master/requests_toolbelt/cookies/forgetful.py
    def set_cookie(self, *args, **kwargs):
        return


class SharedProxyManagerHTTPAdapter(HTTPAdapter):
    def __init__(self, proxy_manager, **kwargs):
        super().__init__(**kwargs)
        self.proxy_manager = proxy_manager


class ConfigurableSession(Session):
    def __init__(self, **kwargs):
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

    def request(self, *args, **kwargs):
        kwargs.setdefault('timeout', getattr(self, 'timeout', None))
        kwargs.setdefault('allow_redirects', getattr(self, 'allow_redirects', True))
        return super().request(*args, **kwargs)


class RegexpMountSession(Session):
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
    def __init__(self, proxylist, **kwargs):
        self.proxylist = proxylist
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

        exclude = []
        for _ in range(max_retries):
            proxy = self.proxylist.get(strategy, exclude=exclude, preserve=preserve_addr,
                                       **proxy_kwargs)
            kwargs['proxies'] = {'http': proxy.url, 'https': proxy.url}
            exc_ = None  # workaround for "smart" python3 variable clearing
            try:
                resp = func(*args, **kwargs)
            except Exception as exc:
                self.proxylist.fail(proxy, exc=exc)
                exclude.append(proxy.addr)
                exc_ = exc  # workaround for "smart" python3 variable clearing
            else:
                if not response_validator or response_validator(resp):
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
    def __init__(self, proxylist, **kwargs):
        super().__init__(proxylist, proxy_manager=proxylist.proxy_pool_manager, **kwargs)

    def send(self, *args, **kwargs):
        return self._proxylist_call(super().send, *args, **kwargs)


class ProxyListSession(ProxyListMixin, ConfigurableSession):
    # Never work with proxies without timeout!
    # NOTE: this timeout applies to each request,
    # so total timeout would be proxy_max_retries * timeout
    timeout = TIMEOUT_DEFAULT

    def __init__(self, proxylist, forgetful_cookies=False, **kwargs):
        adapter = SharedProxyManagerHTTPAdapter(proxylist.proxy_pool_manager)
        kwargs['mount'] = {'http://': adapter, 'https://': adapter}
        if forgetful_cookies:
            kwargs['cookies'] = ForgetfulCookieJar()
        super().__init__(proxylist, **kwargs)

    def request(self, *args, **kwargs):
        # TODO: for now redirects are done without proxy,
        # because it uses self.send method directly, and we
        # can't easily pass proxy_kwargs there
        kwargs['allow_redirects'] = False
        return self._proxylist_call(super().request, *args, **kwargs)


class SuperProxySession(ConfigurableSession):
    def __init__(self, proxy, **kwargs):
        if not proxy.endswith('/'):
            proxy += '/'
        self.proxy = proxy
        self.proxy_kwargs = {k: kwargs.pop(k) for k in tuple(kwargs)
                             if k.startswith('proxy_')}
        self._preserve_addr = None
        super().__init__(**kwargs)

    def request(self, method, url, headers={}, **kwargs):
        # TODO: for now redirects are done without proxy,
        # because it uses self.send method directly, and we
        # can't easily pass proxy_kwargs there
        kwargs['allow_redirects'] = False

        url = self.proxy + url

        for k, v in self.proxy_kwargs.items():
            kwargs.setdefault(k, v)
        preserve = kwargs.get('proxy_preserve', False)
        if preserve is True and self._preserve_addr:
            kwargs['proxy_preserve'] = self._preserve_addr

        proxy_kwargs = {k: kwargs.pop(k) for k in tuple(kwargs)
                        if k.startswith('proxy_')}
        for key, value in proxy_kwargs.items():
            headers['X-Superproxy-' + key.replace('_', '-').title()] = \
                SUPERPROXY_HEADERS[key][1](value)

        resp = super().request(method, url, headers=headers, **kwargs)

        if preserve:
            self._preserve_addr = resp.headers['X-Superproxy-Addr']

        return resp
