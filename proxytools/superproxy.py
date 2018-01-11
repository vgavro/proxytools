import logging
from datetime import datetime, timedelta
from urllib.parse import parse_qsl
from itertools import chain
from base64 import b64decode
import json

from requests.status_codes import _codes, codes
from pytimeparse.timeparse import timeparse
import netaddr

from .models import PROXY_RESULT_TYPE
from .exceptions import ProxyListError
from .utils import ResponseMatch


logger = logging.getLogger(__name__)

ALLOWED_METHODS = ('GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'OPTIONS')

HOP_BY_HOP_HEADERS = frozenset([
    'connection', 'keep-alive', 'proxy-authenticate',
    'proxy-authorization', 'te', 'trailers', 'transfer-encoding',
    'upgrade', 'proxy-connection', 'content-encoding'
])

STATUS_CODE_TITLES = {code: titles[0].replace('_', ' ').title()
                      for code, titles in _codes.items()}

SUPERPROXY_REQUEST_HEADERS = {
    # Keyword arguments for proxylist.request: decode, encode
    'timeout': (int, str),
    'allow_no_proxy': (lambda x: bool(int(x)), lambda x: str(int(x))),
    'proxy_strategy': (lambda x: str(x).upper(), lambda x: str(x).upper()),
    'proxy_max_retries': (int, str),
    'proxy_wait': (lambda x: {'f': False, 't': True}.get(x, int(x)),
                   lambda x: str({True: 't', False: 'f'}.get(x, x))),
    'proxy_persist': (str, str),
    'proxy_countries': (lambda x: x.split(','), lambda x: ','.join(x)),
    'proxy_countries_exclude': (lambda x: x.split(','), lambda x: ','.join(x)),
    'proxy_min_speed': (float, str),
    'proxy_request_ident': (str, str),
    'proxy_success_response': (ResponseMatch._from_superproxy_header,
                               lambda x: x._to_superproxy_header()),
    'proxy_success_timeout': (int, str),
    'proxy_fail_response': (ResponseMatch._from_superproxy_header,
                            lambda x: x._to_superproxy_header()),
    'proxy_fail_timeout': (int, str),
    'proxy_rest_response': (ResponseMatch._from_superproxy_header,
                            lambda x: x._to_superproxy_header()),
    'proxy_rest_timeout': (int, str),
    'proxy_debug': (lambda x: bool(int(x)), lambda x: str(int(x))),
    # TODO: Add authorization header encoder for SuperproxySession,
    # because it's already implemented in wsgi app
}


def is_hop_by_hop(header):
    return header.lower() in HOP_BY_HOP_HEADERS


def reconstruct_url(environ):
    url = environ['PATH_INFO']
    # Fix ;arg=value in url  # TODO: does we really need this?
    # if '%3B' in url:
    #     url, arg = url.split('%3B', 1)
    #     url = ';'.join([url, arg.replace('%3D', '=')])
    # Stick query string back in
    if environ.get('QUERY_STRING'):
        return url + '?' + environ['QUERY_STRING']
    return url


def _match_proxy_search_token(p, token):
    return (
        token in p.addr or
        (p.country and token in p.country) or
        any(token in source for source in p.fetch_sources) or
        any(token in type.name for type in p.types)
    )


def _iter_proxies_by_status(proxylist, status):
    now = datetime.utcnow()
    iterable = []
    if 'active' in status or 'rest' in status:
        iterable = chain(iterable, proxylist.active_proxies.values())
    if 'blacklist' in status:
        iterable = chain(iterable, proxylist.blacklist_proxies.values())
    for p in iterable:
        if p.blacklist:
            if 'blacklist' not in status:
                continue
        elif p.rest_till and p.rest_till > now:
            if 'rest' not in status:
                continue
        elif 'active' not in status:
            continue
        yield p


class WSGISuperProxy:
    def __init__(self, proxylist, proxy_allow_addrs=None, admin_allow_addrs=None,
                 proxy_credentials=None, admin_credentials=None, **session_kwargs):
        from .requests import ProxyListSession
        self.session = ProxyListSession(proxylist, forgetful_cookies=True,
                                        enforce_content_length=True,
                                        **session_kwargs)
        self.proxylist = proxylist

        self.proxy_allow_addrs = proxy_allow_addrs and netaddr.IPGlob(proxy_allow_addrs)
        self.admin_allow_addrs = admin_allow_addrs and netaddr.IPGlob(admin_allow_addrs)
        self.proxy_credentials = proxy_credentials
        self.admin_credentials = admin_credentials

        assert __file__.endswith('.py')
        self.frontend_html = open( __file__[:-3] + '.html').read()

        self.admin_route = {
            '/superproxy/': self.frontend,
            '/status': self.status,
            '/mem_top': self.mem_top,
            '/proxies': self.proxies,
            '/waiting': self.waiting,
            '/history': self.history,
            '/action': self.action,
        }

    def __call__(self, environ, start_resp):
        if environ['REQUEST_METHOD'] not in ALLOWED_METHODS:
            return self.resp(start_resp, codes.METHOD_NOT_ALLOWED,
                             'Method Not Allowed: {}'.format(environ['REQUEST_METHOD']))

        if not environ['PATH_INFO'].startswith('/'):
            self.ensure_remote_addr(environ, start_resp, self.proxy_allow_addrs)
            self.ensure_authorization(environ, start_resp, self.proxy_credentials,
                                      'X-SUPERPROXY-AUTHORIZATION')
            return self.proxy(environ, start_resp)

        # Routing locally otherwise
        self.ensure_remote_addr(environ, start_resp, self.admin_allow_addrs)
        self.ensure_authorization(environ, start_resp, self.admin_credentials, 'AUTHORIZATION')

        if environ['PATH_INFO'] in ('/', '/superproxy'):
            return self.resp(start_resp, codes.FOUND, headers=[('Location', '/superproxy/')])

        handler = self.admin_route.get(environ['PATH_INFO'])
        if handler:
            return handler(environ, start_resp)

        return self.resp(start_resp, codes.NOT_FOUND,
                         'Not found: {}'.format(environ['PATH_INFO']))

    def ensure_remote_addr(self, environ, start_resp, addrs):
        if addrs and environ['REMOTE_ADDR'] not in addrs:
            return self.resp(start_resp, codes.FORBIDDEN,
               'Superproxy connection forbidden: Addr not in {!r}'.format(addrs),
                headers=[('X-Superproxy-Error', 'Superproxy connection forbidden')]
            )

    def ensure_authorization(self, environ, start_resp, credentials, header):
        if credentials:
            auth_header = environ.get('HTTP_' + header)
            if not auth_header:
                return self.unauthorized(environ, start_resp)
            auth = auth_header.split(' ')
            if auth[0] != 'Basic' or not auth[1]:
                return self.unauthorized(environ, start_resp)
            username, password = b64decode(auth[1]).decode('utf8').split(':', 1)
            if not credentials.get(username) == password:
                return self.unauthorized(environ, start_resp)

    def unauthorized(self, environ, start_resp):
        return self.resp(start_resp, codes.UNAUTHORIZED, 'Superproxy connection unauthorized',
            headers=[
                ('WWW-Authenticate', 'Basic realm=superproxy'),
                ('X-Superproxy-Error', 'Superproxy connection unauthorized'),
            ]
        )

    def frontend(self, environ, start_resp):
        auth = environ.get('HTTP_AUTHORIZATION', '')  # Hack to pass authorization for ajax
        return self.resp(start_resp, codes.OK, self.frontend_html, content_type='text/html',
            headers=[('Set-Cookie', 'Authorization="{}"; Max-Age: -1'.format(auth))],
        )

    def status(self, environ, start_resp):
        now = datetime.utcnow()
        active, rest, in_use = 0, 0, 0
        for p in self.proxylist.active_proxies.values():
            if p.rest_till and p.rest_till > now:
                rest += 1
            else:
                active += 1
            in_use += p.in_use

        checker = self.proxylist.checker
        fetcher = self.proxylist.fetcher
        resp = self.proxylist.json_encoder.dumps({
            'rest': rest,
            'active': active,
            'blacklist': len(self.proxylist.blacklist_proxies),
            'in_use': in_use,
            'waiting': len(self.proxylist.waiting),
            'need_update': self.proxylist.need_update,
            'updated_at': self.proxylist.updated_at,
            'checker': bool(checker),
            'checker_processing': len(checker._processing),
            'fetcher': bool(fetcher),
            'fetcher_started_at': fetcher and fetcher.started_at,
            'fetcher_ready': fetcher and fetcher.ready,
        })
        return self.resp(start_resp, codes.OK, resp, content_type='application/json')

    def mem_top(self, environ, start_resp):
        # memory-leak debug
        try:
            from mem_top import mem_top
        except ImportError as exc:
            return self.resp(start_resp, codes.NOT_FOUND, repr(exc))
        else:
            return self.resp(start_resp, codes.OK, str(mem_top()))

    def proxies(self, environ, start_resp):
        qs = dict(parse_qsl(environ.get('QUERY_STRING', '')))
        status = qs.get('status', '').split(',') or ('rest', 'active', 'blacklist')
        search = tuple(set(token for token in token_group.split('+') if token)
                       for token_group in qs.get('search', '').split() if token_group)
        sort, sort_desc = qs.get('sort'), False
        if sort and sort.startswith('-'):
            sort, sort_desc = sort[1:], True
        per_page = int(qs.get('per_page', 50))
        start = ((int(qs['page']) - 1) * per_page) if 'page' in qs else None

        proxies = []
        for p in _iter_proxies_by_status(self.proxylist, status):
            if (not search or any(all(_match_proxy_search_token(p, token)
                                      for token in token_group)
                                  for token_group in search)):
                proxies.append(p)

        if sort and sort == 'speed':
            proxies.sort(key=lambda p: p.speed or -1, reverse=sort_desc)
        elif sort and sort == 'used_at':
            proxies.sort(key=lambda p: p.used_at, reverse=sort_desc)

        resp = self.proxylist.json_encoder.dumps({
            'proxies': (proxies[start: start + per_page]
                        if start is not None else proxies),
            'total': len(proxies),
        })
        return self.resp(start_resp, codes.OK, resp, content_type='application/json')

    def waiting(self, environ, start_resp):
        resp = self.proxylist.json_encoder.dumps(self.proxylist.waiting)
        return self.resp(start_resp, codes.OK, resp, content_type='application/json')

    def history(self, environ, start_resp):
        qs = dict(parse_qsl(environ.get('QUERY_STRING', '')))
        result = [PROXY_RESULT_TYPE[result.upper()] for result
                  in qs.get('result', '').split(',') or ('success', 'fail', 'rest')]
        search = tuple(set(token for token in token_group.split('+') if token)
                       for token_group in qs.get('search', '').split() if token_group)
        per_page = int(qs.get('per_page', 50))
        start = ((int(qs['page']) - 1) * per_page) if 'page' in qs else None

        iterable = chain(self.proxylist.active_proxies.values(),
                         self.proxylist.blacklist_proxies.values())
        history = []
        for p in iterable:
            for h in (p.history or []):
                if (h[1] in result and  # result_type
                    (not search or any(all((_match_proxy_search_token(p, token) or
                                           (h[2] and token in h[2]) or  # reason
                                           (h[3] and token in h[3]))  # request_ident
                                           for token in token_group)
                                       for token_group in search))):
                    history.append(tuple(h) + (p.addr, p.country))

        history.sort(key=lambda h: h[0], reverse=True)
        resp = self.proxylist.json_encoder.dumps({
            'history': (history[start: start + per_page]
                        if start is not None else history),
            'total': len(history),
        })
        return self.resp(start_resp, codes.OK, resp, content_type='application/json')

    def action(self, environ, start_resp):
        data = environ['wsgi.input'].read(int(environ['CONTENT_LENGTH']))
        data = json.loads(data.decode('utf-8'))

        def error(msg):
            return self.resp(start_resp, codes.UNPROCESSABLE, msg)

        proxy_actions = {
            'blacklist': lambda p: self.proxylist.blacklist(p),
            'unblacklist': lambda p: self.proxylist.unblacklist(p),
            'reset_rest_till': lambda p: p.__setattr__('rest_till', None),
            # avoid "can't assign to lambda" with __setattr__ here
            'recheck': lambda p: self.proxylist.checker(p),
        }

        if data['action'] == 'fetch':
            if self.proxylist.fetcher and self.proxylist.fetcher.ready:
                self.proxylist.fetcher()
            else:
                return error('Fetcher not ready')

        elif data['action'] == 'forget_blacklist':
            if 'used_at_before' not in data:
                return error('Required params not found: {}'.format(data))
            used_at_before = (datetime.utcnow() -
                              timedelta(seconds=timeparse(data['used_at_before'])))
            for p in tuple(self.proxylist.blacklist_proxies.values()):
                if p.used_at and used_at_before > p.used_at:
                    del self.proxylist.blacklist_proxies[p.addr]

        elif data['action'] in proxy_actions:
            if data['action'] == 'recheck':
                if (not self.proxylist.checker):
                    return error('No checker configured')

            if 'addr' in data:
                proxy = self.proxylist.get_by_addr(data['addr'])
                if not proxy:
                    return error('Proxy not found: {}'.format(data['addr']))
                proxy_actions[data['action']](proxy)

            elif 'status' in data or 'used_at_before' in data or 'used_at_after' in data:
                status = data.get('status', '').split(',') or ('rest', 'active', 'blacklist')
                used_at_before = (data.get('used_at_before') and
                    datetime.utcnow() - timedelta(seconds=timeparse(data['used_at_before'])))
                used_at_after = (data.get('used_at_after') and
                    datetime.utcnow() - timedelta(seconds=timeparse(data['used_at_after'])))

                for p in tuple(_iter_proxies_by_status(self.proxylist, status)):
                    if used_at_before and p.used_at and used_at_before < p.used_at:
                        continue
                    if used_at_after and p.used_at and used_at_after > p.used_at:
                        continue
                    proxy_actions[data['action']](proxy)

            else:
                return error('Required params not found: {}'.format(data))

        else:
            return self.resp(start_resp, codes.UNPROCESSABLE, 'Unknown action')

        return self.resp(start_resp, codes.OK, '{"status": "ok"}',
                         content_type='application/json')

    def proxy(self, environ, start_resp):
        method = environ['REQUEST_METHOD']
        url = reconstruct_url(environ)

        headers = {
            key.title(): value
            for key, value in (
                # This is a hacky way of getting the header names right
                (key[5:].replace('_', '-'), value)
                for key, value in environ.items()
                # Keys that start with HTTP_ are all headers
                if key.startswith('HTTP_') and not key.startswith('HTTP_X_SUPERPROXY_')
            )
            if not is_hop_by_hop(key)
        }
        try:
            headers['Content-Type'] = environ['CONTENT_TYPE']
        except KeyError:
            pass

        try:
            data = environ['wsgi.input'].read(int(environ['CONTENT_LENGTH']))
        except (KeyError, ValueError):
            data = None

        kwargs = {
            key: SUPERPROXY_REQUEST_HEADERS[key][0](value)
            for key, value in (
                (key[18:].lower(), value)
                for key, value in environ.items()
                if key.startswith('HTTP_X_SUPERPROXY_')
            )
            if key in SUPERPROXY_REQUEST_HEADERS
        }

        try:
            resp = self.session.request(method, url, data=data, headers=headers, **kwargs)
        except BaseException as exc:
            if isinstance(exc, (ProxyListError, KeyboardInterrupt)):
                logger.error('%r', exc)
            else:
                logger.exception('%r', exc)
            return self.resp(start_resp, codes.INTERNAL_SERVER_ERROR, repr(exc),
                             headers=[('X-Superproxy-Error', exc.__class__.__name__)])

        headers = []
        # http://docs.python-requests.org/en/master/user/quickstart/#response-headers
        for k in resp.headers:
            if not is_hop_by_hop(k) and k.lower() not in ('content-length',):
                for v in resp.raw.headers.getheaders(k):
                    # Requests merge same header (set-cookie for example), see link above
                    headers.append((k, v))

        headers += [
            ('X-Superproxy-Addr', resp._proxy and resp._proxy.addr or ''),
        ]

        return self.resp(start_resp, '{0.status_code} {0.reason}'.format(resp),
                         resp.content, headers=headers)

    def resp(self, start_resp, status, content='', headers=[], content_type=None):
        if isinstance(status, int):
            status = '{} {}'.format(status, STATUS_CODE_TITLES[status])
        if not isinstance(content, bytes):
            content = str(content).encode('utf-8')
        if content_type:
            headers = list(headers) + [('Content-Type', content_type)]

        start_resp(status, headers + [('Content-Length', str(len(content)))])
        return [content]
