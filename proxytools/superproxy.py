import logging
from datetime import datetime, timedelta
from urllib.parse import parse_qsl
from itertools import chain
import json

from requests.status_codes import _codes, codes
from pytimeparse.timeparse import timeparse

from .models import PROXY_RESULT_TYPE
from .utils import ResponseMatch


logger = logging.getLogger(__name__)

ALLOWED_METHODS = ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'OPTIONS']

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
    def __init__(self, proxylist, **session_kwargs):
        from .requests import ProxyListSession
        self.session = ProxyListSession(proxylist, forgetful_cookies=True,
                                        enforce_content_length=False,
                                        **session_kwargs)
        self.proxylist = proxylist

    def __call__(self, environ, start_resp):
        if environ['REQUEST_METHOD'] not in ALLOWED_METHODS:
            return self.resp(start_resp, codes.METHOD_NOT_ALLOWED,
                             'Method Not Allowed: {}'.format(environ['REQUEST_METHOD']))

        if environ['PATH_INFO'].startswith('/'):
            return self.resolve_local(environ, start_resp)
        else:
            return self.resolve_proxy(environ, start_resp)

    def resolve_local(self, environ, start_resp):
        if environ['PATH_INFO'] in ('/', '/superproxy'):
            return self.resp(start_resp, codes.FOUND, headers=[('Location', '/superproxy/')])

        elif environ['PATH_INFO'].startswith('/superproxy/'):
            # TODO: dirty hack, and only for debug, cache it later and replace only extension
            resp = open(__file__.replace('.py', '.html')).read()
            return self.resp(start_resp, codes.OK, resp, content_type='text/html')

        elif environ['PATH_INFO'] == '/status':
            now = datetime.utcnow()
            active, rest, in_use = 0, 0, 0
            for p in self.proxylist.active_proxies.values():
                if p.rest_till and p.rest_till > now:
                    rest += 1
                else:
                    active += 1
                in_use += p.in_use

            fetcher = self.proxylist.fetcher
            resp = self.proxylist.json_encoder.dumps({
                'rest': rest,
                'active': active,
                'blacklist': len(self.proxylist.blacklist_proxies),
                'in_use': in_use,
                'waiting': len(self.proxylist.waiting),
                'need_update': self.proxylist.need_update,
                'fetcher': bool(fetcher),
                'fetcher_checker': bool(fetcher and fetcher.checker),
                'fetcher_started_at': fetcher and fetcher.started_at,
                'fetcher_ready': fetcher and fetcher.ready,
            })
            return self.resp(start_resp, codes.OK, resp, content_type='application/json')

        elif environ['PATH_INFO'] == '/proxies':
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

        elif environ['PATH_INFO'] == '/waiting':
            resp = self.proxylist.json_encoder.dumps(self.proxylist.waiting)
            return self.resp(start_resp, codes.OK, resp, content_type='application/json')

        elif environ['PATH_INFO'] == '/history':
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
                        history.append(tuple(h) + (p.addr,))

            history.sort(key=lambda h: h[0], reverse=True)
            resp = self.proxylist.json_encoder.dumps({
                'history': (history[start: start + per_page]
                            if start is not None else history),
                'total': len(history),
            })
            return self.resp(start_resp, codes.OK, resp, content_type='application/json')

        elif environ['PATH_INFO'] == '/action':
            data = environ['wsgi.input'].read(int(environ['CONTENT_LENGTH']))
            data = json.loads(data.decode('utf-8'))

            def error(msg):
                return self.resp(start_resp, codes.UNPROCESSABLE, msg)

            proxy_actions = {
                'blacklist': lambda p: self.proxylist.blacklist(p),
                'unblacklist': lambda p: self.proxylist.unblacklist(p),
                'reset_rest_till': lambda p: p.__setattr__('rest_till', None),
                # avoid "can't assign to lambda" with __setattr__ here
                'recheck': lambda p: self.proxylist.fetcher.checker(p),
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
                    if (not self.proxylist.fetcher or not self.proxylist.fetcher.checker):
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

        else:
            return self.resp(start_resp, codes.NOT_FOUND,
                             'Not found: {}'.format(environ['PATH_INFO']))

    def resolve_proxy(self, environ, start_resp):
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
            # TODO: debugging incomplete responses from instagram
            logger.debug('Ident: %s url: %s, length_remaining(%s), fp_bytes_read(%s), tell(%s) '
                         'content_length(%s)', kwargs.get('proxy_request_ident'), url,
                         resp.raw.length_remaining, resp.raw._fp_bytes_read,
                         resp.raw.tell(), resp.headers.get('Content-Length'))
        except BaseException as exc:
            logger.error('%r', exc)
            # logger.exception('%r', exc)
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
