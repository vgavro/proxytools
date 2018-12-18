import logging
import os
import io
import json
import enum
import random
import collections
from datetime import datetime, date, time
import time as time_
from urllib.parse import quote, unquote
from importlib import import_module
from shutil import which


from gevent.subprocess import run, PIPE


def repr_response(resp, full=False):
    """
    Helper function requests.Response representation.
    """
    if not full and len(resp.content) > 128:
        content = '{}...{}b'.format(resp.content[:128], len(resp.content))
    else:
        content = resp.content
    if 300 <= resp.status_code < 400:
        content = resp.headers.get('Location')
    return '{} {} {}: {}'.format(resp.request.method, resp.status_code,
                                 resp.url, content)


class ResponseMatch:
    """
    Helper class to be used instead callback to match requests.Response object for
    proxy_success_response and proxy_rest_response params,
    also should be used with SuperProxySession.
    """
    def __init__(self, status=[], status_not=[], text=[], text_not=[],
                 header=[], header_not=[]):
        status, status_not = [int(x) for x in status], [int(x) for x in status_not]
        (self.status, self.status_not, self.text, self.text_not,
         self.header, self.header_not) = \
            (status, status_not, text, text_not, header, header_not)

    def __call__(self, resp):
        if self.status and resp.status_code not in self.status:
            return False
        if resp.status_code in self.status_not:
            return False
        if self.text and not any(x in resp.text for x in self.text):
            return False
        if any(x in resp.text for x in self.text_not):
            return False
        for header, *header_text in self.header:
            if header not in resp.headers:
                return False
            elif header_text and not header_text[0] in resp.headers[header]:
                return False
        for header, *header_text in self.header_not:
            if header in resp.headers:
                if not header_text or header_text[0] in resp.headers[header]:
                    return False
        return True

    def _to_superproxy_header(self):
        return quote(json.dumps({k: v for k, v in self.__dict__.items() if v}))

    @classmethod
    def _from_superproxy_header(cls, data):
        return cls(**json.loads(unquote(data)))


def create_country_name_to_alpha2():
    # TODO: remove this and use native convert as this PR will be merged:
    # https://github.com/TuneOSS/pycountry-convert/pull/43
    from pycountry import countries
    from pycountry_convert \
        import WIKIPEDIA_COUNTRY_NAME_TO_COUNTRY_ALPHA2

    rv = {k.upper(): v for k, v in WIKIPEDIA_COUNTRY_NAME_TO_COUNTRY_ALPHA2.items()}
    rv.update({
        'KOREA': 'KR',
        'REPUBLIC OF KOREA': 'KR',
        'PALESTINIAN TERRITORY': 'PS',
        'COTE D\'IVOIRE': 'CI',
        'MYANMAR [BURMA]': 'MM',
        'UNKNOWN': None,
    })
    for country in countries:
        for attr in ('name', 'common_name', 'official_name'):
            name = getattr(country, attr, None)
            if name:
                rv[name.upper()] = country.alpha_2
    return rv


_COUNTRY_NAME_TO_ALPHA2 = {}


def country_name_to_alpha2(name, raise_error=True):
    if not _COUNTRY_NAME_TO_ALPHA2:
        # lazy population
        _COUNTRY_NAME_TO_ALPHA2.update(create_country_name_to_alpha2())
    try:
        return _COUNTRY_NAME_TO_ALPHA2[name.upper()]
    except KeyError:
        if raise_error:
            raise


class EntityLoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger, entity):
        self.logger = logger
        self.entity = entity or '?'

    def process(self, msg, kwargs):
        return '{}: {}'.format(self.entity, msg), kwargs


def dict_merge(d, u):
    # https://stackoverflow.com/a/3233356/450103
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            d[k] = dict_merge(d.get(k, {}), v)
        else:
            d[k] = u[k]
    return d


class classproperty(property):
    """
    A decorator that behaves like @property except that operates
    on classes rather than instances.
    Copy of sqlalchemy.util.langhelpers.classproperty, because first one executed
    on class declaration.
    """

    def __init__(self, fget, *arg, **kw):
        super(classproperty, self).__init__(fget, *arg, **kw)
        self.__doc__ = fget.__doc__

    def __get__(desc, self, cls):
        return desc.fget(cls)


# def gevent_monkey_patch():
#     from gevent import monkey
#     monkey.patch_all()
#
#     # https://github.com/gevent/gevent/issues/937
#     # for error AttributeError: 'super' object has no attribute 'getpeername'
#     # NOTE: already fixed in pysocks master
#     from socks import socksocket
#     socksocket.get_proxy_peername = lambda self: self.getpeername()


def to_isoformat(dt):
    if dt.tzinfo:
        return dt.isoformat()
    # assuming we operate naive datetimes in utc
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def from_isoformat(dt):
    # TODO: Try to use dateutil.parser.parse for times generated
    # not from our code, as optional depency
    return datetime.strptime(dt, '%Y-%m-%dT%H:%M:%SZ')


class JSONEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('indent', 2)
        kwargs.setdefault('separators', (',', ': '))
        super().__init__(*args, **kwargs)

    def default(self, obj):
        if isinstance(obj, datetime):
            # assuming naive datetimes in UTC
            return obj.strftime('%Y-%m-%dT%H:%M:%SZ')
        if isinstance(obj, (date, time)):
            return obj.isoformat()
        if isinstance(obj, enum.Enum):
            return obj.name
        if hasattr(obj, 'to_json'):
            return obj.to_json()
        # if isinstance(obj, set):
        #     return tuple(obj)
        if hasattr(obj, '__iter__'):
            return tuple(obj)
        super().default(obj)

    def dump(self, obj, fp):
        iterator = self.iterencode(obj)
        if isinstance(fp, str):
            with open(fp, 'w') as fp:
                for chunk in iterator:
                    fp.write(chunk)
        else:
            for chunk in iterator:
                fp.write(chunk)

    def dumps(self, obj):
        buf = io.StringIO()
        try:
            self.dump(obj, buf)
            return buf.getvalue()
        finally:
            buf.close()


def str_to_enum(value, enum_cls):
    return isinstance(value, enum.Enum) and value or enum_cls[value.upper()]


class CompositeContains:
    def __init__(self, *objects):
        self.objects = objects

    def __contains__(self, value):
        for obj in self.objects:
            if value in obj:
                return True
        return False


def import_string(import_name):
    *module_parts, attr = import_name.replace(':', '.').split('.')
    if not module_parts:
        raise ImportError('You must specify module and object, separated by ":" or ".", '
                          'got "{}" instead'.format(import_name))
    module = import_module('.'.join(module_parts))
    return getattr(module, attr)


def get_subclasses_from_module(module, cls):
    if isinstance(module, str):
        module = import_module(module)

    rv = []
    for attr in dir(module):
        if attr.startswith('_'):
            continue
        obj = getattr(module, attr)
        try:
            if issubclass(obj, cls) and obj is not cls:
                rv.append(obj)
        except TypeError:
            # issubclass() arg 1 must be a class skip
            pass
    return rv


def get_response_speed(resp, start_at):
    # Returns total kb read in 1 second
    # NOTE: this also depends on connection time for first request to proxy
    # TODO: resp.elapsed is checking headers read besides content read,
    # maybe it would be more clear to substract it,
    # as total speed depends also on connection time
    # Note that content may be be read lazy on resp.content attribute, don't remove
    kb = int(resp.headers.get('Content-Length', len(resp.content))) / 1024
    return round(kb / (time_.time() - start_at), 2)


# from https://techblog.willshouse.com/2012/01/03/most-common-user-agents/
COMMON_USER_AGENTS = [
    ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
     '(KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'),
    ('Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 '
     '(KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'),
    ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 '
     '(KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'),
    ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 '
     '(KHTML, like Gecko) Version/10.1.2 Safari/603.3.8'),
]


def get_random_user_agent(filter_=None):
    # https://techblog.willshouse.com/2012/01/03/most-common-user-agents/
    # Updated December 14th 2018
    if not hasattr(get_random_user_agent, '_user_agents'):
        with open(os.path.dirname(__file__) + '/user_agents.txt') as fh:
            get_random_user_agent._user_agents = [
                x.strip() for x in fh.readlines()
                if x.strip() and not x.strip().startswith('#')
            ]
    if filter_:
        return random.choice([ua for ua in get_random_user_agent._user_agents if filter_(ua)])
    return random.choice(get_random_user_agent._user_agents)


def gocr_response(resp, pattern, convert=which('convert'), gocr=which('gocr')):
    cmd = run('{} - pbm:- | {} -C "{}" -'.format(convert, gocr, pattern),
              stdout=PIPE, input=resp.content, shell=True, check=True)
    return cmd.stdout.strip().decode()
