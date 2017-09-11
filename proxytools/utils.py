import logging
import json
import enum
import collections
from datetime import datetime, date, time
from importlib import import_module


def create_country_name_to_alpha2():
    from pycountry import countries
    from pycountry_convert.country_name_to_country_alpha2 \
        import COUNTRY_NAME_TO_COUNTRY_ALPHA2

    rv = {k.upper(): v for k, v in COUNTRY_NAME_TO_COUNTRY_ALPHA2.items()}
    rv.update({
        'KOREA': 'KR',
        'PALESTINIAN TERRITORY': 'PS',
        'COTE D\'IVOIRE': 'CI',
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


def dict_merge(d, u, copy=False):
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


def gevent_monkey_patch():
    from gevent import monkey
    monkey.patch_all()

    # https://github.com/gevent/gevent/issues/937
    # for error AttributeError: 'super' object has no attribute 'getpeername'
    from socks import socksocket
    socksocket.get_proxy_peername = lambda self: self.getpeername()


def to_isoformat(dt):
    assert not dt.tzinfo  # assuming we operate naive datetimes in utc
    return dt.isoformat(timespec='seconds') + 'Z'


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
            return (obj.isoformat(timespec='seconds') +
                    (obj.tzinfo and '' or 'Z'))
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
