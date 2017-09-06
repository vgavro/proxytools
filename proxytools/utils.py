import logging
import json
import http.client
from datetime import datetime, date, time

import coloredlogs


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


COUNTRY_NAME_TO_ALPHA2 = create_country_name_to_alpha2()


def country_name_to_alpha2(name, raise_error=True):
    name = name.upper()
    try:
        return COUNTRY_NAME_TO_ALPHA2[name]
    except KeyError:
        if raise_error:
            raise


class EntityLoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger, entity):
        self.logger = logger
        self.entity = entity or '?'

    def process(self, msg, kwargs):
        return '{}: {}'.format(self.entity, msg), kwargs


def configure_logging(settings):
    if settings.http_debug:
        http.client.HTTPConnection.debuglevel = 1
    else:
        http.client.HTTPConnection.debuglevel = 0

    field_styles = coloredlogs.DEFAULT_FIELD_STYLES.copy()
    field_styles.update(settings.field_styles)
    level_styles = coloredlogs.DEFAULT_LEVEL_STYLES.copy()
    level_styles.update(settings.level_styles)
    coloredlogs.install(
        field_styles=field_styles, level_styles=level_styles,
        **{k: v for k, v in settings.items() if k in ['level', 'fmt', 'datefmt']}
    )
    for name, level in settings.levels.items():
        logger = logging.getLogger(name)
        logger.setLevel(level)


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
    return datetime.strptime('%Y-%m-%dT%H:%M:%SZ')


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date, time)):
            return obj.isoformat()
        if hasattr(self, 'to_json'):
            return obj.to_json()
        raise TypeError('Type %s not serializable' % type(obj))
