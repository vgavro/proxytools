import logging
import json
import http.client
from datetime import datetime, date, time

import pycountry
import coloredlogs


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


_country_name_fixes = {
    'Macedonia': 'Macedonia, Republic of',
    'Iran': 'Iran, Islamic Republic of',
    'Korea': 'Korea, Republic of',
    'Cote D\'ivoire': 'Côte d\'Ivoire',
    'Timor-leste': 'Timor-Leste',
    'Palestinian Territory': 'Palestine, State of',  # or Israel? :-)
}


def get_country_alpha_2_by_name(name, raise_on_not_found=True):
    if name.isupper():
        name = ' '.join(w.lower() if w in ['AND'] else w.capitalize()
                        for w in name.split())
    name = _country_name_fixes.get(name, name)
    try:
        return pycountry.countries.get(name=name).alpha_2
    except KeyError:
        try:
            return pycountry.countries.get(common_name=name).alpha_2
        except KeyError:
            try:
                return pycountry.countries.get(official_name=name).alpha_2
            except KeyError:
                if raise_on_not_found:
                    raise


def gevent_monkey_patch():
    from gevent import monkey
    monkey.patch_all()

    # https://github.com/gevent/gevent/issues/937
    # for error AttributeError: 'super' object has no attribute 'getpeername'
    from socks import socksocket
    socksocket.get_proxy_peername = lambda self: self.getpeername()


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date, time)):
            return obj.isoformat()
        if hasattr(self, 'to_json'):
            return obj.to_json()
        raise TypeError('Type %s not serializable' % type(obj))
