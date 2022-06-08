import re
from typing import Tuple, Dict

from assemblyline.odm.models.config import VacuumSafelistItem

_safelist = [
    {
        'name': 'ibiblio.org',
        'conditions': {
            'url': r'^mirrors\.ibiblio\.org/'
        }
    },
    {
        'name': 'Symantec Updates',
        'conditions': {
            'url': r'[^/]*\.?liveupdate\.symantecliveupdate\.com(?:\:[0-9]{2,5})?/'
        }
    },
    {
        'name': 'Google Earth',
        'conditions': {
            'url': r'[^/]*\.?google\.com/mw-earth-vectordb/'
        }
    },
    # Examples
    # download.windowsupdate.com/c/msdownload/update/software/defu/2015/01/am_delta_74634f1206094529d2f336f24da8429a7d4ebec0.exe
    # download.windowsupdate.com/c/msdownload/update/software/defu/2015/01/am_delta_74634f1206094529d2f336f24da8429a7d4ebec0.exe
    # au.v4.download.windowsupdate.com/d/msdownload/update/software/defu/2015/01/am_delta_b0023f90835cd814b953c331f93776f3108936b9.exe
    {
        'name': 'Microsoft Windows Updates',
        'conditions': {
            'url': r'[^/]*\.windowsupdate\.com/'
        }
    },
    {
        'name': 'Microsoft Windows Updates',
        'conditions': {
            'domain': r'[^/]*\.windowsupdate\.com'
        }
    },
    {
        'name': 'Microsoft Package Distribution',
        'conditions': {
            'domain': r'[^/]*\.?delivery\.mp\.microsoft\.com'
        }
    },
]

_operators = {
    'in': lambda args: lambda x: x in args,
    'not in': lambda args: lambda x: x not in args,
    'regexp': lambda args: re.compile(*args).match,
}


def _transform(condition):
    if isinstance(condition, str):
        args = [condition]
        func = 'regexp'
    else:
        args = list(condition[1:])
        func = condition[0]

    return _operators[func](args)


def _matches(data, sigs):
    cache = {}
    unknown = 0
    for sig in sigs:
        result = _match(cache, data, sig)
        if result:
            name = sig.get('name', None)
            if not name:
                unknown += 1
                name = "unknown%d" % unknown
            yield name, result
    return


def _match(cache, data, sig):
    summary = {}
    results = [
        _call(cache, data, f, k) for k, f in sig['conditions'].items()
    ]
    if all(results):
        [summary.update(r) for r in results]  # pylint: disable=W0106
    return summary


# noinspection PyBroadException
def _call(cache, data, func, key):
    try:
        value = cache.get(key, None)
        if not value:
            cache[key] = value = data.get(key)
        if not callable(func):
            func = _transform(func)
        return {key: value} if func(value) else {}
    except Exception:
        return {}


class VacuumSafelist:
    def __init__(self, data: list[VacuumSafelistItem]):
        self._safelist = _safelist
        self._safelist.extend([
            row.as_primitives() if isinstance(row, VacuumSafelistItem) else row
            for row in data
        ])
        VacuumSafelist.optimize(self._safelist)

    def drop(self, data: Dict) -> Tuple[str, Dict]:
        return next(_matches(data, self._safelist), ("", {}))

    @staticmethod
    def optimize(signatures):
        for sig in signatures:
            conditions = sig.get('conditions')
            for k, v in conditions.items():
                conditions[k] = _transform(v)

