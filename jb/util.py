std_funcs = [
"""
def translate(obj, t_dict):
    if isinstance(obj, pandas.Series):
        def _translate(value):
            return t_dict.get(value, value)
        return obj.map(_translate)
    else:
        return t_dict.get(value, value)
""",
"""
def date(obj, **kwargs):
    return pandas.to_datetime(obj, **kwargs)
""",
"""
def delta(obj):
    if isinstance(obj, (pandas.Timedelta, relativedelta)):
        return obj
    elif isinstance(obj, str):
        units = dict(d=0,w=0,m=0,y=0)
        nodes = re.findall(r'[A-Za-z]+|[-+]?[0-9]*\\.?[0-9]+', obj)
        try:
            while nodes:
                value = int(float(nodes.pop(0)))
                unit = nodes.pop(0).lower()
                units[unit] += value
            return relativedelta(days=units['d'], weeks=units['w'], months=units['m'], years=units['y'])
        except Exception:
            raise Exception('Invalid delta-string: {}'.format(obj))
    elif pandas.isna(obj):
        return relativedelta()
    else:
        raise Exception('Invalid delta-object: {}'.format(obj))
""",
]