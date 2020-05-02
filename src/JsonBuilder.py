import pandas as pd

class JsonBuilder:
    def __init__(self, **kwargs):
        self.type = kwargs.get('type')
        self.value_col = kwargs.get('value_col')
        self.name_col = kwargs.get('name_col')
        self.value = kwargs.get('value')
        self.name = kwargs.get('name')
        self.multiple = kwargs.get('multiple')

        self.filter_raw = kwargs.get('filter')
        self.split_raw = kwargs.get('split')
        self.transmute_raw = kwargs.get('transmute')

        self.filter = eval("lambda df:"+self.filter_raw) if self.filter_raw else None
        self.split = eval("lambda df:"+self.split_raw) if self.split_raw else None
        self.transmute = eval("lambda x,r,df:"+self.transmute_raw) if self.transmute_raw else None

        self.df = None
        self.row = None

        self.children = []

    def build(self):
        if self.type == 'object':
            self._build_object()
        elif self.type == 'array':
            self._build_array()
        else:
            self._fetch_value()
        self._fetch_name()
        self._transmute()
        return self

    def _build_object(self):
        self.value = {}
        for child in self.children:
            child.df, child.row = self.df, self.row
            for _ in child._next_scope():
                c = child.build()
                self.value[c.name] = c.value

    def _build_array(self):
        self.value = []
        for child in self.children:
            child.df, child.row = self.df, self.row
            for _ in child._next_scope():
                c = child.build()
                self.value.append(c.value)

    def _next_scope(self):
        self._apply_filter()
        if self.split:
            col = self.split(self.df)
            if col.is_unique:
                for row in self.df.itertuples():
                    self.row = row
                    yield
            else:
                for group in self.df.groupby(col, sort=False):
                    self.df = group[1]
                    self.row = next(group[1].itertuples())
                    yield
        else:
            yield
    
    def _fetch_value(self):
        if self.value_col and self.row is not None:
            self.value = getattr(self.row, self.value_col)
    
    def _fetch_name(self):
        if self.name is None and self.name_col is None:
            self.name = self.value_col
        elif self.name_col and self.row is not None:
            self.name = getattr(self.row, self.name_col)

    def _apply_filter(self):
        if self.filter and self.df is not None:
            self.df = self.df[self.filter(self.df)]
            if len(self.df.index)>0:
                self.row = next(self.df.itertuples())
            else:
                self.row = None

    def _transmute(self):
        if self.transmute:
            self.value = self.transmute(self.value, self.row, self.df)

    def load_csv(self, csv):
        self.df = pd.read_csv(csv)
        self.df.index += 1
        self._apply_filter()
        if len(self.df.index)>0:
            self.row = next(self.df.itertuples())
        return self

    def add_functions(self, functions):
        for f in functions:
            exec(f)
            f_name = f.split('(')[0].split(' ')[1]
            globals().update({f_name:eval(f_name)})
        return self
    
    def apply_transforms(self, df_transforms):
        for transform in df_transforms:
            f = eval("lambda df:"+transform)
            out = f(self.df)
            if isinstance(out, pd.core.series.Series):
                name = out.name
                self.df[name] = out
            elif isinstance(out, pd.core.frame.DataFrame):
                self.df = out
        return self

def parse_mapping(mapping):
    children = mapping.pop('children', [])
    this = JsonBuilder(**mapping)
    for c in children:
        this.children.append(parse_mapping(c))
    return this