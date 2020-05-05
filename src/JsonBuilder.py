import pandas as pd
from asteval import Interpreter

aeval = Interpreter()

class JsonBuilder:
    def __init__(self, **kwargs):
        self.type = kwargs.get('type')
        self.value_col = kwargs.get('value_col')
        self.name_col = kwargs.get('name_col')
        self.value = kwargs.get('value')
        self.name = kwargs.get('name')
        self.multiple = kwargs.get('multiple')

        self.filter = kwargs.get('filter')
        self.split = kwargs.get('split')
        self.transmute = kwargs.get('transmute')

        self.transexpr = aeval.parse(self.transmute) if self.transmute else None

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
            col = self.df.eval(self.split)
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
        if self.value_col:
            if self.row is not None:
                self.value = getattr(self.row, self.value_col)
            else:
                self.value = None
    
    def _fetch_name(self):
        if self.name_col:
            if self.row is not None:
                self.name = getattr(self.row, self.name_col)
            else: 
                self.name = None

    def _apply_filter(self):
        if self.filter:
            self.df = self.df.query(self.filter)
            if len(self.df.index)>0:
                self.row = next(self.df.itertuples())
            else:
                self.row = None

    def _transmute(self):
        if self.transmute:
            aeval.symtable['x'] = self.value
            aeval.symtable['r'] = self.row
            aeval.symtable['df'] = self.df
            self.value = aeval.run(self.transexpr)

    def load_csv(self, csv):
        self.df = pd.read_csv(csv)
        self.df.index += 1
        return self

    def add_functions(self, functions):
        for f in functions:
            aeval(f)
        return self
    
    def apply_transforms(self, df_transforms):
        for transform in df_transforms:
            self.df.eval(transform, inplace=True)
        return self

def parse_mapping(mapping):
    children = mapping.pop('children', [])
    this = JsonBuilder(**mapping)
    for c in children:
        this.children.append(parse_mapping(c))
    return this