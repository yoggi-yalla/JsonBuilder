import pandas as pd
from asteval import Interpreter

aeval = Interpreter()

class JsonBuilder:
    def __init__(self, **kwargs):
        self.type = kwargs.get('type')
        self.name = kwargs.get('name')
        self.value = kwargs.get('value')
        self.column = kwargs.get('column')

        self.filter = kwargs.get('filter')
        self.iterate = kwargs.get('iterate')
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
            self._build_primitive()
        self._transmute()
        return self

    def _build_object(self):
        self.value = {}
        for child in self.children:
            child.df, child.row = self.df, self.row
            for _ in child._iterate():
                c = child.build()
                self.value[c.name] = c.value

    def _build_array(self):
        self.value = []
        for child in self.children:
            child.df, child.row = self.df, self.row
            for _ in child._iterate():
                c = child.build()
                self.value.append(c.value)
    
    def _build_primitive(self):
        if self.column:
            self.value = getattr(self.row, self.column) if self.row else None

    def _iterate(self):
        self._filter()
        if self.iterate:
            col = self.df.eval(self.iterate)
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

    def _filter(self):
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
        print(self.df)
        for transform in df_transforms:
            aeval.symtable['df'] = self.df
            out = aeval(transform)
            if isinstance(out, pd.core.frame.DataFrame):
                self.df = out
            elif isinstance(out, pd.core.series.Series):
                self.df[out.name] = out
            print(self.df)
        return self

def parse_mapping(mapping):
    children = mapping.pop('children', [])
    this = JsonBuilder(**mapping)
    for c in children:
        this.children.append(parse_mapping(c))
    return this    