import pandas as pd
from asteval import Interpreter

aeval = Interpreter()

class JsonNode:
    def __init__(self, **kwargs):
        self.name = kwargs.get('name')
        self.value = kwargs.get('value')
        self.column = kwargs.get('column')

        self.filter = kwargs.get('filter')
        self.group_by = kwargs.get('group_by')
        self.iterate = kwargs.get('iterate')
        self.transmute = kwargs.get('transmute')

        self.transexpr = aeval.parse(self.transmute) if self.transmute else None

        self.df = None
        self.row = None

        self.children = []

    def build(self):
        self._filter()
        self._build()
        self._transmute()
        self._validate()
        return self

    def _build(self):
        #This is implemented in the subclasses JsonArray, JsonObject, JsonPrimitive
        pass

    def _validate(self):
        """
        This is currently not used, but could be useful if one wants to implement 
        different validators for different Node types
        """
        pass

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
     
    def _iterate(self):
        if self.group_by:
            col = self.df.eval(self.group_by)
            for group in self.df.groupby(col, sort=False):
                self.df = group[1]
                self.row = next(self.df.itertuples())
                yield   
        elif self.iterate:
            iterator = self.df.itertuples()
            self.df = None
            for row in iterator:
                self.row = row
                yield   
        else:
            yield

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


class JsonArray(JsonNode):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _build(self):
        self.value = []    
        for child in self.children:
            child.df, child.row = self.df, self.row
            for _ in child._iterate():
                c = child.build()
                self.value.append(c.value)

class JsonObject(JsonNode):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def _build(self):
        self.value = {}
        for child in self.children:
            child.df, child.row = self.df, self.row
            for _ in child._iterate():
                c = child.build()
                self.value[c.name] = c.value

class JsonPrimitive(JsonNode):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def _build(self):
        if self.column:
            self.value = getattr(self.row, self.column) if self.row else None


def parse_mapping(mapping):
    children = mapping.pop('children', [])
    if mapping.get("type") == "object":
        this = JsonObject(**mapping)
    elif mapping.get("type") == "array":
        this = JsonArray(**mapping)
    else:
        this = JsonPrimitive(**mapping)
    for c in children:
        this.children.append(parse_mapping(c))
    return this    