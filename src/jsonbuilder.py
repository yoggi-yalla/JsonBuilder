from asteval import Interpreter
import pandas
import json

class Tree:
    def __init__(self, fmt, table):
        mapping = fmt.get("mapping",{})
        functions = fmt.get("functions",[])
        df_transforms = fmt.get("df_transforms",[])
        
        self.aeval = Interpreter()
        self.df = pandas.read_csv(table)
        self.root = Tree.parse_mapping(self, mapping)
        self.intermediate_dfs = []

        for func in functions:
            self.aeval(func)
        
        for transform in df_transforms:
            self.save_intermediate_df()
            self.apply_transform(transform)
        self.save_intermediate_df()

        
    @staticmethod
    def parse_mapping(tree, mapping):
        children = mapping.pop('children', [])
        if mapping.get("type") == "object":
            this = JsonObject(tree, **mapping)
        elif mapping.get("type") == "array":
            this = JsonArray(tree, **mapping)
        else:
            this = JsonPrimitive(tree, **mapping)
        for c in children:
            this.children.append(Tree.parse_mapping(tree, c))
        return this
    
    def save_intermediate_df(self):
        if len(self.df.index) > 100:
            head,tail = self.df.head(50).copy(), self.df.tail(50).copy()
            intermediate_df = pandas.concat([head,tail])
        else:
            intermediate_df = self.df.copy()
        self.intermediate_dfs.append(intermediate_df)

    def apply_transform(self, transform):
        self.aeval.symtable['df'] = self.df
        out = self.aeval(transform)
        if isinstance(out, pandas.core.frame.DataFrame):
            self.df = out
        elif isinstance(out, pandas.core.series.Series):
            self.df[out.name] = out
        else:
            raise Exception("Invalid return type from df_transform: {}, return type: {}.".format(transform, type(out)))

    def build(self):
        self.root.df = self.df
        self.root.build()
        return self
    
    def toJson(self, indent):
        return json.dumps(self.root.value, indent=indent)

class Node:
    def __init__(self, tree, **kwargs):
        self.tree = tree
        self.name = kwargs.get('name')
        self.value = kwargs.get('value')
        self.column = kwargs.get('column')

        self.filter = kwargs.get('filter')
        self.group_by = kwargs.get('group_by')
        self.iterate = kwargs.get('iterate')
        self.transmute = kwargs.get('transmute')

        self.transexpr = self.tree.aeval.parse(self.transmute) if self.transmute else None

        self.df = None
        self.row = None

        self.children = []

    def build(self):
        self._filter()
        self._build()
        return self

    def _build(self):
        # This is implemented in the subclasses JsonArray, JsonObject, JsonPrimitive
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
            self.tree.aeval.symtable['x'] = self.value
            self.tree.aeval.symtable['r'] = self.row
            self.tree.aeval.symtable['df'] = self.df
            self.value = self.tree.aeval.run(self.transexpr)
     
    def _iterate(self):
        if self.group_by:
            for group in self.df.groupby(self.group_by, sort=False):
                self.df = group[1]
                self.row = next(self.df.itertuples())
                yield   
        elif self.iterate:
            rows = self.df.itertuples()
            self.df = None
            for row in rows:
                self.row = row
                yield
        else:
            yield

class JsonArray(Node):
    def __init__(self, tree, **kwargs):
        super().__init__(tree, **kwargs)

    def _build(self):
        self.value = []    
        for child in self.children:
            child.df, child.row = self.df, self.row
            child._filter()
            for _ in child._iterate():
                c = child._build()
                self.value.append(c.value)
        self._transmute()
        return self

class JsonObject(Node):
    def __init__(self, tree, **kwargs):
        super().__init__(tree, **kwargs)
    
    def _build(self):
        self.value = {}
        for child in self.children:
            child.df, child.row = self.df, self.row
            child._filter()
            for _ in child._iterate():
                c = child._build()
                self.value[c.name] = c.value
        self._transmute()
        return self

class JsonPrimitive(Node):
    def __init__(self, tree, **kwargs):
        super().__init__(tree, **kwargs)
    
    def _build(self):
        if self.column and self.row:
            self.value = getattr(self.row, self.column)
        self._transmute()
        return self
