from asteval import Interpreter
import pandas
import json

class Tree:
    def __init__(self, fmt, table):
        mapping = fmt.get("mapping",{})
        functions = fmt.get("functions",[])
        df_transforms = fmt.get("df_transforms",[])
        separator = fmt.get("separator")
        sheet_name = fmt.get("sheet_name", 0)
        inspect_row = fmt.get("inspect_row")
        
        self.eval = Interpreter()
        self.df = Tree.load_table(table, separator, sheet_name)
        self.root = Tree.parse_mapping(self, mapping)
        
        self.intermediate_dfs = []

        for func in functions:
            try:
                self.eval(func)
            except:
                raise Exception(f"\n\nFailed to load function:\n{func}")
        
        for transform in df_transforms:
            self.save_intermediate_df(inspect_row)
            self.apply_transform(transform)
        self.save_intermediate_df(inspect_row)

    @staticmethod
    def load_table(table, separator, sheet_name):
        try:
            sep = separator or Tree.sniff_for_sep(table)
            df = pandas.read_csv(table,sep=sep)
        except:
            df = pandas.read_excel(table,sheet_name=sheet_name)
        df.index += 1
        return df

    @staticmethod
    def sniff_for_sep(csvfile):
        separators = [",",";","\t","|"]
        with open(csvfile, 'r') as f:
            sniffstring = f.read(10000)
            max_count = -1
            for s in separators:
                n = sniffstring.count(s)
                if n > max_count:
                    max_count = n
                    guess = s
        return guess

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
    
    def save_intermediate_df(self, row):
        if row and len(self.df.index) >= row:
            intermediate_df = self.df.iloc[row]
        elif len(self.df.index) > 40:
            head,tail = self.df.head(20).copy(), self.df.tail(20).copy()
            intermediate_df = pandas.concat([head,tail])
        else:
            intermediate_df = self.df.copy()
        self.intermediate_dfs.append(intermediate_df)

    def apply_transform(self, transform):
        self.eval.symtable['df'] = self.df
        try:
            out = self.eval(transform)
        except:
            raise Exception(f"\n\nFailed to apply df_transform:\n{transform}")
        if isinstance(out, pandas.core.frame.DataFrame):
            self.df = out
        elif isinstance(out, pandas.core.series.Series):
            self.df[out.name] = out
        else:
            raise Exception(f"\n\nInvalid return type from df_transform:\n{transform}\nWith return type: {type(out)}")

    def build(self):
        self.root.df = self.df
        self.root.row = next(self.root.df.itertuples())
        self.root.build()
        return self
    
    def toJson(self, indent):
        def json_encoder(obj):
            if isinstance(obj, pandas.Timestamp):
                return obj.date().isoformat()
            else:
                return str(obj)
        return json.dumps(self.root.value, indent=indent, default=json_encoder)

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

        try:
            self.transexpr = self.tree.eval.parse(self.transmute) if self.transmute else None
        except:
            raise Exception(f"\n\nFailed to parse transmute:\n{self.transmute}")

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
            try:
                self.df = self.df.query(self.filter)
            except:
                raise Exception(f"\n\nFailed to apply filter:\n{self.filter}")
            if len(self.df.index)>0:
                self.row = next(self.df.itertuples())
            else:
                self.row = None

    def _transmute(self):
        if self.transmute:
            self.tree.eval.symtable['x'] = self.value
            self.tree.eval.symtable['r'] = self.row
            self.tree.eval.symtable['df'] = self.df
            try:
                self.value = self.tree.eval.run(self.transexpr)
            except Exception as e:
                raise Exception(f"\n\nFailed to run transmute: {self.transmute}\nOn row: {self.row}\nWhen building: {self.name}\n{type(e).__name__}")
     
    def _iterate(self):
        if self.group_by:
            try:
                groups = self.df.groupby(self.group_by, sort=False)
            except:
                raise Exception(f"\n\nFailed to group_by: '{self.group_by}'")
            for group in groups:
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