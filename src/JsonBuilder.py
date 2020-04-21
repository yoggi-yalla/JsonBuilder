class JsonBuilder(object):
    def __init__(self, m={}, df=None, functions={}):
        self.type = m.get('type')
        self.value_col = m.get('value_col')
        self.name_col = m.get('name_col')
        self.value = m.get('value')
        self.name = m.get('name')
        self.multiple = m.get('multiple')
        self.children = []

        self.df = df
        self.row = None
        
        globals().update(functions)
        self.func = eval(m.get('func', 'None'))
        self.filter = eval(m.get('filter', 'None'))
        self.group_by = eval(m.get('group_by', 'None'))

        for child in m.get('children', []):
            self.children.append(JsonBuilder(child))
        
        if self.df is not None:
            if self.filter:
                self.df = self.df[self.filter(self.df)]
            if len(self.df.index)>0:
                self.row = next(df.itertuples())

    def build(self):
        if self.type == 'object':
            self._build_object()

        elif self.type == 'array':
            self._build_array()

        else:
            self._fetch_value()
            
        self._fetch_name()

        self._apply_func()

        return self.name, self.value

    def _build_object(self):
        self.value = {}
        for child in self.children:
                child.df, child.row = self.df, self.row
                for _ in child._next_scope():
                    k,v = child.build()
                    self.value[k] = v

    def _build_array(self):
        if self.value_col:
            self.value = self.df[self.value_col].tolist()
        else:
            self.value = []
            for child in self.children:
                child.df, child.row = self.df, self.row
                for _ in child._next_scope():
                    k,v = child.build()
                    self.value.append(v)

    def _next_scope(self):
        if self.filter:
            self.df = self.df[self.filter(self.df)]

        if self.multiple:
            if self.group_by:
                for group in self.df.groupby(self.group_by(self.df), sort=False):
                    self.df = group[1]
                    self.row = next(group[1].itertuples())
                    yield
            else:
                for row in self.df.itertuples():
                    self.row = row
                    yield
        else:
            yield
    
    def _fetch_value(self):
        if self.value_col and self.row is not None:
            self.value = getattr(self.row, self.value_col)
    
    def _fetch_name(self):
        if self.name_col and self.row is not None:
            self.name = getattr(self.row, self.name_col)

    def _apply_func(self):
        if self.func:
            self.value = self.func(self.value, self.row, self.df)