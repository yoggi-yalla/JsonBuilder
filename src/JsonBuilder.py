class Node(object):
    def __init__(self, m, df=None, functions={}):
        self.type = m.get('type', 'primitive')
        self.value_col = m.get('value_col')
        self.name_col = m.get('name_col')
        self.value = m.get('value')
        self.name = m.get('name')
        self.multiple = m.get('multiple')
        self.children = []

        self.df = df
        self.row = None

        self._is_primitive = self.type == 'primitive'
        self._is_object = self.type == 'object'
        self._is_array = self.type == 'array'
        self._is_prim_array = self.type == 'prim_array'
        
        globals().update(functions)
        self.func = eval(m.get('func', 'None'))
        self.filter = eval(m.get('filter', 'None'))
        self.group_by = eval(m.get('group_by', 'None'))

        for child in m.get('children', []):
            self.children.append(Node(child))

    def build(self):
        if self._is_object or self._is_array:
            self.value = {} if self._is_object else []
            for child in self.children:
                child.df, child.row = self.df, self.row
                for _ in child._next_scope():
                    self.attach(child.build())

        elif self._is_primitive:
            if self.value_col:
                self.value = getattr(self.row, self.value_col)   

        elif self._is_prim_array:
            self.value = self.df[self.value_col].tolist()

        if self.name_col:
            self.name = getattr(self.row, self.name_col)

        if self.func:
            self.value = self.func(self.value, self.row, self.df)

        return self

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
    
    def attach(self, child):
        if self._is_object:
            self.value[child.name] = child.value
        elif self._is_array:
            self.value.append(child.value)