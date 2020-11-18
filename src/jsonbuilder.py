from asteval import Interpreter
import pandas
import rapidjson
import logging
native_eval = 0

class Tree:
    def __init__(self, fmt, table, date=None, inspect_row=None, use_native_eval=False):
        logging.info("Initializing Tree")
        mapping = fmt.get("mapping", {})
        functions = fmt.get("functions", [])
        df_transforms = fmt.get("df_transforms", [])
        separator = fmt.get("separator")
        sheet_name = fmt.get("sheet_name", 0)

        if use_native_eval:
            # Only use this when 'fmt' comes from a trusted source
            # No attempts have been made to sanitize input or similar
            global native_eval, today
            native_eval = 1
            today = pandas.Timestamp(date)

        self.eval = Interpreter()
        self.root = Tree.parse_mapping(self, mapping, 1)
        self.df = Tree.load_table(table, separator, sheet_name)
        self.intermediate_dfs = []

        self.load_functions(functions)
        self.eval.symtable['today'] = pandas.Timestamp(date)
        self.transform_table(df_transforms, inspect_row)

    @staticmethod
    def parse_mapping(tree, mapping, first):
        logging.info("Parsing mapping") if first else None
        children = mapping.pop('children', [])
        t = mapping.get("type")
        if t == "object":
            this = JsonObject(tree, **mapping)
        elif t == "array":
            this = JsonArray(tree, **mapping)
        elif t in ["primitive", None]:
            this = JsonPrimitive(tree, **mapping)
        else:
            logging.error(f"Invalid node type: '{t}''")
            raise Exception(f"Invalid node type: '{t}''")
        for c in children:
            this.children.append(Tree.parse_mapping(tree, c, 0))
        return this

    @staticmethod
    def load_table(table, separator, sheet_name):
        logging.info("Loading table")
        try:
            if not separator:
                candidates = [",", ";", "\t", "|"]
                with open(table, 'r', encoding='utf-8') as f:
                    sniffstring = f.read(10000)
                    max_count = -1
                    for s in candidates:
                        n = sniffstring.count(s)
                        if n > max_count:
                            max_count = n
                            guess = s
                separator = guess
            df = pandas.read_csv(table, sep=separator)
        except Exception:
            try:
                df = pandas.read_excel(table, sheet_name=sheet_name)
            except Exception:
                logging.error("Failed to load table")
                raise
        df.index += 1
        return df

    def load_functions(self, functions):
        logging.info("Loading functions")
        if native_eval:
            for func in functions:
                exec(func, globals())
        else:
            for func in functions:
                try:
                    self.eval(func, show_errors=False)
                except Exception:
                    logging.error("Failed to load functions")
                    raise
        return self

    def transform_table(self, df_transforms, inspect_row):
        logging.info("Transforming table")
        for transform in df_transforms:
            self._save_intermediate_df(inspect_row)
            self._apply_transform(transform)
        self._save_intermediate_df(inspect_row)
        return self

    def _save_intermediate_df(self, inspect_row):
        if inspect_row and 1 < inspect_row < len(self.df.index):
            intermediate_df = self.df.iloc[inspect_row-2:inspect_row+1].copy()
        elif len(self.df.index) > 40:
            head, tail = self.df.head(20).copy(), self.df.tail(20).copy()
            intermediate_df = pandas.concat([head, tail])
        else:
            intermediate_df = self.df.copy()
        self.intermediate_dfs.append(intermediate_df)

    def _apply_transform(self, transform):
        if native_eval:
            f = eval("lambda df:" + transform)
            out = f(self.df)
        else:
            self.eval.symtable['df'] = self.df
            parsed_transform = self.eval.parse(transform)
            out = self.eval.run(parsed_transform, with_raise=False)
            if self.eval.error:
                logging.error(f"Failed to apply transform: {transform}")
                raise Exception(self.eval.error[0].msg)
        if isinstance(out, pandas.core.frame.DataFrame):
            self.df = out
        elif isinstance(out, pandas.core.series.Series):
            self.df[out.name] = out
        else:
            logging.error("Unexpected error while pre-processing DataFrame")
            msg = f"\n\nInvalid return type from df_transform: {transform}\n"\
                  f"With return type: {type(out)}\n"\
                  f"Should be one of: 'pandas.core.series.Series' (column) or 'pandas.core.frame.DataFrame' (table)"
            raise Exception(msg)

    def build(self):
        logging.info("Building Tree")
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
        logging.info("Dumping Tree to JSON")
        return rapidjson.dumps(self.root.value, indent=indent, default=json_encoder)


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
        self.transexpr = None
        self.df = None
        self.row = None
        self.children = []

        if self.transmute:
            if native_eval:
                self.transexpr = eval(
                    "lambda x,r,df:("+self.transmute+",)[-1]")
            else:
                self.transexpr = self.tree.eval.parse(self.transmute)
                if self.tree.eval.error:
                    logging.error(
                        f"Unexpected error while loading transmute: {self.transmute}")
                    raise Exception(self.tree.eval.error_msg)

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
                ld = {'today': self.tree.eval.symtable['today']}
                self.df = self.df.query(self.filter, local_dict=ld)
            except Exception:
                logging.error(f"Failed to apply filter: {self.filter}")
                raise
            if len(self.df.index) > 0:
                self.row = next(self.df.itertuples())
            else:
                self.row = None

    def _transmute(self):
        if self.transmute:
            if native_eval:
                try:
                    self.value = self.transexpr(self.value, self.row, self.df)
                except Exception:
                    logging.error(
                        f"Unexpected error while transmuting {self.name} on row: {getattr(self.row, 'Index')}")
                    raise
            else:
                self.tree.eval.symtable['x'] = self.value
                self.tree.eval.symtable['r'] = self.row
                self.tree.eval.symtable['df'] = self.df
                self.value = self.tree.eval.run(
                    self.transexpr, with_raise=False)
                if self.tree.eval.error:
                    logging.error(
                        f"Unexpected error while transmuting {self.name} on row: {getattr(self.row, 'Index')}")
                    raise Exception(self.tree.eval.error[0].msg)

    def _iterate(self):
        if self.group_by:
            try:
                groups = self.df.groupby(self.group_by, sort=False)
            except Exception:
                logging.error(f"Failed to group_by: '{self.group_by}'")
                raise
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
            try:
                self.value = getattr(self.row, self.column)
            except Exception:
                logging.error(
                    f"Failed to fetch data from column '{self.column}' while building '{self.name}'")
                raise
        self._transmute()
        return self
