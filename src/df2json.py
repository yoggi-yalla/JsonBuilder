import pandas as pd
import numpy as np
import argparse
import time
import json
import orjson
from collections import namedtuple

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--format')
parser.add_argument('-t', '--table')
parser.add_argument('-o', '--output', nargs='?')
args = parser.parse_args()

'''
Run the script in command line from this working dir:
python df2json.py -t ../test/test.csv -f ../test/format3.json
'''

with open(args.format) as f:
    format = json.load(f)

for f in format.get('functions',[]):
    exec(f)

mapping = format.get('mapping', {})

df = pd.read_csv(args.table)

for t in format.get('column_transforms',[]):
    f = eval(t['func'])
    col = t.get('col')
    df[col] = f(df,col)


def main(): 
    mapping_tree = build_tree(mapping)
    stack = []

    output_native = traverse(mapping_tree, stack, df)
    output_binary = orjson.dumps(
                output_native, 
                option=orjson.OPT_INDENT_2|orjson.OPT_NON_STR_KEYS,
                )
    
    if args.output:
        with open(args.output, 'wb') as f:
            f.write(output_binary)
    else:
        print(output_binary.decode('UTF8'))

    print(time.process_time())


def traverse(node, stack, df):
    # Traverses the dataframe and coordinates 'build'-calls
    if node.filter:
        df = df[eval(node.filter)]

    if len(df.index)>0:
        if node.multiple:
            if node.group_by:
                for group in df.groupby(eval(node.group_by), sort=False):
                    build(node, stack, df=group[1])
            else:
                for row in df.itertuples():
                    build(node, stack, row=row)
        else:
            build(node, stack, df=df)
    
    return stack[-1] if stack else None


def build(node, stack, row=None, df=None):
    # Builds one JSON value (obj, arr, or prim) and appends it to the stack
    if df is not None:
        row = namedtuple('SomeGenericTupleName',df.iloc[0].index)(*df.iloc[0])

    if node._is_object or node._is_array:
        stack.append({} if node._is_object else [])
        if df is not None:
            for child in node.children:
                traverse(child, stack, df)
        else:
            # Skip traverse if the scope is just a row
            for child in node.children:
                build(child, stack, row=row)

    elif node._is_primitive:
        if node.value_col:
            value = getattr(row, node.value_col)
            if value is np.nan:
                value = None
            stack.append(value if value else node.value)
        else:
            stack.append(node.value)

    elif node._is_quickarray:
        stack.append(df[node.value_col].tolist())

    if node.name_col:
        name = getattr(row, node.name_col)
        if name is np.nan:
            name = None
        node.name = name if name else node.name

    if node.func:
        # Modifies the top element of the stack before consolidating
        stack[-1] = node.func(stack[-1], row, df)
    
    consolidate(node, stack)

def consolidate(node, stack):
    # Takes the top element of the stack and attaches it to its parent
    value = stack.pop()
    if stack:
        if isinstance(stack[-1], dict):
            stack[-1][node.name] = value
        else:
            stack[-1].append(value)
    else:
        stack.append(value)

class Node(object):
    def __init__(self, m):
        self.type = m.get('type', 'primitive')
        self.name = m.get('name')
        self.name_col = m.get('name_col')
        self.value = m.get('value')
        self.value_col = m.get('value_col')
        self.filter = m.get('filter')
        self.multiple = m.get('multiple')
        self.group_by = m.get('group_by')
        self.func = eval(m.get('func', 'False'))
        self.children = []

        self._is_primitive = self.type == 'primitive'
        self._is_object = self.type == 'object'
        self._is_array = self.type == 'array'
        self._is_quickarray = self.type == 'quickarray'
        
        if self.type not in ('object', 'array', 'primitive', 'quickarray'):
            raise ValueError("Invalid node type: {}".format(self.type))
        

    def add_child(self, obj):
        self.children.append(obj)

def build_tree(m):
    root = Node(m)
    for c in m.get('children', []):
        build_sub_tree(root, c)
    return root

def build_sub_tree(parent, m):
    this_node = Node(m)
    parent.add_child(this_node)
    for c in m.get('children', []):
        build_sub_tree(this_node, c)


if __name__ == '__main__':
    main()
    