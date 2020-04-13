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

mapping = format.get('mapping', {})

df = pd.read_csv(args.table)

for t in format.get('df_transforms',[]):
    exec(t)

for f in format.get('functions',[]):
    exec(f)


def main(): 
    mapping_tree = build_tree(mapping)
    stack = []

    output_raw = traverse(mapping_tree, stack, df)
    output_bin = orjson.dumps(
                output_raw, 
                option=orjson.OPT_INDENT_2|orjson.OPT_NON_STR_KEYS, 
                default=lambda x:None
                )
    
    if args.output:
        with open(args.output, 'wb') as f:
            f.write(output_bin)
    else:
        print(output_bin.decode('UTF8'))

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
    # Builds one JSON value (obj, arr, or prim) and pushes it to the stack
    if df is not None:
        row = namedtuple('SomeGenericTupleName',df.iloc[0].index)(*df.iloc[0])

    if node.is_obj:
        stack.append({})
        if df is not None:
            for child in node.children:
                traverse(child, stack, df)
        else:
            # Skip traverse if the scope is a single row
            for child in node.children:
                build(child, stack, row=row)

    if node.is_arr:
        stack.append([])
        if df is not None:
            for child in node.children:
                traverse(child, stack, df)
        else:
            # Skip traverse if the scope is a single row
            for child in node.children:
                build(child, stack, row=row)
                
    elif node.is_prim:
        if node.value_col:
            value = getattr(row, node.value_col)
            stack.append(value if value else node.value)
        else:
            stack.append(node.value)
    

    if node.name_col:
        name = getattr(row, node.name_col)
        node.name = name if name else node.name

    if node.func:
        # Modifies the top element of the stack before consolidating
        stack[-1] = node.func(stack[-1], row, df)
    
    consolidate(node, stack)

def consolidate(node, stack):
    # Takes the top element of the stack and attaches it to its parent
    attachment = stack.pop()
    if stack:
        if isinstance(stack[-1], dict):
            stack[-1][node.name] = attachment
        else:
            stack[-1].append(attachment)
    else:
        stack.append(attachment)

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
        self.is_prim = self.type == 'primitive'
        self.is_obj = self.type == 'object'
        self.is_arr = self.type == 'array'
        
        if self.type not in ('object', 'array', 'primitive'):
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
    