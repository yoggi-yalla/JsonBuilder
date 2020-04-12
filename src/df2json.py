import pandas as pd
import argparse
import json
import rapidjson
import time
import cProfile
import pstats
import os
import functools
from collections import namedtuple

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--format')
parser.add_argument('-t', '--table', nargs ='?')
parser.add_argument('-o', '--output', nargs='?')
args = parser.parse_args()


'''
Run the script in command line from this working dir:
python df2json.py -t ../test/test.csv -f ../test/format3.json
'''


with open(args.format) as f:
    format = json.load(f)

mapping = format.get("mapping", {})

if args.table:
    df = pd.read_csv(args.table)
else:
    df = pd.DataFrame()

for t in format.get("df_transforms",[]):
    exec(t)
    
for f in format.get("functions",[]):
    exec(f)

mem = [] # dedicated stack for json construction


def main(): 
    root = build_tree(mapping)
    
    parent_is_obj = root.type == "object"

    output_raw = traverse(root, df, parent_is_obj)
    output_str = rapidjson.dumps(output_raw, indent=3)
    
    if args.output:
        with open(args.output, "w") as f:
    	    f.write(output_str)
    else:
        print(output_str)

    print(time.process_time())


def traverse(node, df, parent_is_obj):
    if node.filter:
        df = df[eval(node.filter)]

    if node.multiple:
        if node.group_by:
            for group in df.groupby(eval(node.group_by), sort=False):
                build_slow(node, group[1])
        else:
            for row in df.itertuples():
                build_fast(node, row, parent_is_obj)
    else:
        build_slow(node, df)
    
    return mem[-1] if mem else None


def build_slow(node, df):
    if not len(df.index) == 0:
        row = namedtuple_me(df.iloc[0])

    if node.type == "object":
        mem.append({})
        if not len(df.index) == 0:
            for child in node.children:
                traverse(child, df, True)
        else:
            for child in node.children:
                build_fast(node, row, True)

    elif node.type == "array":
        mem.append([])
        if not len(df.index) == 0:
            for child in node.children:
                traverse(child, df, False)
        else:
            for child in node.children:
                build_fast(node, row, False)
                
    else:
        if node.value_col:
            mem.append(getattr(row, node.value_col, node.value))
        else:
            mem.append(node.value)

    if node.name_col:
        node.name = getattr(row, node.name_col, node.name)

    if node.func:
        mem[-1] = node.func(mem[-1], row, df)

    attach_slow(node)


def build_fast(node, row, parent_is_obj):

    if node.type == "leaf":
        if node.value_col:
            mem.append(getattr(row, node.value_col, node.value))
        else:
            mem.append(node.value)

    elif node.type == "object":
        mem.append({})
        for child in node.children:
            build_fast(child,row,True)

    else:
        mem.append([])
        for child in node.children:
            build_fast(child,row,False)

    if parent_is_obj:
        if node.name_col:
            node.name = getattr(row, node.name_col, node.name)

    if node.func:
        mem[-1] = node.func(mem[-1], row, df)

    attach_fast(node, parent_is_obj)


def attach_slow(node):
    attachment = mem.pop()
    if mem:
        if isinstance(mem[-1], dict):
            mem[-1][node.name] = attachment
        else:
            mem[-1].append(attachment)
    else:
        mem.append(attachment)

def attach_fast(node,parent_is_obj):
    attachment = mem.pop()
    if parent_is_obj:
        mem[-1][node.name] = attachment
    else:
        mem[-1].append(attachment)


class Node(object):
    def __init__(self, m):
        self.type = m.get("type", "leaf")
        self.name = m.get("name", "MISSING_NAME")
        self.name_col = m.get("name_col")
        self.value = m.get("value")
        self.value_col = m.get("value_col")
        self.filter = m.get("filter")
        self.multiple = m.get("multiple")
        self.group_by = m.get("group_by")
        self.func = eval(m.get("func", "False"))
        self.children = []

    def add_child(self, obj):
        self.children.append(obj)

def build_tree(m):
    root = Node(m)
    for c in m.get("children", []):
        build_sub_tree(root, c)
    return root

def build_sub_tree(parent, m):
    this_node = Node(m)
    parent.add_child(this_node)
    for c in m.get("children", []):
        build_sub_tree(this_node, c)


@functools.lru_cache(maxsize=None)
def _get_class(fieldnames, name):
    return namedtuple(name, fieldnames)

def namedtuple_me(series, name='do_not_use_this_as_col_name'):
    klass = _get_class(tuple(series.index), name)
    return klass._make(series)
    
if __name__ == "__main__":
    main()
    #cProfile.run("main()", 'tmp')
    #p = pstats.Stats('tmp')
    #p.sort_stats('cumtime').print_stats(20)
    #os.remove('tmp')
    