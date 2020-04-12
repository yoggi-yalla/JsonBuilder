import pandas as pd
import argparse
import json
from collections import namedtuple

#orjson gives a huge performance boost when writing json
import orjson

#The following imports are only relevant for benchmarking
import time
import cProfile
import pstats
import os



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
    
for f in format.get("functions",[]):
    exec(f)

mem = [] # dedicated stack for json construction


def main(): 
    if args.table:
        df = pd.read_csv(args.table)
    else:
        df = pd.DataFrame()

    for t in format.get("df_transforms",[]):
        exec(t)

    root = build_tree(mapping)
    output_raw = traverse(root, df, False)
    
    if args.output:
        with open(args.output, "wb") as f:
            output_binary = orjson.dumps(output_raw, option=orjson.OPT_INDENT_2)
            f.write(output_binary)
    else:
        print(json.dumps(output_raw, indent=2))

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
        row = namedtuple('SomeGenericTupleName',df.iloc[0].index)(*df.iloc[0])
    else:
        row = ()

    if node.is_obj:
        mem.append({})
        if not len(df.index) == 0:
            for child in node.children:
                traverse(child, df, True)
        else:
            for child in node.children:
                build_fast(node, row, True)

    elif node.is_arr:
        mem.append([])
        if not len(df.index) == 0:
            for child in node.children:
                traverse(child, df, False)
        else:
            for child in node.children:
                build_fast(node, row, False)
                
    else: #if node.is_prim:
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
    if node.is_obj:
        mem.append({})
        for child in node.children:
            build_fast(child,row,True)

    elif node.is_arr:
        mem.append([])
        for child in node.children:
            build_fast(child,row,False)

    else: #if node.is_prim:
        if node.value_col:
            mem.append(getattr(row, node.value_col, node.value))
        else:
            mem.append(node.value)

    if parent_is_obj:
        if node.name_col:
            node.name = getattr(row, node.name_col, node.name)


    if node.func:
        mem[-1] = node.func(mem[-1], row, None)

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

def attach_fast(node, parent_is_obj):
    attachment = mem.pop()
    if parent_is_obj:
        mem[-1][node.name] = attachment
    else:
        mem[-1].append(attachment)


class Node(object):
    def __init__(self, m):
        self.type = m.get("type", "primitive")
        self.name = m.get("name", "MISSING_NAME")
        self.name_col = m.get("name_col")
        self.value = m.get("value")
        self.value_col = m.get("value_col")
        self.filter = m.get("filter")
        self.multiple = m.get("multiple")
        self.group_by = m.get("group_by")
        self.func = eval(m.get("func", "False"))
        self.children = []
        self.is_prim = self.type == "primitive"
        self.is_obj = self.type == "object"
        self.is_arr = self.type == "array"

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


if __name__ == "__main__":
    main()
    #cProfile.run("main()", 'tmp')
    #p = pstats.Stats('tmp')
    #p.sort_stats('cumtime').print_stats(20)
    #os.remove('tmp')
    