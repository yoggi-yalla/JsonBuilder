import pandas as pd
import argparse
import json
import time
import re

parser = argparse.ArgumentParser()
parser.add_argument('-table', nargs='?')
parser.add_argument('-mapping', nargs='?')
args = parser.parse_args()


'''
Run the script in command line from this working dir:
python df2json.py -table ../test/test.txt -mapping ../test/mapping3.json
'''

def main():    
    df = pd.read_csv(args.table)

    with open(args.mapping) as f:
        mapping = json.load(f)

    root = build_tree(mapping)
    mem = {}
    ref = 0

    output = traverse(df, root, mem, ref)

    print(df)
    out = json.dumps(output, indent=3, sort_keys=True, ensure_ascii=False)
    print(out)
    print(time.process_time())


def traverse(df, node, mem, ref):
    if node.filter:
        df = df[df[node.filter_col].apply(is_match, regex=node.filter)]

    if node.multiple:
        if node.group_col:
            for group in df.groupby(node.group_col, sort=False):
                process_group(group[1], node, mem, ref)
        else:
            for row in df.itertuples():          
                process_row(row, node, mem, ref)
    else:
        process_group(df, node, mem, ref)
    
    return mem[1]


def process_group(df, node, mem, ref):
    ref += 1

    if node.type == "object":
        mem[ref] = {}
        for child in node.children:
            traverse(df, child, mem, ref)

    elif node.type == "array":
        mem[ref] = []
        for child in node.children:
            traverse(df, child, mem, ref)

    elif node.type == "leaf":
        if node.value_col:
            mem[ref] = df.iloc[0][node.value_col]
        else:
            mem[ref] = node.value
            
    if node.name_col:
        node.name = df.iloc[0][node.name_col]

    ref -= 1
    attach(node, mem, ref)


def process_row(row, node, mem, ref):
    ref += 1

    if node.type == "object":
        mem[ref] = {}
        for child in node.children:     
            process_row(row, child, mem, ref)

    elif node.type == "array":
        mem[ref] = []
        for child in node.children:
            process_row(row, child, mem, ref)

    elif node.type == "leaf":
        if node.value_col:
            mem[ref] = getattr(row, node.value_col)
        else:
            mem[ref] = node.value
            
    if node.name_col:
        node.name = getattr(row, node.name_col)

    ref -= 1
    attach(node, mem, ref)


def attach(node, mem, ref):
    if ref == 0:
        pass
    else:            
        if type(mem[ref]) == dict:
            mem[ref][node.name] = mem[ref+1]
            del mem[ref+1]
        else:
            mem[ref].append(mem[ref+1])
            del mem[ref+1]


def is_match(obj, regex):
    if obj:
        match = re.search(regex, obj)
        if match:
            return True
        else:
            return False
    else:
        return False


class Node(object):
    def __init__(self, mapping):
        self.type = mapping["type"]
        self.name = mapping.get("name")
        self.name_col = mapping.get("name_col")
        self.value = mapping.get("value")
        self.value_col = mapping.get("value_col")
        self.filter = mapping.get("filter")
        self.filter_col = mapping.get("filter_col")
        self.multiple = mapping.get("multiple")
        self.group_col = mapping.get("group_col")
        self.children = []

    def add_child(self, obj):
        self.children.append(obj)

def build_tree(mapping):
    root = Node(mapping)
    for c in mapping["children"]:
        build_sub_tree(root, c)
    return root

def build_sub_tree(parent, mapping):
    this_node = Node(mapping)
    parent.add_child(this_node)
    if mapping.get("children") == None or len(mapping["children"]) == 0:
        pass
    else:
        for c in mapping["children"]:
            build_sub_tree(this_node, c)
        

if __name__ == "__main__":
    main()