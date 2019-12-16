import pandas as pd
import argparse
import json
import time

parser = argparse.ArgumentParser()
parser.add_argument('-data', nargs='?')
parser.add_argument('-mapping', nargs='?')
args = parser.parse_args()

df = pd.read_csv(args.data)
mapping = json.load(open(args.mapping))

'''
Run the script in command line from this working dir:
python df2json.py -data ../test/test.txt -mapping ../test/mapping3.json
'''

def main():
    
    mem = {}
    ref = 0
    tree = build_tree(mapping)

    output = traverse(df, tree, mem, ref)

    print(df)
    out = json.dumps(output, indent=3, sort_keys=True, ensure_ascii=False)
    print(out)
    print(time.process_time())


def traverse(df, node, mem, ref):
    if node.filter:
        df = df.loc[eval(node.filter)]

    if node.multiple == True:
        if node.group_by:
            for group in df.groupby(eval(node.group_by), sort=False):
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

    if node.func:
        f = eval(node.func)
        mem[ref] = f(mem[ref], df.iloc[0], mem[ref-1])

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

    if node.func:
        f = eval(node.func)
        mem[ref] = f(mem[ref], row, mem[ref-1])

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


class Node(object):
    def __init__(self, data):
        self.type = data["type"]
        self.name = data.get("name")
        self.name_col = data.get("name_col")
        self.value = data.get("value")
        self.value_col = data.get("value_col")
        self.filter = data.get("filter")
        self.multiple = data.get("multiple")
        self.group_by = data.get("group_by")
        self.func = data.get("func")
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