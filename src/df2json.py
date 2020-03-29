import pandas as pd
import argparse
import json
import time
import orjson
import profile

parser = argparse.ArgumentParser()
parser.add_argument('-table', nargs='?')
parser.add_argument('-format', nargs='?')
parser.add_argument('-o', nargs='?')
args = parser.parse_args()


'''
Run the script in command line from this working dir:
python df2json.py -table ../test/test.txt -format ../test/format3.json
'''


with open(args.format) as f:
    format = json.load(f)

for f in format.get("functions",[]):
    exec(f)


def main():

    df = pd.read_csv(args.table)
    for t in format.get("df_transforms",[]):
        exec(t)

    root = build_tree(format["mapping"])

    mem = {}
    ref = 0

    output_json = traverse(df, root, mem, ref)
    output_str = json.dumps(output_json, indent=3)
    
    with open(args.o, "w") as f:
    	f.write(output_str)

    print(time.process_time())


def traverse(df, node, mem, ref):

    if node.filter:
        df = df[eval(node.filter)]

    if node.multiple:
        if node.group_by:
            for group in df.groupby(eval(node.group_by), sort=False):
                process_group(group[1], node, mem, ref)
        else:
            for row in df.itertuples():
                row = row._asdict()
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
        f = node.func
        mem[ref] = f(mem[ref], df.iloc[0].to_dict(), mem[ref-1])

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
            mem[ref] = row[node.value_col]
        else:
            mem[ref] = node.value
    
    if node.name_col:
        node.name = row[node.name_col]

    if node.func:
        f = node.func
        mem[ref] = f(mem[ref], row, mem[ref-1])

    ref -= 1
    attach(node, mem, ref)


def attach(node, mem, ref):
    if ref == 0:
        pass
    else:            
        if isinstance(mem[ref], dict):
            mem[ref][node.name] = mem[ref+1]
        else:
            mem[ref].append(mem[ref+1])


class Node(object):
    def __init__(self, m):
        self.type = m["type"]
        self.name = m.get("name")
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
    for c in m["children"]:
        build_sub_tree(root, c)
    return root


def build_sub_tree(parent, m):
    this_node = Node(m)
    parent.add_child(this_node)
    if not m.get("children"):
        pass
    else:
        for c in m["children"]:
            build_sub_tree(this_node, c)
        


if __name__ == "__main__":
    main()