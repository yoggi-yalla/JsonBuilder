import pandas as pd
import argparse
import json
import rapidjson
import time
import cProfile
import pstats

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

mem = [] # dedicated stack memory for json construction


def main(): 
    root = build_tree(mapping)
    
    output_raw = traverse(root, df)
    output_str = rapidjson.dumps(output_raw, indent=3)
    
    if args.output:
        with open(args.output, "w") as f:
    	    f.write(output_str)
    else:
        print(output_str)

    print(time.process_time())


def traverse(node, df):
    if node.filter:
        df = df[eval(node.filter)]

    if node.multiple:
        if node.group_by:
            for group in df.groupby(eval(node.group_by), sort=False):
                process(node, df=group[1])
        else:
            for row in df.itertuples():
                row = row._asdict()
                process(node, row=row)
    else:
        process(node, df=df)
    
    return mem[-1] if mem else None


def process(node, df=pd.DataFrame(), row={}):
    if not row and len(df)>0:
        row = df.iloc[0].to_dict()

    if node.type in ("object", "array"):
        mem.append({} if node.type == "object" else [])
        if len(df)>0:
            for child in node.children:
                traverse(child, df)
        else:
            for child in node.children:
                process(child, row=row)
    else:
        if node.value_col:
            mem.append(row.get(node.value_col))
        else:
            mem.append(node.value)
    
    if node.name_col:
        node.name = row.get(node.name_col)

    if node.func:
        mem[-1] = node.func(mem[-1], row, df)

    if len(mem) > 1:
        attachment = mem.pop()
        if isinstance(mem[-1], dict):
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

    
if __name__ == "__main__":
    main()
    #cProfile.run("main()", 'restats')
    #p = pstats.Stats('restats')
    #p.sort_stats('time').print_stats(35)