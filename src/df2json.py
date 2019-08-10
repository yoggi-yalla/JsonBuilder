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

assert mapping["type"] in ["object", "array"]
assert "func" not in mapping
assert "split" not in mapping

'''
Run the script in command line from this working dir:
python df2json.py -data ../test/test.txt -mapping ../test/mapping3.json
'''


def main():

    '''
    mem is used as RAM when building the json
    ref is used to reference data in mem
    '''
    mem = {}
    ref = 0

    output = traverse(df, mapping, mem, ref)

    print(df)
    out = json.dumps(output, indent=3, sort_keys=True, ensure_ascii=False)
    print(out)
    print(time.process_time())


def traverse(df, node, mem, ref):

    if "filter" in node:
        scope = df.loc[eval(node["filter"])]
    else:
        scope = df

    if "split_by" in node:

        if node["split_by"] == "row":
            for row in scope.itertuples():
                process_row(row, node, mem, ref)

        elif node["split_by"] == "group":
            assert "group" in node
            f = eval(node["group"])
            for group in scope.groupby(f(), sort=False):
                process_scope(group[1], node, mem, ref)

    else:
        process_scope(scope, node, mem, ref)

    return mem[1]


def process_scope(scope, node, mem, ref):

    ref += 1

    if node["type"] == "object":
        mem[ref] = {}
        for child in node["children"]:
            traverse(scope, child, mem, ref)

    elif node["type"] == "array":
        mem[ref] = []
        for child in node["children"]:
            traverse(scope, child, mem, ref)

    elif node["type"] == "leaf":
        if "value_col" in node:
            mem[ref] = scope.iloc[0][node["value_col"]]
        else:
            mem[ref] = node["value"]
            
    if "name_col" in node:
        node["name"] = scope.iloc[0][node["name_col"]]

    if "func" in node:
        f = eval(node["func"])
        mem[ref] = f(mem[ref], scope.iloc[0], mem[ref-1])

    ref -= 1

    attach(node, mem, ref)


def process_row(row, node, mem, ref):

    ref += 1

    if node["type"] == "object":
        mem[ref] = {}
        for child in node["children"]:     
            process_row(row, child, mem, ref)

    elif node["type"] == "array":
        mem[ref] = []
        for child in node["children"]:
            process_row(row, child, mem, ref)

    elif node["type"] == "leaf":
        if "value_col" in node:
            mem[ref] = getattr(row, node["value_col"])
        else:
            mem[ref] = node["value"]
            
    if "name_col" in node:
        node["name"] = getattr(row, node["name_col"])

    if "func" in node:
        f = eval(node["func"])
        mem[ref] = f(mem[ref], row, mem[ref-1])

    ref -= 1

    attach(node, mem, ref)


def attach(node, mem, ref):
    if ref == 0:
        pass
    else:            
        if type(mem[ref]) == dict:
            mem[ref][node["name"]] = mem[ref+1]
            del mem[ref+1]
        else:
            mem[ref].append(mem[ref+1])
            del mem[ref+1]
       
        

if __name__ == "__main__":
    main()