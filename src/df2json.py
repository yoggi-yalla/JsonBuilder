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
assert mapping["group"] == True
assert "func" not in mapping

'''
Test this script by running below line in the terminal:
python df2json.py -data '../test/test.txt' -mapping '../test/mapping3.json'
'''


def main():
    
    memory = {}
    level = 0

    output = traverse(df, mapping, memory, level)

    print(json.dumps(output, indent=3))
    print(time.process_time())


def traverse(df, node, memory, level):

    if "scope" in node:
        scope = df.loc[eval(node["scope"])]
    else:
        scope = df

    if "group" in node:
        if node["group"] == True:
            iterator = scope.groupby(scope.index != None)
        else:
            iterator = scope.groupby(node["group"])
    else:
        iterator = scope.groupby(scope.index)   

    for group in iterator:

        level += 1

        if node["type"] == "leaf":
            if "value_col" in node:
                memory[level] = group[1].iloc[0][node["value_col"]]
            else:
                memory[level] = node["value"]

        elif node["type"] == "object":
            memory[level] = {}
            for sub_node in node["children"]:
                traverse(group[1], sub_node, memory, level)

        elif node["type"] == "array":
            memory[level] = []
            for sub_node in node["children"]:
                traverse(group[1], sub_node, memory, level)
                
        if "name_col" in node:
            node["name"] = group[1].iloc[0][node["name_col"]]

        if "func" in node:
            f = eval(node["func"])
            memory[level] = f(memory[level], scope.iloc[0], memory[level-1])

        level -= 1

        if level == 0:
            return memory[1]
        else:            
            if type(memory[level]) == dict:
                memory[level][node["name"]] = memory[level+1]
                del memory[level+1]
            else:
                memory[level].append(memory[level+1])
                del memory[level+1]
        

if __name__ == "__main__":
    main()