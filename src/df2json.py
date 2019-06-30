import pandas as pd
import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument('-data', nargs='?')
parser.add_argument('-mapping', nargs='?')
args = parser.parse_args()


# setup
if args.data:
    df = pd.read_csv(args.data)
else:
    df = pd.read_csv('../test/test.txt', na_filter=False).astype(str)

if args.mapping:
    with open(args.mapping, 'r') as f:
        mapping = json.load(f)
else:
    with open('../test/mapping3.json', "r") as f:
        mapping = json.load(f)

assert mapping["type"] in ["array", "object"]
assert mapping["group"] == True
assert "scope" not in mapping

memory = {}
json_level = 0


def main():

    traverse(df, mapping)
    output = memory[1]

    print('\n Full dataframe:')
    print(df)
    print('')
    print(json.dumps(output, indent=3))


def traverse(df, node):
    
    global json_level
    global memory

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
        
        if "name_col" in node:
            node["name"] = group[1].iloc[0][node["name_col"]]


        if node["type"] == "leaf":
            if "value_col" in node:
                child = group[1].iloc[0][node["value_col"]]
            else:
                child = node["value"]

        elif node["type"] == "object":
            json_level += 1
            memory[json_level] = {}
            for sub_node in node["children"]:
                traverse(group[1], sub_node)
            json_level -= 1
            child = memory[json_level+1]

        elif node["type"] == "array":
            json_level += 1
            memory[json_level] = []
            for sub_node in node["children"]:
                traverse(group[1], sub_node)
            json_level -= 1
            child = memory[json_level+1]


        if "func" in node:
            f = eval(node["func"])
            child = f(child, scope.iloc[0], memory[json_level])


        if json_level == 0:
            pass
        else:
            if type(memory[json_level]) == dict:
                memory[json_level][node["name"]] = child
            else:
                memory[json_level].append(child)


if __name__ == "__main__":
    main()