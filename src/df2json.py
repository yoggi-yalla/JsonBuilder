import pandas as pd
import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument('-df', nargs='?')
parser.add_argument('-mapping', nargs='?')
args = parser.parse_args()


# setup
if args.df:
    df = pd.read_csv(args.df)
else:
    df = pd.read_csv('../test/test.txt', na_filter=False).astype(str)

if args.mapping:
    with open(args.mapping, 'r') as f:
        mapping = json.load(f)
else:
    with open('../test/mapping1.json', "r") as f:
        mapping = json.load(f)

temp_nodes = {}
temp_iterators = {}
level = 0

if mapping["type"] == "object":
    temp_nodes[0] = {}
    temp_nodes[0]["root"] = {}
else:
    temp_nodes[0] = []



def main():

    traverse_mapping(df, mapping, level)

    if type(temp_nodes[0]) == dict:
        output = temp_nodes[0]["root"]
    else:
        if len(temp_nodes[0]) > 0:
            output = temp_nodes[0][0]
        else:
            output = temp_nodes[0]

    print('\n Full dataframe:')
    print(df)
    print('')
    print(json.dumps(output, indent=3))


def traverse_mapping(df, node, level):

    if "scope" in node:
        scope = df.loc[eval(node["scope"])]
    else:
        scope = df


    if "group" in node:
        if node["group"] == True:
            temp_iterators[level] = scope.groupby(scope.index != None)
        else:
            temp_iterators[level] = scope.groupby(node["group"])
    else:
        temp_iterators[level] = scope.groupby(scope.index)


    for group in temp_iterators[level]:

        parent = temp_nodes[level]
        
        if "name_col" in node:
            node["name"] = group[1].iloc[0][node["name_col"]]

        if node["type"] == "leaf":
            if "value_col" in node:
                child = group[1].iloc[0][node["value_col"]]
            else:
                child = node["value"]

        elif node["type"] == "object":
            level += 1
            temp_nodes[level] = {}
            for sub_node in node["sub_nodes"]:
                traverse_mapping(group[1], sub_node, level)
            level -= 1
            child = temp_nodes[level+1]


        elif node["type"] == "array":
            level += 1
            temp_nodes[level] = []
            for sub_node in node["sub_nodes"]:
                traverse_mapping(group[1], sub_node, level)
            level -= 1
            child = temp_nodes[level+1]

        if "func" in node:
            f = eval(node["func"])
            child = f(child, scope.iloc[0], parent)

        attach_node(node, child, parent)
    

def attach_node(node, child, parent):
    if type(parent) == dict:
        parent[node["name"]] = child
    else:
        parent.append(child)

if __name__ == "__main__":
    main()
