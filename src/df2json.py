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
    with open('../test/mapping.txt.json', "r") as f:
        mapping = json.load(f)

temp_items = {}
temp_iterators = {}
level = 0

if mapping["type"] == "object":
    temp_items[0] = {}
    temp_items[0]["root"] = {}
else:
    temp_items[0] = []



def main():

    process_items(df, mapping, level)

    if type(temp_items[0]) == dict:
        output = temp_items[0]["root"]
    else:
        if len(temp_items[0]) > 0:
            output = temp_items[0][0]
        else:
            output = temp_items[0]

    print('\n Full dataframe:')
    print(df)
    print('')
    print(json.dumps(output, indent=4))


def process_items(df, item, level):

    scope = df
    if "scope" in item:
        scope = df.loc[eval(item["scope"])]


    if "group" in item:
        if item["group"] == True:
            temp_iterators[level] = scope.groupby(scope.index != None)
        else:
            temp_iterators[level] = scope.groupby(item["group"])
    else:
        temp_iterators[level] = scope.groupby(scope.index)


    for group in temp_iterators[level]:

        parent = temp_items[level]

        if item["type"] == "simple":
            if "column" in item:
                child = group[1].iloc[0][item["column"]]
            else:
                child = item["value"]


        elif item["type"] == "object":
            level += 1
            temp_items[level] = {}
            for sub_item in item["shape"]:
                process_items(group[1], sub_item, level)
            level -= 1
            child = temp_items[level+1]


        elif item["type"] == "array":
            level += 1
            temp_items[level] = []
            for sub_item in item["shape"]:
                process_items(group[1], sub_item, level)
            level -= 1
            child = temp_items[level+1]


        if "func" in item:
            f = eval(item["func"])
            child = f(child, scope.iloc[0], parent)

        attach_item(item, child, parent)    

    scope = df

def attach_item(item, child, parent):
    if type(parent) == dict:
        parent[item["name"]] = child
    else:
        parent.append(child)

if __name__ == "__main__":
    main()
