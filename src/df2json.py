import pandas as pd
import argparse
import json

parser = argparse.ArgumentParser()
# parser.add_argument('-df')
parser.add_argument('-mapping', nargs='?')
args = parser.parse_args()



'''
item
type        = mandatory for all items
name        = mandatory for items within an object
shape       = mandatory for objects and arrays
value       = mandatory for simple
display     = optional
scope       = optional
group       = optional
func        = optional

an item is the highest form of abstraction, each item has a type

type can be array, object, or simple
an object can hold items with names
an array can hold items without names
a simple can't hold another item

shape contains all the items that are held by the parent item

value is the value (string, float, int, True, False, or None) of a simple

display will be used in a GUI to help navigate/build the mapping

scope is used to set the scope of the dataframe

group is used to split the scope into groups and build one item for each group

every row of the current scope is a group by default

func will be used to apply a function to an item after it has been created
'''

# mapping and data frame will be the input 
# validate that mapping["type"] is in ["object", "array"]
# validate that mapping["group"] == True
# validate that mapping["name"] == "root"

mapping = {  
    "type":"object",
    "name": "root",
    "group": True,
    "shape":[  
        {  
            "type":"array",
            "name":"market_data",
            "group": True,
            "shape":[  
                {  
                    "type":"object",
                    "group": "name",
                    "shape":[  
                        {  
                            "type":"simple",
                            "name":"action",
                            "value":"ADD_IR"
                        },
                        {  
                            "type":"simple",
                            "name":"name",
                            "column":"name"
                        },
                        {  
                            "type":"array",
                            "name":"points",
                            "group": True,
                            "shape":[
                                {  
                                    "type":"object",
                                    "shape":[  
                                        {  
                                            "type":"simple",
                                            "column": "date",
                                            "name":"date"
                                        },
                                        {  
                                            "type":"simple",
                                            "column": "discount_factor",
                                            "name":"df"
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]
}

temp_items = {}
temp_types = {}
temp_iterators = {}
level = 0

if mapping["type"] == "array":
    temp_items[0] = []
    temp_types[0] = "array"
else:
    temp_items[0] = {}
    temp_items[0]["root"] = {}
    temp_types[0] = "object"


def main():

    df = pd.read_csv('../test/test.txt', na_filter=False).astype(str)
    scope = df

    print('\n Full dataframe:')
    print(df)
    print('')

    process_items(df, mapping, level)

    if temp_types[0] == "object":
        output = temp_items[0]["root"]
    else:
        if len(temp_items[0]) > 0:
            output = temp_items[0][0]
        else:
            output = temp_items[0]

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
        parent_type = temp_types[level]

        if item["type"] == "simple":
            if "column" in item:
                child = group[1].iloc[0][item["column"]]
            else:
                child = item["value"]


        elif item["type"] == "object":
            level += 1
            temp_items[level] = {}
            temp_types[level] = "object"
            for sub_item in item["shape"]:
                process_items(group[1], sub_item, level)
            level -= 1
            child = temp_items[level+1]


        elif item["type"] == "array":
            level += 1
            temp_items[level] = []
            temp_types[level] = "array"
            for sub_item in item["shape"]:
                process_items(group[1], sub_item, level)
            level -= 1
            child = temp_items[level+1]

        if "func" in item:
            f = eval(item["func"])
            child = f(child, scope.iloc[0])

        attach_item(item, child, parent)    

    scope = df

def attach_item(item, child, parent):
    if type(parent) == dict:
        parent[item["name"]] = child
    else:
        parent.append(child)

if __name__ == "__main__":
    main()
