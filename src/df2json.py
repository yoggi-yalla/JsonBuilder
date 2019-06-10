import pandas as pd
import argparse
import os
import json
import datetime
import pprint

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
an object can hold items with a name
an array can hold items without name
a simple can't hold another item

shape contains all the items that are held by the parent item

value is the value (string, float, int, true, false, or none) of a simple

display will be used in a GUI to help navigate/build the mapping

scope will be used to set the scope of the dataframe

group will be used to split the scope into groups and build one item for each group

(every row of the current scope is a group by default)

func will be used to apply a function to an item after it has been created
'''

output = {}
level = 0

# mapping and data frame will be the input 
mapping = {  
    "type":"object",
    "shape":[  
        {  
            "type":"array",
            "name":"market_data",
            "shape":[  
                {  
                    "type":"object",
                    "shape":[  
                        {  
                            "type":"simple",
                            "name":"action",
                            "value":"ADD_IR"
                        },
                        {  
                            "type":"simple",
                            "name":"name",
                            "value":"USD_OIS"
                        },
                        {  
                            "type":"array",
                            "name":"points",
                            "shape":[  
                                {  
                                    "type":"object",
                                    "shape":[  
                                        {  
                                            "type":"simple",
                                            "name":"date",
                                            "value":"2019-05-18"
                                        },
                                        {  
                                            "type":"simple",
                                            "name":"df",
                                            "value":"0.99"
                                        }
                                    ]
                                },
                                {  
                                    "type":"object",
                                    "shape":[  
                                        {  
                                            "type":"simple",
                                            "name":"date",
                                            "value":"2020-05-18"
                                        },
                                        {  
                                            "type":"simple",
                                            "name":"df",
                                            "value":"0.95"
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                },
                {  
                    "type":"object",
                    "shape":[  
                        {  
                            "type":"simple",
                            "name":"action",
                            "value":"ADD_IR"
                        },
                        {  
                            "type":"simple",
                            "name":"name",
                            "value":"EUR_OIS"
                        },
                        {  
                            "type":"array",
                            "name":"points",
                            "shape":[  
                                {  
                                    "type":"object",
                                    "shape":[  
                                        {  
                                            "type":"simple",
                                            "name":"date",
                                            "value":"2019-05-18"
                                        },
                                        {  
                                            "type":"simple",
                                            "name":"df",
                                            "value":"0.99"
                                        }
                                    ]
                                },
                                {  
                                    "type":"object",
                                    "shape":[  
                                        {  
                                            "type":"simple",
                                            "name":"date",
                                            "value":"2020-05-18"
                                        },
                                        {  
                                            "type":"simple",
                                            "name":"df",
                                            "value":"0.95"
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


def main():

    df = pd.read_csv('../test/test.txt', na_filter=False).astype(str)
    scope = df

    print('\n Full dataframe:')
    print(df)
    print('')

    output = process_item(df, mapping, level)
    print(json.dumps(output, indent=4))

def process_item(scope, item, level):

    if item["type"] == "simple":
        return item["value"]

    elif item["type"] == "object":
        level += 1
        output[level] = {}
        for sub_item in item["shape"]:
            output[level][sub_item["name"]] = process_item(df, sub_item, level)
        level -= 1
        return output[level+1]

    elif item["type"] == "array":
        level += 1
        output[level] = []
        for sub_item in item["shape"]:
            output[level].append(process_item(df, sub_item, level))
        level -= 1
        return output[level+1]


if __name__ == "__main__":
    main()
