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
type
shape
name
value
display
filter
group
func

an item is the highest form of abstraction, each item has a type

a type can be an object, array, or simple
an object can hold name:item pairs
an array can hold items
a simple is the lowest form of data and can't hold another item

shape contains all the items that are held by an item

name is the name of an item within an object

value is the value (string, float, int, bool, or none) of a simple

display might get used in a GUI later on to understand what is what

filter is used to set the scope of the df

group is used to split the scope into groups and build one type for each group

func is used to apply a function to a type after it has been created
'''

output = {}

# mapping and data frame will be the user input 
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

    print('\nFull dataframe:')
    print(df)
    print('')
    
    # df.loc is used to set the scope of the dataframe
    print('Filtered for EUR_OIS:')
    df = df.loc[df.name == 'EUR_OIS']
    print(df)
    print('')

    level = 0
    output = process_item(df, mapping, level)
    print(json.dumps(output, indent=4))

def process_item(df, item, level):
    
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
