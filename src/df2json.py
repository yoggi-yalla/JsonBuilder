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
type
shape
name
value
display
filter
group
func

a type can be an object, array, key, or simple
an array can hold arrays, objects, or simples
an object can hold keys
a key can hold one object, array, or simple
a simple is the lowest form of data and can't hold another type

shape contains all the details of a type

name is the name of a type within an object, i.e. the name of a key

value is the value (string, float, int, bool, or none) of a simple

display might get used in a GUI later on to understand what is what

filter is used to set the scope of the df

group is used to split the scope into groups and build one type for each group

func is used to apply a function to a type after it has been created
'''

output = {}

# mapping and data frame will be the user input 
mapping = {
    "type": "object",
    "shape": [
        {
            "type": "key",
            "name": "market_data",
            "shape": {
                "type": "array",
                "shape": [
                    {
                        "type": "object",
                        "shape": [
                            {
                                "type": "key",
                                "name": "action",
                                "shape": {
                                    "type": "simple",
                                    "value": "ADD_IR"
                                }
                            },
                            {
                                "type": "key",
                                "name": "name",
                                "shape": {
                                    "type": "simple",
                                    "value": "EUR_OIS"
                                }
                            },
                            {
                                "type": "key",
                                "name": "points",
                                "shape": {
                                    "type": "array",
                                    "shape": [
                                        {
                                            "type": "object",
                                            "shape": [
                                                {
                                                    "type": "key",
                                                    "name": "date",
                                                    "shape": {
                                                        "type": "simple",
                                                        "value": "2019-05-18"
                                                    }
                                                },
                                                {
                                                    "type": "key",
                                                    "name": "df",
                                                    "shape": {
                                                        "type": "simple",
                                                        "value": "0.99"
                                                    }
                                                }
                                            ]
                                        },
                                        {
                                            "type": "object",
                                            "shape": [
                                                {
                                                    "type": "key",
                                                    "name": "date",
                                                    "shape": {
                                                        "type": "simple",
                                                        "value": "2020-05-18"
                                                    }
                                                },
                                                {
                                                    "type": "key",
                                                    "name": "df",
                                                    "shape": {
                                                        "type": "simple",
                                                        "value": "0.95"
                                                    }
                                                }
                                            ]
                                        }   
                                    ]
                                }
                            }
                        ]
                    },
                    {
                        "type": "object",
                        "shape": [
                            {
                                "type": "key",
                                "name": "action",
                                "shape": {
                                    "type": "simple",
                                    "value": "ADD_IR"
                                }
                            },
                            {
                                "type": "key",
                                "name": "name",
                                "shape": {
                                    "type": "simple",
                                    "value": "EUR_OIS"
                                }
                            },
                            {
                                "type": "key",
                                "name": "points",
                                "shape": {
                                    "type": "array",
                                    "shape": [
                                        {
                                            "type": "object",
                                            "shape": [
                                                {
                                                    "type": "key",
                                                    "name": "date",
                                                    "shape": {
                                                        "type": "simple",
                                                        "value": "2019-05-18"
                                                    }
                                                },
                                                {
                                                    "type": "key",
                                                    "name": "df",
                                                    "shape": {
                                                        "type": "simple",
                                                        "value": "0.99"
                                                    }
                                                }
                                            ]
                                        },
                                        {
                                            "type": "object",
                                            "shape": [
                                                {
                                                    "type": "key",
                                                    "name": "date",
                                                    "shape": {
                                                        "type": "simple",
                                                        "value": "2020-05-18"
                                                    }
                                                },
                                                {
                                                    "type": "key",
                                                    "name": "df",
                                                    "shape": {
                                                        "type": "simple",
                                                        "value": "0.95"
                                                    }
                                                }
                                            ]
                                        }   
                                    ]
                                }
                            }
                        ]
                    }
                ]
            }
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
    
    elif item["type"] == "key":
        return process_item(df, item["shape"], level)

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