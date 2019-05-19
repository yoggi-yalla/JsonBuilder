import pandas as pd
import argparse
import os
import pprint
import json

parser = argparse.ArgumentParser()
#parser.add_argument('-df')
parser.add_argument('-ruleset', nargs='?')
parser.parse_args()
args = parser.parse_args()

dataFrame = pd.read_excel('test.xlsx')

pp = pprint.PrettyPrinter(indent=4)

def main():

    print(dataFrame)
    outputJson = initializeOutput()
    print(json.dumps(outputJson, indent=4, sort_keys=True))

def initializeOutput():
    newJson = json.load('test.json')
    return newJson

if __name__ == "__main__":
    main()