import pandas as pd
import argparse
import os
import json

parser = argparse.ArgumentParser()
# parser.add_argument('-df')
parser.add_argument('-instructions', nargs='?')
args = parser.parse_args()

df = pd.read_excel('test.xlsx').astype(str)

def main():

    print(df)
    
    # print(dataFrame.groupby(['name'], as_index=True).apply(lambda x: x.to_dict('r')).to_json())

    with open('test.json') as f:
        targetJson = json.load(f)

    print('\n' + 'target:')
    print(json.dumps(targetJson, indent=2, sort_keys=True))

if __name__ == "__main__":
    main()