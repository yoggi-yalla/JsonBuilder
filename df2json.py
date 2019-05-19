import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-df')
parser.add_argument('-ruleset', nargs='?')
parser.parse_args()
args = parser.parse_args()


def main():
    outputJson = initializeOutput()
    print(outputJson)

def initializeOutput():
    newJson = {
        'trades':'',
        'csa_agreements':'',
        'market_data':'',
        'fixings':'',
        'credit_curves':'',
        'funding_curves':''
    }
    return newJson

if __name__ == "__main__":
    main()