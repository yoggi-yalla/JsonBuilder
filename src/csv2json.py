import jsonbuilder
import argparse
import time
import json

#These are only relevant for profiling
import cProfile
import pstats
import os
import sys

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--format')
parser.add_argument('-t', '--table')
parser.add_argument('-o', '--output')
args = parser.parse_args()

'''
Run the script in command line from this working dir:
python csv2json.py -t ../testdata/test.csv -f ../testdata/format2.json
'''

def main(): 
    with open(args.format) as f:
        fmt = json.load(f)

    jbTree = jsonbuilder.Tree(fmt, args.table)
    
    for df in jbTree.intermediate_dfs:
        print(df)
    
    output_json = jbTree.build().toJson(indent=2)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(output_json)
    else:
        if len(output_json) < 10000:
            print(output_json)
        else:
            print("Successful, but output is too large to print.")

    print("\nElapsed time:")
    print(time.process_time())

if __name__ == '__main__':
    main()
    #cProfile.run('main()', 'tmp')
    #pstats.Stats('tmp').sort_stats('time').print_stats(20)
    #os.remove('tmp')