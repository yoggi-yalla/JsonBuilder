import JsonBuilder
import argparse
import time
import json
import orjson

import cProfile
import pstats
import os

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--format')
parser.add_argument('-t', '--table')
parser.add_argument('-o', '--output')
parser.add_argument('-s', '--silent', dest='silent', action='store_true')
args = parser.parse_args()

'''
Run the script in command line from this working dir:
python csv2json.py -t ../testdata/test.csv -f ../test/format3.json
'''

def main(): 
    with open(args.format) as f:
        format = json.load(f)

    functions = format.get('functions',[])
    transforms = format.get('df_transforms', [])
    mapping = format.get('mapping')

    output_native = JsonBuilder.parse_mapping(mapping)          \
                               .add_functions(functions)        \
                               .load_csv(args.table)            \
                               .apply_transforms(transforms)    \
                               .build()                         \
                               .value

    output_binary = orjson.dumps(output_native,
                                 option=orjson.OPT_INDENT_2|
                                 orjson.OPT_NON_STR_KEYS,
                                 default=lambda x:None)

    if not args.silent:
        if args.output:
            with open(args.output, 'wb') as f:
                f.write(output_binary)
        else:
            print(output_binary.decode('UTF8'))

    print(time.process_time())

if __name__ == '__main__':
    main()
    #cProfile.run('main()', 'tmp')
    #pstats.Stats('tmp').sort_stats('time').print_stats(20)
    #os.remove('tmp')