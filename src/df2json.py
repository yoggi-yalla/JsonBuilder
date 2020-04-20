import pandas as pd
import argparse
import time
import json
import orjson
import JsonBuilder

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--format')
parser.add_argument('-t', '--table')
parser.add_argument('-o', '--output', nargs='?')
parser.add_argument('-s', '--silent', dest='silent', action='store_true')
args = parser.parse_args()

'''
Run the script in command line from this working dir:
python df2json.py -t ../test/test.csv -f ../test/format3.json
'''

def main(): 
    with open(args.format) as f:
        format = json.load(f)

    functions = generate_functions(format.get('functions',[]))

    df = pd.read_csv(args.table)
    df = apply_transforms(df, functions, format.get('col_transforms',[]))

    mapping = format.get('mapping')

    
    jb = JsonBuilder.Node(mapping, df, functions)

    name, value = jb.build()
    
    output_binary = orjson.dumps(
                value, 
                option=orjson.OPT_INDENT_2|orjson.OPT_NON_STR_KEYS,
                default=lambda x:None
                )

    if not args.silent:
        if args.output:
            with open(args.output, 'wb') as f:
                f.write(output_binary)
        else:
            print(df)
            print(output_binary.decode('UTF8'))

    print(time.process_time())


def generate_functions(funcs):
    functions = {}
    for f in funcs:
        exec(f)
        f_name = f.split(' ')[1].split('(')[0]
        functions[f_name] = eval(f_name)
    return functions

def apply_transforms(df, functions, column_transforms):
    for t in column_transforms:
        if t['func'].startswith('def'): 
            f = functions[t['func']]
        else: 
            f = eval(t['func'])
        col = t.get('col')
        df[col] = f(df,col)
    return df

if __name__ == '__main__':
    main()

    