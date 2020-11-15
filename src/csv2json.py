import jsonbuilder
import argparse
import time
import json

run_profiler = 0

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
    if run_profiler:
        import io, pstats, cProfile, tabulate
        pr = cProfile.Profile()
        pr.enable()
        main()
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats(1).print_stats(30)
        rows = s.getvalue().split("\n")[5:-3]
        split_rows = []
        for row in rows:
            split = row.split()
            s_out = split[:5]
            s_out.append(" ".join([x for x in split[5:]]))
            split_rows.append(s_out)
        print(tabulate.tabulate(split_rows))
    else:
        main()