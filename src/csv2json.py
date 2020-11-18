import jsonbuilder
import argparse
import time
import json
import logging
import sys
sys.stdout.reconfigure(encoding='utf-8')

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--format')
parser.add_argument('-t', '--table')
parser.add_argument('-o', '--output')
parser.add_argument('-d', '--date')
parser.add_argument('-i', '--inspect_row', type=int)
parser.add_argument('-n', '--native_eval', action='store_true')
parser.add_argument('-p', '--profiler', action='store_true')
args = parser.parse_args()

'''
Run the script in command line from this working dir:
python csv2json.py -t ../testdata/test.csv -f ../testdata/format2.json
'''


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            # logging.FileHandler("debug.log"),
            logging.StreamHandler()
        ]
    )
    logging.info("Process started")

    with open(args.format) as f:
        fmt = json.load(f)

    jbTree = jsonbuilder.Tree(fmt, args.table, date=args.date,
                              inspect_row=args.inspect_row,
                              use_native_eval=args.native_eval)

    for df in jbTree.intermediate_dfs:
        print("\n\n", df)

    output_json = jbTree.build().toJson(indent=2)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(output_json)
    else:
        if len(output_json) < 10000:
            print(output_json)

    logging.info("Process completed")
    logging.info("Elapsed time: " + str(time.process_time()) + " seconds")


if __name__ == '__main__':
    if args.profiler:
        import io
        import pstats
        import cProfile
        import tabulate
        pr = cProfile.Profile()
        pr.run('main()')
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats(1).print_stats(30)
        rows = [x.split(maxsplit=5) for x in s.getvalue().split("\n")]
        print('\nProfiler Results:\n')
        print(tabulate.tabulate(rows[5:-3], headers='firstrow'))
    else:
        main()
