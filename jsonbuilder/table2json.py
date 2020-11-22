from jsonbuilder import Tree
import argparse
import time
import json
import logging

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--format")
parser.add_argument("-t", "--table")
parser.add_argument("-o", "--output")
parser.add_argument("-d", "--date")
parser.add_argument("-i", "--inspect_row", type=int)
parser.add_argument("-n", "--native_eval", action="store_true")
parser.add_argument("-p", "--profiler", action="store_true")
parser.add_argument("-v", "--verbose", action="store_true")
args = parser.parse_args()

"""
Run this script from command line in the project root dir:
python scripts/table2json.py -t tests/testdata/test.csv -f tests/testdata/format2.json -v
"""


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            # logging.FileHandler("debug.log"),
            logging.StreamHandler()
        ],
    )
    logging.info("Process started")

    with open(args.format) as f:
        fmt = json.load(f)

    jbTree = Tree(
        fmt,
        args.table,
        date=args.date,
        inspect_row=args.inspect_row,
        use_native_eval=args.native_eval,
    )

    if args.verbose:
        for df in jbTree.intermediate_dfs:
            print("\n\n", df)

    output_json = jbTree.build().toJson(indent=2)

    if args.output:
        with open(args.output, "w") as f:
            f.write(output_json)

    if args.verbose:
        if len(output_json) > 100000:
            print("Output JSON is too large to print...")
        else:
            print(output_json)

    logging.info("Process completed")
    logging.info("Elapsed time: " + str(time.process_time()) + " seconds")


if __name__ == "__main__":
    if args.profiler:
        import io
        import pstats
        import cProfile
        import tabulate

        pr = cProfile.Profile()
        pr.run("main()")
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats(1).print_stats(30)
        rows = [x.split(maxsplit=5) for x in s.getvalue().split("\n")]
        print("\nProfiler Results:\n")
        print(tabulate.tabulate(rows[5:-3], headers="firstrow"))
    else:
        main()
