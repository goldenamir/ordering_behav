################################################################
# Importing required libraries

import argparse
from ordering import orderingFunc, lookupFunc


################################################################
# main function

def main():

    orderingFunc(args.pat)
    lookupFunc()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('-p', '--pat', required=True,
                    help='Path to the file')

    args = ap.parse_args()
    main()
