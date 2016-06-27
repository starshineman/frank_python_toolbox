__author__ = 'frank1'
import argparse
import sys

# http://www.2cto.com/kf/201504/387122.html
def main():
    parser = argparse.ArgumentParser(description='test parsing arguments')

    parser.add_argument('pos1', nargs='*')
    parser.add_argument('pos2')
    parser.add_argument('-o1')
    parser.add_argument('-o2')
    parser.add_argument('pos3', nargs='*')

    print sys.argv
    # arg = parser.parse_args(sys.argv[1:])
    arg = parser.parse_known_args(sys.argv[1:])
    print arg

    # print parser.print_help()



if __name__ == "__main__":
   # get_opt()
   main()