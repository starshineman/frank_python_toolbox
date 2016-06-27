#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
import getopt


def get_opt():

    opts, args = getopt.getopt(sys.argv[1:], "hi:o:")
    input_file=""
    output_file=""
    for op, value in opts:
      if op == "-i":
        input_file = value
        print input_file
      elif op == "-o":
        output_file = value
        print output_file
      elif op == "-h":
        print "help!"
        sys.exit()

def get_simple_params():
    print "脚本名：", sys.argv[0]
    for i in range(1, len(sys.argv)):
      print "参数", i, sys.argv[i]


def Usage():
    print 'PyTest.py usage:'
    print '-h,--help: print help message.'
    print '-v, --version: print script version'
    print '-o, --output: input an output verb'
    print '--foo: Test option '
    print '--fre: another test option'
def Version():
    print 'PyTest.py 1.0.0.0.1'
def OutPut(args):
    print 'Hello, %s'%args
def main(argv):
    try:
        opts, args = getopt.getopt(argv[1:], 'hvo:', ['output=', 'foo=', 'fre='])
    except getopt.GetoptError, err:
        print str(err)
        Usage()
        sys.exit(2)
    for o, a in opts:
        if o in ('-h', '--help'):
            Usage()
            sys.exit(1)
        elif o in ('-v', '--version'):
            Version()
            sys.exit(0)
        elif o in ('-o', '--output'):
            OutPut(a)
            sys.exit(0)
        elif o in ('--foo',):
            OutPut(a)
            Foo=a
        elif o in ('--fre',):
            OutPut(a)
            Fre=a
        else:
            print 'unhandled option'
            sys.exit(3)


if __name__ == "__main__":
   # get_opt()
   main(sys.argv)