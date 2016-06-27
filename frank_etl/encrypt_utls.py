#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
import os
import datetime
import time
import re
import subprocess
import shlex
import json





def encrptStr(srcStr):
   import base64

   s1 = base64.encodestring(srcStr)
   s2 = base64.decodestring(s1)
   print s1,s2
   return s1

if __name__ == "__main__":

    srcStr = sys.argv[1]
    destStr = encrptStr(srcStr)
    print "src:%s,dest:%s"%(srcStr,destStr)
