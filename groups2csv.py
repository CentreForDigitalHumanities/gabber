#!/usr/bin/python3

import json
import sys
import getopt
import csv
from pymongo import *

#
# this tool exports group data from the mongodb to csv.
# make sure to run gabgroups.py first!
# it is part of the Gabber toolset.
# see https://github.com/nomennesc-io/gabber
#

def showhelp():

  # a generic function to print how to use this program

  print("usage: groups2csv.py [-h] [-o <filename>]")
  print("")
  print("groups2csv.py collects group data from scraped posts.")
  print("make sure to run gabgroups.py before exporting.")
  print("See https://github.com/nomennesc-io/gabber")
  print("")
  print("arguments:")
  print("  -h             show this help message")
  print("  -o <filename>  the output filename, default is gabgroups.csv")
  print("")


def main(argv):

  # getting command line parameters

  outfile = "gabgroups.csv"

  try:
    opts, args = getopt.getopt(argv,"rh")
  except getopt.GetoptError:
    showhelp()
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
      showhelp()
      sys.exit()
    if opt == '-o':
      outfile = arg

  # setting up the database

  db = MongoClient().gab

  # open the output file

  with open(outfile,'w',newline='',encoding='utf8') as csvfile:
    out = csv.writer(csvfile, delimiter=',', quotechar="'", quoting=csv.QUOTE_ALL)

    print('exporting to ' + outfile)
    print('format is: comma separated, single quote delimited')

    # fill the first row with descriptions

    out.writerow(['id','postcount','title','description','cover url','pinned post id'])

    # loop over all the groups and write them to csv

    for group in db.groups.find():
      out.writerow([group['id'],group['postcount'],group['title'],group['description'],group['cover_url'],group['pinned_post_id']])


if __name__ == "__main__":
   main(sys.argv[1:])
