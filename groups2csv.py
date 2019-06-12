#!/usr/bin/python3

import json
import sys
import argparse
import csv
from pymongo import *

#
# this tool exports group data from the mongodb to csv.
# make sure to run gabgroups.py first!
# it is part of the Gabber toolset.
# see https://github.com/utrecht-data-school/gabber
#

def main(argv):

  # getting command line parameters

  parser = argparse.ArgumentParser()

  parser.add_argument('-o','--output', dest='output', help='Set the output csv file', type=str, required=False, nargs='?', default='gabgroups.csv')
  args = parser.parse_args()

  outfile = args.output

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
