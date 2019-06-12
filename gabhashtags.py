#!/usr/bin/python3

import sys
import re
import argparse
import csv
from pymongo import *

#
# this tool exports hashtags from the mongodb to csv.
# it is part of the Gabber toolset.
# see https://github.com/utrecht-data-school/gabber
#

def extracthashtags(message):
  global allhashtags

  # use a regular expression to get all hashtags in the message

  hashtags = re.findall(r'#[^\s,\.\)\?\!#@\':=\-]+', message)

  # loop over each hashtag found in the message

  for htag in hashtags:

    # convert it to lowercase

    h = htag.lower()

    # if we already had this hashtag in the collection, increase the count
    # else, add it to the collection with count = 1

    if h in allhashtags:
      allhashtags[h] += 1
    else:
      allhashtags[h] = 1

  return True


def main(argv):
  global allhashtags

  # getting command line parameters

  parser = argparse.ArgumentParser()

  parser.add_argument('-o','--output', dest='output', help='Set the output csv file', type=str, required=False, nargs='?', default='gabhashtags.csv')
  args = parser.parse_args()

  outfile = args.output

  # setting up the database

  db = MongoClient().gab

  # set up the dictionary where we'll count the hashtags

  allhashtags = {}

  # prepare the output csv file

  with open(outfile,'w',newline='',encoding='utf8') as csvfile:
    out = csv.writer(csvfile, delimiter=',', quotechar="'", quoting=csv.QUOTE_ALL)
  
    # loop over all the posts and extract hashtags
    
    print("extracting hashtags from posts...")

    for post in db.posts.find({}, no_cursor_timeout=True):
      extracthashtags(post['post']['body'])

    # loop over all the comments and extract hashtags

    print("extracting hashtags from comments...")

    for comment in db.comments.find({}, no_cursor_timeout=True):
      extracthashtags(comment['body'])

    # sort all the hashtags we found

    print("sorting...")

    sortedhashtags = sorted(allhashtags.items(), key=lambda kv: kv[1], reverse = True)

    # and write them all to csv

    print("writing to file: " + outfile)

    for h in sortedhashtags:
      out.writerow([h[0],h[1]])

    print("done!")


if __name__ == "__main__":
   main(sys.argv[1:])
