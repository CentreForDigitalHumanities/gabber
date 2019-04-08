#!/usr/bin/python3

import sys
import getopt
import csv
import re
from pymongo import *

#
# this tool counts activity per month from the mongodb and writes this to csv.
# it is part of the Gabber toolset.
# see https://github.com/nomennesc-io/gabber
#

def showhelp():

  # a generic function to print how to use this program

  print("usage: gabactivity.py [-h] [-o <filename>]")
  print("")
  print("gabhashtags.py collects hashtags from scraped posts and comments.")
  print("See https://github.com/nomennesc-io/gabber")
  print("")
  print("arguments:")
  print("  -h             show this help message")
  print("  -o <filename>  the output filename, default is gabhashtags.csv")
  print("")


def main(argv):
  global allhashtags

  # getting command line parameters

  outfile = "gabactivity.csv"

  try:
    opts, args = getopt.getopt(argv,"beho:")
  except getopt.GetoptError:
    showhelp()
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
      showhelp()
      sys.exit()
    if opt in ('-o'):
      outfile = arg

  # setting up the database

  db = MongoClient().gab

  # prepare the output csv file

  with open(outfile,'w',newline='',encoding='utf8') as csvfile:
    out = csv.writer(csvfile, delimiter=',', quotechar="'", quoting=csv.QUOTE_ALL)

    out.writerow(['month','usercount','postcount','repostcount','commentcount'])

    print("counting posts...")
    allposts = {}
    for post in db.posts.find({'type':'post'},{'published_at':1,'actuser.id':1}):
      month = post['published_at'][0:7]
      userid = post['actuser']['id']

      if month in allposts:
        allposts[month].append(userid)
      else:
        allposts[month] = [userid]

    print("counting reposts...")
    allreposts = {}
    for repost in db.posts.find({'type':'repost'},{'published_at':1,'actuser.id':1}):
      month = repost['published_at'][0:7]
      userid = repost['actuser']['id']

      if month in allreposts:
        allreposts[month].append(userid)
      else:
        allreposts[month] = [userid]
    
    print("counting comments...")
    allcomments = {}
    for comment in db.comments.find({},{'created_at':1,'user.id':1}):
      month = comment['created_at'][0:7]
      userid = comment['user']['id']

      if month in allcomments:
        allcomments[month].append(userid)
      else:
        allcomments[month] = [userid]

    for month in sorted(allposts):
      postusers = allposts[month]
      postcount = len(postusers)
      if month in allreposts:
        repostusers = allreposts[month]
      else:
        repostusers = []
      repostcount = len(repostusers)
      if month in allcomments:
        commentusers = allcomments[month]
      else:
        commentusers = []
      commentcount = len(commentusers)
      allusers = postusers + repostusers + commentusers
      usercount = len(set(allusers))

      out.writerow([month,usercount,postcount,repostcount,commentcount])

  print("done!")


if __name__ == "__main__":
   main(sys.argv[1:])
