#!/usr/bin/python3

import sys
import argparse
import csv
import re
from pymongo import *

#
# this tool counts activity per month from the mongodb and writes this to csv.
# it is part of the Gabber toolset.
# see https://github.com/utrecht-data-school/gabber
#

def main(argv):
  global allhashtags

  # getting command line parameters

  parser = argparse.ArgumentParser()

  parser.add_argument('-o','--output', dest='output', help='Set the output csv file', type=str, required=False, nargs='?', default='gabactivity.csv')
  args = parser.parse_args()

  outfile = args.output

  # setting up the database

  db = MongoClient().gab

  # prepare the output csv file

  with open(outfile,'w',newline='',encoding='utf8') as csvfile:
    out = csv.writer(csvfile, delimiter=',', quotechar="'", quoting=csv.QUOTE_ALL)

    # write the first row defining the columns
    out.writerow(['month','usercount','postcount','repostcount','commentcount'])

    print("counting posts...")

    # initialise allposts, where we'll collect all userid's per month
    allposts = {}

    # loop over all the posts
    for post in db.posts.find({'type':'post'},{'published_at':1,'actuser.id':1}):
      # the first 8 characters of published_at are the year and month
      month = post['published_at'][0:7]
      userid = post['actuser']['id']

      # add the userid of this post to the appropriate month in allposts
      if month in allposts:
        allposts[month].append(userid)
      else:
        allposts[month] = [userid]

    print("counting reposts...")
    # repeating the same trick for reposts
    allreposts = {}
    for repost in db.posts.find({'type':'repost'},{'published_at':1,'actuser.id':1}):
      month = repost['published_at'][0:7]
      userid = repost['actuser']['id']

      if month in allreposts:
        allreposts[month].append(userid)
      else:
        allreposts[month] = [userid]
    
    print("counting comments...")
    # and for comments
    allcomments = {}
    for comment in db.comments.find({},{'created_at':1,'user.id':1}):
      month = comment['created_at'][0:7]
      userid = comment['user']['id']

      if month in allcomments:
        allcomments[month].append(userid)
      else:
        allcomments[month] = [userid]

    # loop over every month in allposts
    for month in sorted(allposts):
      # get the list of userid's from all posts that month
      postusers = allposts[month]
      postcount = len(postusers)
      # same for reposts and comments
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
      # add the posts, reposts, and comments together to get a sum of all activity
      allusers = postusers + repostusers + commentusers
      # reduce it to a set to get the unique userid's with activity this month
      usercount = len(set(allusers))

      # write our findings to the csv
      out.writerow([month,usercount,postcount,repostcount,commentcount])

  print("done!")


if __name__ == "__main__":
   main(sys.argv[1:])
