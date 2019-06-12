#!/usr/bin/python3

import json
import sys
import argparse
from pymongo import *

#
# this tool collects group data from scraped posts from gab.ai. 
# it is part of the Gabber toolset.
# see https://github.com/utrecht-data-school/gabber
#

def main(argv):

  # getting command line parameters

  parser = argparse.ArgumentParser()

  parser.add_argument('-r','--reposts', dest='reposts', help='Include reposts as edges in the graph', action='store_true', required=False, default=False)

  args = parser.parse_args()

  # setting up the database

  db = MongoClient().gab

  # create a fresh new collection for the groups and make sure they are unique

  db.groups.drop()
  db.groups.create_index('id', unique=True)

  # depending on whether we include reposts, either find everything with group metadata, or just the original posts

  if args.reposts:
    groups = db.posts.find({'post.group':{"$exists":1}},{'post.group':1}, no_cursor_timeout=True)
  else:
    groups = db.posts.find({'type':'post','post.group':{"$exists":1}},{'post.group':1}, no_cursor_timeout=True)

  # loop over all the posts with group metadata we found

  for group in groups:
    try:
    
      # try to insert the group, this will fail if the group is already in the collection 

      group['post']['group']['postcount'] = 1
      db.groups.insert_one(group['post']['group'])
    except:
    
      # if the insert failed, the group was already in the collection and we just need to increment the postcount

      db.groups.update({'id':group['post']['group']['id']},{"$inc":{'postcount':1}})


if __name__ == "__main__":
   main(sys.argv[1:])
