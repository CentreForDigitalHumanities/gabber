#!/usr/bin/python3

import sys
from pymongo import *

#
# this tool adds community metadata to all scraped 
# posts and comments in mongodb
# make sure to run gabcommunities.py first!
# it is part of the Gabber toolset.
# see https://github.com/utrecht-data-school/gabber
#

def main(argv):
  global db

  # setting up the database

  db = MongoClient().gab

  print("enriching the posts...")

  # looping over all the posts
  for post in db.posts.find({}, no_cursor_timeout=True):
    postid = post['_id']
    uid = post['actuser']['id']
    # fetching the profile of the actuser of the post
    user = db.profiles.find_one({'id':uid})
    # did we detect communities for this user?
    if 'communities' in user:
      obj = user['communities']
      # then add them to the actuser of the post!
      db.posts.update_one({'_id':postid},{"$set":{'actuser.communities':obj}})

  print("enriching the comments...")

  # doing the same trick for all the comments
  for comment in db.comments.find({'language':'en'}, no_cursor_timeout=True):
    commentid = comment['_id']
    uid = comment['user']['id']
    user = db.profiles.find_one({'id':uid})
    if user:
      if 'communities' in user:
        obj = user['communities']
        db.comments.update_one({'_id':commentid},{"$set":{'user.communities':obj}})

  print("done!")


if __name__ == "__main__":
   main(sys.argv[1:])
