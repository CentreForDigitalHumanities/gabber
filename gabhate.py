#!/usr/bin/python3

import sys
from pymongo import *
from hatesonar import Sonar

#
# this tool adds hate & offensive speech indicators 
# to all scraped posts and comments in mongodb
# it is part of the Gabber toolset.
# see https://github.com/utrecht-data-school/gabber
#

def hateometer(msg):
  global sonar

  indication = sonar.ping(text=msg)
  obj = {}
  for speech in indication['classes']:
    if speech['class_name'] == 'hate_speech':
      obj['hate_confidence'] = speech['confidence']
    if speech['class_name'] == 'offensive_language':
      obj['offensive_confidence'] = speech['confidence']
    if speech['class_name'] == 'neither':
      obj['neither'] = speech['confidence']
  obj['indicator'] = indication['top_class']
  return obj


def main(argv):
  global sonar

  # setting up the database

  db = MongoClient().gab

  # initialising the hateometer

  sonar = Sonar()

  # parsing all posts

  print("hateometing the posts...")

  for post in db.posts.find({'post.language':'en'}, no_cursor_timeout=True):
    obj = hateometer(post['post']['body'])
    postid = post['_id']
    db.posts.update_one({'_id':postid},{"$set":{'post.hateometer':obj}})

  print("hateometing the comments...")

  for comment in db.comments.find({'language':'en'}, no_cursor_timeout=True):
    obj = hateometer(comment['body'])
    commentid = comment['_id']
    db.comments.update_one({'_id':commentid},{"$set":{'hateometer':obj}})

  print("done!")


if __name__ == "__main__":
   main(sys.argv[1:])
