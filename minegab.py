#!/usr/bin/python3

import requests
import json
import sys
import getopt
import time
import pymongo
from pymongo import *
import codecs
#sys.stdout = codecs.getwriter('utf8')(sys.stdout)
#sys.stderr = codecs.getwriter('utf8')(sys.stderr)

class Gabber:
  def __init__(self,id,username):
    self.id = id
    self.username = username


def dbgmsg(e):
  global debug
  if debug:
    print(e)


def getfromgab(url):
  global debug
  failcount = 0
  while failcount < 15:
    dbgmsg("retrieving " + url)
    try:
      r = requests.get(url)
      if r.status_code == 200:
        try:
          ret = json.loads(r.text)
        except JSONDecodeError as e:
          dbgmsg("failed to decode json, giving up on " + url)
          return False
        return ret
      elif r.status_code == 429:
        dbgmsg("hit the rate limiting, waiting 5 seconds...")
        failcount += 1
        time.sleep(5)
      elif r.status_code == 400:
        dbgmsg("got 400, that was most likely a private account, giving up on " + url) 
        return False
      else:
        dbgmsg("got " + str(r.status_code) + ", will keep trying a few times...")
        failcount += 1
        time.sleep(1)
    except:
      e = sys.exc_info()[0]
      dbgmsg("request failed with: " + str(e) + ", will keep trying a few times...")
      failcount += 1
      time.sleep(1)
  dbgmsg("too many errors, giving up on " + url)
  return False


def getprofile(username):
  global db
  userurl = 'https://gab.ai/users/' + username
  profile = getfromgab(userurl)
  if profile:
    gabber = Gabber(id = profile['id'], username = profile['username'])
    try:
      db.profiles.insert_one(profile)
    except pymongo.errors.WriteError as e:
      dbgmsg("skipping " + username + " - already mined")
      return False
    return gabber
  else:
    print("FAILURE: failed to scrape profile for " + username)
    return False


def getfollowers(username):
  global db
  followers = []
  length = 1
  before = 0
  while length > 0:
    followerurl = 'https://gab.ai/users/' + username + '/followers?before=' + str(before)
    followerset = getfromgab(followerurl)
    if followerset:
      length = len(followerset['data'])
      before += length
      for follower in followerset['data']:
        gabber = Gabber(id = follower['id'], username = follower['username'])
        try:
          db.discovered.insert_one(follower)
        except pymongo.errors.WriteError as e:
          dbgmsg("error adding discovered follower account " + follower['username'] + " : " + str(e))
        followers.append(gabber)
    else:
      print("FAILURE: could not mine followers for " + username)
      return False
  return followers


def getfollowing(username):
  global db
  following = []
  length = 1
  before = 0
  while length > 0:
    followingurl = 'https://gab.ai/users/' + username + '/following?before=' + str(before)
    followingset = getfromgab(followingurl)
    if followingset:
      length = len(followingset['data'])
      before += length
      for follows in followingset['data']:
        gabber = Gabber(id = follows['id'], username = follows['username'])
        try:
          db.discovered.insert_one(follows)
        except pymongo.errors.WriteError as e:
          dbgmsg("error adding discovered following account " + follows['username'] + " : " + str(e))
        following.append(gabber)
    else:
      print("FAILURE: could not mine following for " + username)
      return False
  return following


def gettimeline(username):
  global db
  before = False
  nomore = False
  while nomore == False:
    feedurl = 'https://gab.ai/api/feed/' + username
    if before:
      feedurl = 'https://gab.ai/api/feed/' + username + '?before=' + before
    contentset = getfromgab(feedurl)
    if contentset:
      nomore = contentset['no-more']
      if len(contentset['data']) == 0:
        nomore = True
      for content in contentset['data']:
        try:
          db.posts.insert_one(content)
          if content['post']['reply_count'] > 0:
            getcomments(content['post']['id'])
        except pymongo.errors.WriteError as e:
          dbgmsg("error storing post " + str(content['post']['id']) + " backend returned: " + str(e))
        if content['post']['is_quote'] and 'parent' in content['post']:
          try:
            db.discovered.insert_one(content['post']['parent']['user'])
          except pymongo.errors.WriteError as e:
            dbgmsg("error adding discovered quoting account " + content['post']['parent']['user']['username'] + " backend returned: " + str(e))
        before = content['published_at']
    else:
      print("FAILURE: failed to scrape timeline for " + username)
      return False
  return True


def getcomments(post):
  global db
  commentsurl = 'https://gab.ai/posts/' + str(post) + '/comments/index?limit=1000'
  done = False
  comments = getfromgab(commentsurl)
  if comments:
    for comment in comments['data']:
      try:
        db.comments.insert_one(comment)
        try:
          db.discovered.insert_one(comment['user'])
        except pymongo.errors.WriteError as e:
          dbgmsg("error adding discovered commenting account " + comment['user']['username'] + " backend returned: " + str(e))
        if comment['reply_count'] > 0:
          getcomments(comment['id'])
      except pymongo.errors.WriteError as e:
        dbgmsg("error adding comment " + str(comment['id']) + " backend returned: " + str(e))
    return True
  else:
    dbgmsg("failed to scrape comments from " + commentsurl)
    return False
  return True


def minegabber(username):
  global db
  gabber = getprofile(username)
  if gabber:
    print("now scraping " + username)
    gettimeline(username)
    followers = getfollowers(username)
    following = getfollowing(username)
    if followers:
      for follower in followers:
        edge = { "follower_id": follower.id, "follower_username": follower.username, "following_id": gabber.id, "following_username": gabber.username }
        try:
          db.followers.insert_one(edge)
        except pymongo.errors.WriteError as e:
          dbgmsg("error adding follower edge, backend gave us: " + str(e))
    if following:
      for follower in following:
        edge = { "follower_id": gabber.id, "follower_username": gabber.username, "following_id": follower.id, "following_username": follower.username }
        try:
          db.followers.insert_one(edge)
        except pymongo.errors.WriteError as e:
          dbgmsg("error adding follower edge, backend gave us: " + str(e))


def getall():
  global db
  for discovered in db.discovered.find({},{'id':1,'username':1}):
    if db.profiles.find_one({'id':discovered['id']}) is None:
      minegabber(discovered['username'])


def main(argv):
  global db
  global debug
  db = MongoClient().gab
  db.profiles.create_index('id', unique=True)
  db.discovered.create_index('id', unique=True)
  db.followers.create_index([('follower_id',1),('following_id',1)], unique=True)
  db.posts.create_index([('post.actuser.id',1),('post.id',1),('post.type',1)], unique=True)
  db.comments.create_index([('id',1),('user.id',1)], unique=True)
  shouldgetall = False
  username = False
  debug = False
  try:
    opts, args = getopt.getopt(argv,"hadu:")
  except getopt.GetoptError:
    print('minegab.py [ -adh ] [ -u <username> ]')
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
      print('minegab.py [ -adh ] [ -u <username> ]')
      sys.exit()
    if opt == '-a':
      shouldgetall = True
    if opt == '-d':
      debug = True
    if opt in ("-u"):
      username = arg
  if(username):
    minegabber(username)
  if(shouldgetall):
    getall()

if __name__ == "__main__":
   main(sys.argv[1:])
