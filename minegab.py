#!/usr/bin/python3

import requests
import json
import sys
import getopt
import time
import pymongo
from pymongo import *

class Gabber:
  def __init__(self,id,username):
    self.id = id
    self.username = username


def getprofile(username):
  global db
  global debug
  userurl = 'https://gab.ai/users/' + username
  done = False
  while not done:
    r = requests.get(userurl)
    if r.status_code == 200:
      profile = json.loads(r.text)
      gabber = Gabber(id = profile['id'], username = profile['username'])
      try:
        db.profiles.insert_one(profile)
      except pymongo.errors.WriteError as e:
        print("skipping " + username + " - already mined")
        return False
      done = True
      return gabber
    elif r.status_code == 429:
      if debug:
        print("hit the rate limiting, waiting 5 seconds...")
      time.sleep(5)
    else:
      print("failed to mine " + username + " - call to " + userurl + " returned: " + str(r.status_code))
      done = True
      return False


def getfollowers(username):
  global db
  global debug
  followers = []
  length = 1
  before = 0
  while length > 0:
    followerurl = 'https://gab.ai/users/' + username + '/followers?before=' + str(before)
    r = requests.get(followerurl)
    if r.status_code == 200:
      followerset = json.loads(r.text)
      length = len(followerset['data'])
      before += length
      for follower in followerset['data']:
        gabber = Gabber(id = follower['id'], username = follower['username'])
        try:
          db.discovered.insert_one(follower)
        except pymongo.errors.WriteError as e:
          if debug:
            print("error adding discovered follower account " + follower['username'] + " : " + str(e))
        followers.append(gabber)
    elif r.status_code == 429:
      if debug:
        print("hit the rate limiting, waiting 5 seconds...")
      length = 30
      before -= 30
      time.sleep(5)
    else:
      print("failed to mine followers for " + username + " - call to " + followerurl + " returned: " + str(r.status_code))
      return False
  return followers


def getfollowing(username):
  global db
  global debug
  following = []
  length = 1
  before = 0
  while length > 0:
    followingurl = 'https://gab.ai/users/' + username + '/following?before=' + str(before)
    r = requests.get(followingurl)
    if r.status_code == 200:
      followingset = json.loads(r.text)
      length = len(followingset['data'])
      before += length
      for follows in followingset['data']:
        gabber = Gabber(id = follows['id'], username = follows['username'])
        try:
          db.discovered.insert_one(follows)
        except pymongo.errors.WriteError as e:
          if debug:
            print("error adding discovered following account " + follows['username'] + " : " + str(e))
        following.append(gabber)
    elif r.status_code == 429:
      if debug:
        print("hit the rate limiting, waiting 5 seconds...")
      length = 30
      before -= 30
      time.sleep(5)
    else:
      print("failed to mine followers for " + username + " - call to " + followingurl + " returned: " + str(r.status_code))
      return False
  return following


def gettimeline(username):
  global db
  global debug
  before = False
  nomore = False
  while nomore == False:
    feedurl = 'https://gab.ai/api/feed/' + username
    if before:
      feedurl = 'https://gab.ai/api/feed/' + username + '?before=' + before
    r = requests.get(feedurl)
    if r.status_code == 200:
      contentset = json.loads(r.text)
      nomore = contentset['no-more']
      for content in contentset['data']:
        try:
          db.posts.insert_one(content)
          if content['post']['reply_count'] > 0:
            getcomments(content['post']['id'])
        except pymongo.errors.WriteError as e:
          if debug:
            print("error storing post " + str(content['post']['id']) + " backend returned: " + str(e))
        if content['post']['is_quote'] and 'parent' in content['post']:
          try:
            db.discovered.insert_one(content['post']['parent']['user'])
          except pymongo.errors.WriteError as e:
            if debug:
              print("error adding discovered quoting account " + content['post']['parent']['user']['username'] + " backend returned: " + str(e))
        before = content['published_at']
    elif r.status_code == 429:
      if debug:
        print("hit the rate limiting, waiting 5 seconds...")
      time.sleep(5)
    else:
      print("failed to scrape timeline at " + feedurl + " , call got status: " + str(r.status_code))
      return False
  return True


def getcomments(post):
  global db
  global debug
  feedurl = 'https://gab.ai/posts/' + str(post) + '/comments/index?limit=1000'
  done = False
  while not done:
    r = requests.get(feedurl)
    if r.status_code == 200:
      comments = json.loads(r.text)
      for comment in comments['data']:
        try:
          db.comments.insert_one(comment)
          try:
            db.discovered.insert_one(comment['user'])
          except pymongo.errors.WriteError as e:
            if debug:
              print("error adding discovered commenting account " + comment['user']['username'] + " backend returned: " + str(e))
          if comment['reply_count'] > 0:
            getcomments(comment['id'])
        except pymongo.errors.WriteError as e:
          if debug:
            print("error adding comment " + str(comment['id']) + " backend returned: " + str(e))
      done = True
    elif r.status_code == 429:
      if debug:
        print("hit the rate limiting, waiting 5 seconds...")
      time.sleep(5)
    else:
      print("failed to scrape comments at " + feedurl + " , call got status: " + str(r.status_code))
      return False
  return True


def minegabber(username):
  global db
  global debug
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
          if debug:
            print("error adding follower edge, backend gave us: " + str(e))
    if following:
      for follower in following:
        edge = { "follower_id": gabber.id, "follower_username": gabber.username, "following_id": follower.id, "following_username": follower.username }
        try:
          db.followers.insert_one(edge)
        except pymongo.errors.WriteError as e:
          if debug:
            print("error adding follower edge, backend gave us: " + str(e))


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
  db.posts.create_index([('post.actuser.id',1),('post.id',1)], unique=True)
  db.comments.create_index([('id',1),('user.id',1)], unique=True)
  shouldgetall = False
  username = False
  debug = False
  try:
    opts, args = getopt.getopt(argv,"hadu:")
  except getopt.GetoptError:
    print('parsegabber.py [ -adh ] [ -u <username> ]')
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
      print('parsegabber.py [ -adh ] [ -u <username> ]')
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
