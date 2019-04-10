#!/usr/bin/python3

import requests
import json
import sys
import getopt
import time
import pymongo
from pymongo import *

#
# this is a scraper for gab.ai, part of the Gabber toolset
# see https://github.com/utrecht-data-school/gabber
#
# to follow the execution flow, read from bottom to top
#

class Gabber:

  # a data object with the bare essentials of a gab user

  def __init__(self,id,username):
    self.id = id
    self.username = username
    self.private = False


def showhelp():

  # a generic function to print how to use this program

  print("usage: minegab.py [-a] [-h] [-v] [-u <username>] [-d <username>]")
  print("")
  print("minegab.py is a scraper. It is part of the Gabber toolset for analysing gab.ai")
  print("See https://github.com/utrecht-data-school/gabber")
  print("")
  print("arguments:")
  print("  -a             scrape all discovered accounts")
  print("  -h             show this help message")
  print("  -n             scrape all the newsitems")
  print("  -v             verbose: print debug output")
  print("  -d <username>  delete the scraped profile of <username>")
  print("  -u <username>  start by scraping <username>")
  print("")


def dbgmsg(e):

  # a generic function for printing debug messages

  global debug
  if debug:
    print(e)
    sys.stdout.flush()


def getfromgab(url):

  # a generic function to get JSON data from gab.ai and parse the output

  global debug

  # if there's rate-limiting or network trouble, we'll try again 15 times
  # if we manage to fetch and parse the JSON data from gab, we return the corresponding data object
  # if not, we return false

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
      elif r.status_code == 502 or r.status_code == 504:
        dbgmsg("gab seems to be down, waiting 5 minutes...")
        failcount += 1
        time.sleep(300)
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

  # fetch this user's profile

  userurl = 'https://gab.ai/users/' + username
  profile = getfromgab(userurl)
  if profile:
    gabber = Gabber(id = profile['id'], username = profile['username'])

    # add the profile to the database, our unique index will prevent duplicates and raise an error
    # return false if this is a duplicate, so we won't scrape any further

    try:
      db.profiles.insert_one(profile)
      if profile['is_private']:
        gabber.private = True
    except pymongo.errors.WriteError as e:
      dbgmsg("skipping " + username + " - already mined")
      return False
  
    # if all went well, return a dataobject with the user's id, username and private status

    return gabber
  else:
    print("FAILURE: failed to scrape profile for " + username)
    sys.stdout.flush()
    return False


def getfollowers(username):
  global db

  # prepare a list of followers of this user

  followers = []
  length = 1
  before = 0

  # the followers list is fetched in chunks, keep fetching until we get an empty chunk
  # the 'before' parameter indicates which chunk we are fetching

  while length > 0:

    # fetch the chunk

    followerurl = 'https://gab.ai/users/' + username + '/followers?before=' + str(before)
    followerset = getfromgab(followerurl)
    if followerset:
      
      # update the before parameter for the chunk after this one

      length = len(followerset['data'])
      before += length

      # loop over the accounts in this chunk

      for follower in followerset['data']:
        gabber = Gabber(id = follower['id'], username = follower['username'])

        # add everyone to the discovered accounts in the database, our unique index will prevent duplicates and raise an error

        try:
          db.discovered.insert_one(follower)
        except pymongo.errors.WriteError as e:
          dbgmsg("error adding discovered follower account " + follower['username'] + " : " + str(e))

        # add the account following our user to the list

        followers.append(gabber)
    else:
      print("FAILURE: could not mine followers for " + username)
      sys.stdout.flush()
      return False

  # if all went well, return the complete list of accounts this user is following

  return followers


def getfollowing(username):
  global db

  # prepare a list of accounts this user is following

  following = []
  length = 1
  before = 0

  # the following list is fetched in chunks, keep fetching until we get an empty chunk
  # the 'before' parameter indicates which chunk we are fetching

  while length > 0:

    # fetch the chunk

    followingurl = 'https://gab.ai/users/' + username + '/following?before=' + str(before)
    followingset = getfromgab(followingurl)
    if followingset:
      
      # update the before parameter for the chunk after this one

      length = len(followingset['data'])
      before += length

      # loop over the accounts in this chunk

      for follows in followingset['data']:
        gabber = Gabber(id = follows['id'], username = follows['username'])

        # add everyone to the discovered accounts in the database, our unique index will prevent duplicates and raise an error

        try:
          db.discovered.insert_one(follows)
        except pymongo.errors.WriteError as e:
          dbgmsg("error adding discovered following account " + follows['username'] + " : " + str(e))

        # add the account our user is following to the list

        following.append(gabber)
    else:
      print("FAILURE: could not mine following for " + username)
      sys.stdout.flush()
      return False

  # if all went well, return the complete list of accounts this user is following

  return following


def gettimeline(username):
  global db

  # the timeline is fetched in chunks, gab should tell us no-more is true at the last chunk
  # chunks are specified by the 'before' timestamp

  before = False
  nomore = False
  while nomore == False:

    # fetch the next chunk from the timeline

    feedurl = 'https://gab.ai/api/feed/' + username
    if before:
      feedurl = 'https://gab.ai/api/feed/' + username + '?before=' + before
    contentset = getfromgab(feedurl)
    if contentset:
      nomore = contentset['no-more']

      # if the chunk was retrieved succesfully, but turned out empty, treat this as the end of the timeline
      # in theory, this shouldn't happen, but sometimes it does...

      if not len(contentset['data']):
        nomore = True
      
      # loop over all the posts in the chunk

      for content in contentset['data']:

        # store the post in the timeline

        try:
          db.posts.insert_one(content)

          # if the posts has comments, scrape those aswell

          if content['post']['reply_count'] > 0:
            getcomments(content['post']['id'])
        except pymongo.errors.WriteError as e:
          dbgmsg("error storing post " + str(content['post']['id']) + " backend returned: " + str(e))

        # if the post is a quote and the 'parent' is still available, get the author who this post is quoting from 
        # add the author to the discovered accounts in our database, our unique index will prevent duplicates and raise an error

        if content['post']['is_quote'] and 'parent' in content['post']:
          try:
            db.discovered.insert_one(content['post']['parent']['user'])
          except pymongo.errors.WriteError as e:
            dbgmsg("error adding discovered quoting account " + content['post']['parent']['user']['username'] + " backend returned: " + str(e))
        before = content['published_at']
    else:
      print("FAILURE: failed to scrape timeline for " + username)
      sys.stdout.flush()
      return False
  return True


def getcomments(post):
  global db

  # fetch the list of comments under the given post

  commentsurl = 'https://gab.ai/posts/' + str(post) + '/comments/index?limit=1000'
  comments = getfromgab(commentsurl)

  # if the comments were fetched succesfully, loop over the list and store them in the database

  if comments:
    for comment in comments['data']:
      try:
        db.comments.insert_one(comment)

        # add the author of the comment to the discovered accounts in the database, our unique index will prevent duplicates and raise an error

        try:
          db.discovered.insert_one(comment['user'])
        except pymongo.errors.WriteError as e:
          dbgmsg("error adding discovered commenting account " + comment['user']['username'] + " backend returned: " + str(e))

        # if there are replies under the found comment, go down the rabbit hole and scrape those comments aswell

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

  # start by scraping the user profile

  gabber = getprofile(username)

  # if we managed to succesfully scrape the profile and it didn't exist in the database already, continue scraping
  # this check also ensures that two scrapers running parallel will never scrape the same account simultaneously

  if gabber:

    # don't scrape private accounts, there's nothing to get there and it will only give failures

    if not gabber.private:
      print("now scraping " + username)
      sys.stdout.flush()

      # scrape the timeline and get the follower- and following-lists

      gettimeline(username)
      followers = getfollowers(username)
      following = getfollowing(username)

      # loop over the follower- and following-lists to store the edges in the database, the unique index will prevent duplicates and raise an error

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
    else:
      dbgmsg("this is a private account, not scraping timeline or followers.")


def getall():
  global db

  # loop over all discovered accounts, if no profile has been scraped yet, start scraping it

  for discovered in db.discovered.find({},{'id':1,'username':1}, no_cursor_timeout=True):
    if db.profiles.find_one({'id':discovered['id']}) is None:
      minegabber(discovered['username'])


def getridof(user):
  global db

  # removes an account from the scraped profiles, allowing it to be scraped again

  try:
    db.profiles.delete_one({'username':user})
    print("removed user " + user)
  except pymongo.errors.WriteError as e:
    print("error removing user profile " + user + ": " + str(e))


def getnews():
  global db

  # fetches the news items published on gab

  newsurl="https://gab.ai/api/articles"
  latestnews = getfromgab(newsurl)
  if latestnews:
    counter = int(latestnews['data'][0]['id'])
    dbgmsg("highest found: " + str(counter))
    while counter > 0:
      articleurl = newsurl + '/' + str(counter)
      news = getfromgab(articleurl)
      if news:
        try:
          db.news.insert_one(news)
        except pymongo.errors.WriteError as e:
          dbgmsg("error adding news item, backend gave us: " + str(e))
      counter -= 1
    print("fetched all the news!")
  else:
    print("FAILURE: couldn't fetch news")


def main(argv):
  global db
  global debug

  # setting up the database

  db = MongoClient().gab

  # setting unique indeces to prevent duplicates in our database

  db.profiles.create_index('id', unique=True)
  db.discovered.create_index('id', unique=True)
  db.news.create_index('id', unique=True)
  db.followers.create_index([('follower_id',1),('following_id',1)], unique=True)
  db.posts.create_index([('post.actuser.id',1),('post.id',1),('post.type',1)], unique=True)
  db.comments.create_index([('id',1),('user.id',1)], unique=True)

  # setting defaults and parsing the command line arguments

  shouldgetall = False
  username = False
  debug = False
  delete = False
  shouldgetnews = False
  try:
    opts, args = getopt.getopt(argv,"ahnvd:u:")
  except getopt.GetoptError:
    showhelp()
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
      showhelp()
      sys.exit()
    if opt == '-a':
      shouldgetall = True
    if opt == '-n':
      shouldgetnews = True
    if opt == '-v':
      debug = True
    if opt in ("-d"):
      delete = arg
    if opt in ("-u"):
      username = arg
  
  # and start scraping

  if(username):
    minegabber(username)
  if(shouldgetall):
    getall()
  if(delete):
    getridof(delete)
  if(shouldgetnews):
    getnews()


if __name__ == "__main__":
   main(sys.argv[1:])
