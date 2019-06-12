#!/usr/bin/python3

import sys
import re
import argparse
import csv
from pymongo import *

#
# this tool prints hate statistics per community
# make sure to run gabhate.py and com2posts.py first!
# it is part of the Gabber toolset.
# see https://github.com/nomennesc-io/gabber
#

def main(argv):
  db = MongoClient().gab

  communities = {}
  communities['follow'] = {}
  communities['comment'] = {}
  communities['quote'] = {}
  communities['repost'] = {}

  total_count = 0
  total_hatecount = 0

  for post in db.posts.find({},{'post.hateometer.indicator':1,'actuser.communities':1}, no_cursor_timeout=True):
    hate = False
    follow = False
    comment = False
    quote = False
    repost = False
    if 'hateometer' in post['post']:
      total_count += 1
      if post['post']['hateometer']['indicator'] == 'hate_speech':
        hate = True
        total_hatecount += 1
      if 'communities' in post['actuser']:
        if 'follow' in post['actuser']['communities']:
          follow = post['actuser']['communities']['follow']['id']
        if 'comment' in post['actuser']['communities']:
          comment = post['actuser']['communities']['comment']['id']
        if 'quote' in post['actuser']['communities']:
          quote = post['actuser']['communities']['quote']['id']
        if 'repost' in post['actuser']['communities']:
          repost = post['actuser']['communities']['repost']['id']
      if follow and follow in communities['follow']:
        communities['follow'][follow]['count'] += 1
        if hate:
          communities['follow'][follow]['hatecount'] += 1
      elif follow:
        communities['follow'][follow] = {}
        communities['follow'][follow]['count'] = 1
        if hate:
          communities['follow'][follow]['hatecount'] = 1
        else:
          communities['follow'][follow]['hatecount'] = 0

      if comment and comment in communities['comment']:
        communities['comment'][comment]['count'] += 1
        if hate:
          communities['comment'][comment]['hatecount'] += 1
      elif comment:
        communities['comment'][comment] = {}
        communities['comment'][comment]['count'] = 1
        if hate:
          communities['comment'][comment]['hatecount'] = 1
        else:
          communities['comment'][comment]['hatecount'] = 0

      if quote and quote in communities['quote']:
        communities['quote'][quote]['count'] += 1
        if hate:
          communities['quote'][quote]['hatecount'] += 1
      elif quote:
        communities['quote'][quote] = {}
        communities['quote'][quote]['count'] = 1 
        if hate:
          communities['quote'][quote]['hatecount'] = 1 
        else:
          communities['quote'][quote]['hatecount'] = 0 

      if repost and repost in communities['repost']:
        communities['repost'][repost]['count'] += 1
        if hate:
          communities['repost'][repost]['hatecount'] += 1
      elif repost:
        communities['repost'][repost] = {}
        communities['repost'][repost]['count'] = 1
        if hate:
          communities['repost'][repost]['hatecount'] = 1
        else:
          communities['repost'][repost]['hatecount'] = 0


  for com in communities['follow']:
    hatepercent = (communities['follow'][com]['hatecount'] / communities['follow'][com]['count']) * 100
    print("follow community: " + str(com) + " has " + str(communities['follow'][com]['hatecount']) + " hate messages out of a total of " + str(communities['follow'][com]['count']) + " resulting in a percentage of: " + str(hatepercent))

  for com in communities['comment']:
    hatepercent = (communities['comment'][com]['hatecount'] / communities['comment'][com]['count']) * 100
    print("comment community: " + str(com) + " has " + str(communities['comment'][com]['hatecount']) + " hate messages out of a total of " + str(communities['comment'][com]['count']) + " resulting in a percentage of: " + str(hatepercent))

  for com in communities['quote']:
    hatepercent = (communities['quote'][com]['hatecount'] / communities['quote'][com]['count']) * 100
    print("quote community: " + str(com) + " has " + str(communities['quote'][com]['hatecount']) + " hate messages out of a total of " + str(communities['quote'][com]['count']) + " resulting in a percentage of: " + str(hatepercent))

  for com in communities['repost']:
    hatepercent = (communities['repost'][com]['hatecount'] / communities['repost'][com]['count']) * 100
    print("repost community: " + str(com) + " has " + str(communities['repost'][com]['hatecount']) + " hate messages out of a total of " + str(communities['repost'][com]['count']) + " resulting in a percentage of: " + str(hatepercent))

  print("total hate posts: " + str(total_hatecount) + " out of " + str(total_count))

if __name__ == "__main__":
   main(sys.argv[1:])
