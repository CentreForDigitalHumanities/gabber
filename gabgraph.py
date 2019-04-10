#!/usr/bin/python3

import sys
import os
import random
import string
import argparse
from pymongo import *

#
# this tool exports a follower-network in graphml from scraped data from gab.ai. 
# it is part of the Gabber toolset.
# see https://github.com/utrecht-data-school/gabber
#

def mkheader(directed,postedges,adddate,addlang):
  # this function writes the graphml header to file

  global out

  out.write('<?xml version="1.0" encoding="UTF-8"?>' + "\n")
  out.write('<graphml xmlns="http://graphml.graphdrawing.org/xmlns"' + "\n")
  out.write('         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"' + "\n")
  out.write('         xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns' + "\n")
  out.write('         http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">' + "\n")
  out.write('  <key id="v_uid" for="node" attr.name="uid" attr.type="string"/>' + "\n")
  out.write('  <key id="v_username" for="node" attr.name="username" attr.type="string"/>' + "\n")
  out.write('  <key id="e_type" for="edge" attr.name="type" attr.type="string"/>' + "\n")
  if postedges:
    out.write('  <key id="e_eid" for="edge" attr.name="eid" attr.type="string"/>' + "\n")
    out.write('  <key id="e_oid" for="edge" attr.name="oid" attr.type="string"/>' + "\n")
    if adddate:
      out.write('  <key id="e_date" for="edge" attr.name="date" attr.type="string"/>' + "\n")
    if addlang:
      out.write('  <key id="e_lang" for="edge" attr.name="lang" attr.type="string"/>' + "\n")
  if directed:
    out.write('  <graph id="gabgraph" edgedefault="directed">' + "\n")
  else:
    out.write('  <graph id="gabgraph" edgedefault="undirected">' + "\n")



def writenode(id,username):
  # this function writes a node to file

  global out

  out.write('    <node id="' + str(id) + '">' + "\n")
  out.write('      <data key="v_uid">' + str(id) + '</data>' + "\n")
  out.write('      <data key="v_username">' + username + '</data>' + "\n")
  out.write('    </node>' + "\n")


def writeedge(source,target,edgetype,eid=False,oid=False,date=False,lang=False):
  # this function writes an edge to a temporary file

  global tmpout

  tmpout.write('    <edge source="' + str(source) + '" target="' + str(target) + '">' + "\n")
  tmpout.write('      <data key="e_type">' + str(edgetype) + '</data>' + "\n")
  if eid and oid:
    tmpout.write('      <data key="e_eid">' + str(eid) + '</data>' + "\n")
    tmpout.write('      <data key="e_oid">' + str(oid) + '</data>' + "\n")
    if date:
      tmpout.write('      <data key="e_date">' + str(date) + '</data>' + "\n")
    if lang:
      tmpout.write('      <data key="e_lang">' + str(lang) + '</data>' + "\n")
  tmpout.write('    </edge>' + "\n")


def mkfooter():
  # this function writes the graphml footer to file

  global out

  out.write('  </graph>' + "\n")
  out.write('</graphml>' + "\n")


def preparenodes(allnodes):
  # this sets up the nodes table in mongodb
  global db

  # first, empty the table if it already exists
  try:
    db.nodes.delete_many({})
  except:
    e = sys.exc_info()[0]

  # then, set a unique index to user id, so we don't get duplicates
  db.nodes.create_index('id', unique=True)

  # if the -a parameter was set, fill the nodes table with all the users we know of
  if allnodes:
    for node in db.discovered.find({}, no_cursor_timeout=True):
      try:
        db.nodes.insert_one({'id':node['id'],'username':node['username']})
      except:
        e = sys.exc_info()[0]

    for node in db.profiles.find({}, no_cursor_timeout=True):
      try:
        db.nodes.insert_one({'id':node['id'],'username':node['username']})
      except:
        e = sys.exc_info()[0]


def parsefollowers(compilenodes,followdir):
  # loop over all the known follow-relations, add nodes to the nodes table, and write the edges

  global db
  for edge in db.followers.find({}, no_cursor_timeout=True):
    if compilenodes:
      try:
        db.nodes.insert_one({'id':edge['follower_id'],'username':edge['follower_username']})
      except:
        e = sys.exc_info()[0]
      try:
        db.nodes.insert_one({'id':edge['following_id'],'username':edge['following_username']})
      except:
        e = sys.exc_info()[0]

    if followdir == 1:
      writeedge(edge['follower_id'],edge['following_id'],'follow')
    elif followdir == 2:
      writeedge(edge['following_id'],edge['follower_id'],'follow')
    out.flush


def parsereposts(compilenodes,repostdir,adddate,addlang):
  # loop over all the known repost-relations, add nodes to the nodes table, and write the edges

  global db
  for edge in db.posts.find({'type':'repost'},{'actuser.id':1,'actuser.username':1,'post.user.id':1,'post.user.username':1,'id':1,'post.id':1,'post.language':1,'published_at':1}, no_cursor_timeout=True):
    if compilenodes:
      try:
        db.nodes.insert({'id':edge['actuser']['id'],'username':edge['actuser']['username']})
      except:
        e = sys.exc_info()[0]
      try:
         db.nodes.insert({'id':edge['post']['user']['id'],'username':edge['post']['user']['username']})
      except:
        e = sys.exc_info()[0]

    if adddate and addlang:
      if repostdir == 1:
        writeedge(edge['actuser']['id'],edge['post']['user']['id'],'repost',edge['id'],edge['post']['id'],edge['published_at'],edge['post']['language'])
      if repostdir == 2:
        writeedge(edge['post']['user']['id'],edge['actuser']['id'],'repost',edge['id'],edge['post']['id'],edge['published_at'],edge['post']['language'])
    elif adddate:
      if repostdir == 1:
        writeedge(edge['actuser']['id'],edge['post']['user']['id'],'repost',edge['id'],edge['post']['id'],edge['published_at'])
      if repostdir == 2:
        writeedge(edge['post']['user']['id'],edge['actuser']['id'],'repost',edge['id'],edge['post']['id'],edge['published_at'])
    elif addlang:
      if repostdir == 1:
        writeedge(edge['actuser']['id'],edge['post']['user']['id'],'repost',edge['id'],edge['post']['id'],edge['post']['language'])
      if repostdir == 2:
        writeedge(edge['post']['user']['id'],edge['actuser']['id'],'repost',edge['id'],edge['post']['id'],edge['post']['language'])
    else:
      if repostdir == 1:
        writeedge(edge['actuser']['id'],edge['post']['user']['id'],'repost',edge['id'],edge['post']['id'])
      if repostdir == 2:
        writeedge(edge['post']['user']['id'],edge['actuser']['id'],'repost',edge['id'],edge['post']['id'])


def parsequotes(compilenodes,quotedir,adddate,addlang):
  # loop over all the known quote-relations, add nodes to the nodes table, and write the edges

  global db
  for edge in db.posts.find({'type':'post','post.is_quote':True,'post.parent':{"$exists":True}},{'actuser.id':1,'actuser.username':1,'published_at':1,'id':1,'post.parent.user.id':1,'post.parent.user.username':1,'post.parent.id':1,'post.language':1}, no_cursor_timeout=True):
    if compilenodes:
      try:
        db.nodes.insert({'id':edge['actuser']['id'],'username':edge['actuser']['username']})
      except:
        e = sys.exc_info()[0]
      try:
        db.nodes.insert({'id':edge['post']['parent']['user']['id'],'username':edge['post']['parent']['user']['username']})
      except:
        e = sys.exc_info()[0]

    if adddate and addlang:
      if quotedir == 1:
        writeedge(edge['actuser']['id'],edge['post']['parent']['user']['id'],'quote',edge['id'],edge['post']['parent']['id'],edge['published_at'],edge['post']['language'])
      if quotedir == 2:
        writeedge(edge['post']['parent']['user']['id'],edge['actuser']['id'],'quote',edge['id'],edge['post']['parent']['id'],edge['published_at'],edge['post']['language'])
    elif adddate:
      if quotedir == 1:
        writeedge(edge['actuser']['id'],edge['post']['parent']['user']['id'],'quote',edge['id'],edge['post']['parent']['id'],edge['published_at'])
      if quotedir == 2:
        writeedge(edge['post']['parent']['user']['id'],edge['actuser']['id'],'quote',edge['id'],edge['post']['parent']['id'],edge['published_at'])
    elif addlang:
      if quotedir == 1:
        writeedge(edge['actuser']['id'],edge['post']['parent']['user']['id'],'quote',edge['id'],edge['post']['parent']['id'],edge['post']['language'])
      if quotedir == 2:
        writeedge(edge['post']['parent']['user']['id'],edge['actuser']['id'],'quote',edge['id'],edge['post']['parent']['id'],edge['post']['language'])
    else:
      if quotedir == 1:
        writeedge(edge['actuser']['id'],edge['post']['parent']['user']['id'],'quote',edge['id'],edge['post']['parent']['id'])
      if quotedir == 2:
        writeedge(edge['post']['parent']['user']['id'],edge['actuser']['id'],'quote',edge['id'],edge['post']['parent']['id'])


def parsecomments(compilenodes,commentdir,adddate,addlang):
  # loop over all the known comment-relations, add nodes to the nodes table, and write the edges

  global db
  for edge in db.comments.find({'parent':{"$exists":True}},{'user.id':1,'user.username':1,'id':1,'parent.user.id':1,'parent.user.username':1,'parent.id':1,'language':1,'created_at':1}, no_cursor_timeout=True):
    if compilenodes:
      try:
        db.nodes.insert({'id':edge['user']['id'],'username':edge['user']['username']})
      except:
        e = sys.exc_info()[0]
      try:
        db.nodes.insert({'id':edge['parent']['user']['id'],'username':edge['parent']['user']['username']})
      except:
        e = sys.exc_info()[0]

    if adddate and addlang:
      if commentdir == 1:
        writeedge(edge['user']['id'],edge['parent']['user']['id'],'comment',edge['id'],edge['parent']['id'],edge['created_at'],edge['language'])
      if commentdir == 2:
        writeedge(edge['parent']['user']['id'],edge['user']['id'],'comment',edge['id'],edge['parent']['id'],edge['created_at'],edge['language'])
    elif adddate:
      if commentdir == 1:
        writeedge(edge['user']['id'],edge['parent']['user']['id'],'comment',edge['id'],edge['parent']['id'],edge['created_at'])
      if commentdir == 2:
        writeedge(edge['parent']['user']['id'],edge['user']['id'],'comment',edge['id'],edge['parent']['id'],edge['created_at'])
    elif addlang:
      if commentdir == 1:
        writeedge(edge['user']['id'],edge['parent']['user']['id'],'comment',edge['id'],edge['parent']['id'],edge['language'])
      if commentdir == 2:
        writeedge(edge['parent']['user']['id'],edge['user']['id'],'comment',edge['id'],edge['parent']['id'],edge['language'])
    else:
      if commentdir == 1:
        writeedge(edge['user']['id'],edge['parent']['user']['id'],'comment',edge['id'],edge['parent']['id'])
      if commentdir == 2:
        writeedge(edge['parent']['user']['id'],edge['user']['id'],'comment',edge['id'],edge['parent']['id'])


def main(argv):
  global db
  global out
  global tmpout

  # setting up the database

  db = MongoClient().gab

  # setting default values and fetching command line arguments

  parser = argparse.ArgumentParser()

  parser.add_argument('-c','--include-comments', dest='docomments', help='Include comments as edges in the graph', action='store_true', required=False, default=False)
  parser.add_argument('-f','--include-follow', dest='dofollow', help='Include follow relations as edges in the graph', action='store_true', required=False, default=False)
  parser.add_argument('-q','--include-quotes', dest='doquotes', help='Include quotes as edges in the graph', action='store_true', required=False, default=False)
  parser.add_argument('-r','--include-reposts', dest='doreposts', help='Include reposts as edges in the graph', action='store_true', required=False, default=False)
  parser.add_argument('-d','--directed', dest='directed', help='Make the graph directed', action='store_true', required=False)
  parser.add_argument('-o','--output', dest='output', help='Set the output graphml file', type=str, required=False, nargs='?', default='gabgraph.graphml')
  parser.add_argument('-a','--all-nodes', dest='allnodes', help='Include all known users as nodes in the graph', action='store_true', required=False, default=False)
  parser.add_argument('-n','--no-compile-nodes', dest='compilenodes', help='Do not recompile the nodes collection', action='store_false', required=False, default=True)
  parser.add_argument('--follow-direction', dest='followdir', help='Direction of the follow relation edges (1 or 2)', type=int, default=1, required=False)
  parser.add_argument('--comment-direction', dest='commentdir', help='Direction of the comment edges (1 or 2)', type=int, default=1, required=False)
  parser.add_argument('--quote-direction', dest='quotedir', help='Direction of the quote edges (1 or 2)', type=int, default=1, required=False)
  parser.add_argument('--repost-direction', dest='repostdir', help='Direction of the repost edges (1 or 2)', type=int, default=1, required=False)
  parser.add_argument('--add-date', dest='adddate', help='Add the timestamp as attribute to the edge', action='store_true', default=False, required=False)
  parser.add_argument('--add-lang', dest='addlang', help='Add the language as attribute to the edge', action='store_true', default=False, required=False)

  args = parser.parse_args()

  # open both our output file and a temporary file to writes edges to
  # although the graphml specification does not enforce any order between nodes and edges,
  # the igraph graphml reader insists that nodes come first. therefore, we first write all
  # the edges to a temporary file and copy that into our actual output file after the nodes
  # have been written.

  out = open(args.output,'w',encoding='utf8')
  outputtmp = args.output + '-' + ''.join(random.choice(string.ascii_lowercase) for _ in range(6))
  tmpout = open(outputtmp,'w',encoding='utf8')

  # first write the header to file, the attributes defined in the header depend on whether
  # we include post-id's in the edge data (which is not the case with follower relations)
  # and whether we include the timestamp and/or language

  postedges = args.docomments or args.doquotes or args.doreposts

  mkheader(args.directed,postedges,args.adddate,args.addlang)
  out.flush()

  # now, prepare the nodes table

  if args.compilenodes:
    print("preparing nodes...")
    preparenodes(args.allnodes)

  # work through all the edge types based on which parameters were given

  if args.docomments: 
    print("exporting comment relations, this may take a while.")
    parsecomments(args.compilenodes,args.commentdir,args.adddate,args.addlang)

  if args.dofollow: 
    print("exporting follower relations, this may take a while.")
    parsefollowers(args.compilenodes,args.followdir)

  if args.doreposts:
    print("exporting repost relations, this may take a while.")
    parsereposts(args.compilenodes,args.repostdir,args.adddate,args.addlang)

  if args.doquotes:
    print("exporting quote relations, this may take a while.")
    parsequotes(args.compilenodes,args.quotedir,args.adddate,args.addlang)

  # and close the temporary file, now that we are done with all the edges

  tmpout.close()

  # write all the nodes to file

  print("exporting nodes...")
  for node in db.nodes.find({}, no_cursor_timeout=True):
    writenode(node['id'],node['username'])

  out.flush()

  # now copy all the edges from the temporary file to the actual output file

  print("rearranging edge data...")
  tmpin = open(outputtmp,'r',encoding='utf8')
  for x in tmpin:
    out.write(x)

  # and remove the temporary file

  tmpin.close()
  os.unlink(outputtmp)

  # finally, add the footer and done!

  mkfooter()
  out.close()
  print("done exporting to: " + str(args.output))


if __name__ == "__main__":
   main(sys.argv[1:])
