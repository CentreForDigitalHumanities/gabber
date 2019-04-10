#!/usr/bin/python3

import sys
import argparse
import os
from igraph import *
from pymongo import *

#
# this tool reads from a graphml and detects communities
# output can be written to separate graphml files and/or mongodb
#
# this tool is part of the Gabber toolset.
# see https://github.com/utrecht-data-school/gabber
#

def main(argv):
  parser = argparse.ArgumentParser()

  parser.add_argument('-p','--pagerank', dest='pagerank', help='Calculate pagerank for each node in the community', action='store_true', required=False)
  parser.add_argument('-n','--name', dest='comname', help='Set the name for the community type', required=False, nargs='?', default='')
  parser.add_argument('-i','--infile', dest='infile', help='Set the input graphml file', type=str, required=False, nargs='?', default='gabgraph.graphml')
  parser.add_argument('-o','--outdir', dest='outdir', help='Set the output directory for the community graphml files', type=str, required=False, nargs='?', default='')

  args = parser.parse_args()

  if args.outdir:
    if not os.path.exists(args.outdir):
      os.makedirs(args.outdir)

  if args.comname:
    db = MongoClient().gab

  # open the input file

  print("reading the input graph...")
  g = Graph.Read_GraphML(args.infile)

  # extract the giant cluster from our input graph

  print("detecting communities...")
  clusters = g.clusters()
  giant = clusters.giant()

  # detect the communities

  c = giant.community_multilevel()
  print("Modularity score: " + str(c.modularity))

  print("parsing communities...")

  # looping over the detected communities:
  i = 0
  for s in c.subgraphs():

    if args.pagerank:
      # adding pagerank for each node
      s.vs["pagerank"] = s.pagerank()

    if args.outdir:
      # writing the output file
      outfile = args.outdir + '/' + str(i) + ".graphml"
      s.write_graphml(outfile)

    if args.comname:
      comname = 'communities.' + args.comname
      for node in s.vs:
        obj = {}
        obj['id'] = i
        if args.pagerank:
          obj['pagerank'] = node['pagerank']
        db.profiles.update_one({'id':int(node['uid'])},{"$set":{comname:obj}})

    i += 1


if __name__ == "__main__":
   main(sys.argv[1:])
