#!/usr/bin/python3

import gensim
import nltk
import re
import sys
import os
import random
import string
import argparse
from gensim.utils import simple_preprocess
from gensim import corpora, models
from nltk.stem import *
from nltk.corpus import stopwords
from pymongo import *
import pyLDAvis.gensim


def main(argv):
  global db
  # setting default values and fetching command line arguments

  parser = argparse.ArgumentParser()

  parser.add_argument('-l','--lang', dest='lang', help='Set the language', type=str, nargs='?', required=False, default='en')
  parser.add_argument('-e','--edgetype', dest='edgetype', help='Set the edge type', type=str, nargs='?', required=False, default='repost')
  parser.add_argument('-c','--community', dest='community', help='Set the community id', type=int, nargs='?', required=True)
  parser.add_argument('-t','--topics', dest='topics', help='Set the amount of topics', type=int, nargs='?', required=False, default=10)
  parser.add_argument('-o','--output', dest='output', help='Set the output graphml file', type=str, required=False, nargs='?', default='gab-topics.html')

  args = parser.parse_args()

  # setting up the database

  db = MongoClient().gab

  # setting up the stemmers

  lmtzr = False

  if args.lang == 'en':
    nltk.download('wordnet')
    lmtzr = nltk.WordNetLemmatizer()
    stmr = nltk.stem.snowball.EnglishStemmer()
    stopWords = set(stopwords.words('english'))
  elif args.lang == 'nl':
    stmr = nltk.stem.snowball.DutchStemmer()
    stopWords = set(stopwords.words('dutch'))
  elif args.lang == 'de':
    stmr = nltk.stem.snowball.GermanStemmer()
    stopWords = set(stopwords.words('german'))
  else:
    print('language not supported!')
    return False

  # add a few extra stopwords to exclude

  stopWords = stopWords.union(['http','https','html'])
 
  # initialise our documents list

  docs = []

  # get all the posts from the community and in the language we're after

  query = {'type':'post','post.language':args.lang,'actuser.communities.' + args.edgetype + '.id' : args.community}
 
  # loop over all the posts we found

  for post in db.posts.find(query, {'post.body':1}, no_cursor_timeout=True):
    # remove any links
    body = re.sub('https?:\/\/.*($|[\s\r\n$])','',post['post']['body'],flags=re.MULTILINE)
    # remove any hashtags
    body = re.sub('#[^\s,\.\)\?\!#@\':=\-]+','',body,flags=re.MULTILINE)
    # remove any @ mentions
    body = re.sub('@[^\s,\.\)\?\!#@\':=\-]+','',body,flags=re.MULTILINE)
    # initialise an empty document list
    doc = []
    # go through all the words in the post that are atleast 4 characters long
    for w in simple_preprocess(body,min_len=4):
      # ignore stopwords
      if not w in stopWords:
        # if a lemmatizer is available for our language, use it
        if lmtzr:
          w = lmtzr.lemmatize(w)
        # stem the word and add it to the document list
        doc.append(stmr.stem(w))
    # add the document list (filled with words) to the documents list (filled with documents)
    docs.append(doc)
  
  print("done collecting documents")
 
  # set up our dictionary
  dictionary = gensim.corpora.Dictionary(docs)
  
  # filter out words that appear too little, keep the top 100000 
  dictionary.filter_extremes(no_below=15, no_above=0.5, keep_n=100000)
 
  # and set up our corpus
  corpus = [dictionary.doc2bow(doc) for doc in docs]
  
  print("done setting up the dictionary and corpus")

  # release the documents list to free up memory
  del docs
  
  # create the LDA model
  lda_model = gensim.models.LdaMulticore(corpus, num_topics=args.topics, id2word=dictionary, passes=2, workers=4)
 
  # output the results
  print("topics: ")
  print("")
  
  for idx, topic in lda_model.print_topics(-1):
    print('Topic: {} Words: {}'.format(idx, topic))
 
  # create a nice visualisation in html
  pyLDAvis.save_html(pyLDAvis.gensim.prepare(lda_model, corpus, dictionary),args.output)
  
if __name__ == "__main__":
  main(sys.argv[1:])

