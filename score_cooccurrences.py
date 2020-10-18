#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to score coocurrences
#
# Usage:
#   score_cooccurrences.py [--data_prefix str] [--language str]
#   (It reads and makes files in the data directory.)
#
# Example:
#   $ ./score_cooccurrences.py --data_prefix enwiki --language en
#   $ ./score_cooccurrences.py --data_prefix jawiki --language ja
#
# Copyright 2020 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain a copy of the License at
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.  See the License for the specific language governing permissions
# and limitations under the License.
#--------------------------------------------------------------------------------------------------

import logging
import math
import operator
import os
import regex
import sys
import time
import tkrzw
import tkrzw_dict


MAX_COOC_PER_WORD = 128
BASE_SCORE = 1000
PROB_CACHE_CAPACITY = 50000


logger = tkrzw_dict.GetLogger()


class CoocScoreBatch:
  def __init__(self, data_prefix, language):
    self.data_prefix = data_prefix
    self.language = language

  def Run(self):
    start_time = time.time()
    logger.info("Process started: data_prefix={}".format(self.data_prefix))
    self.Start()
    it = self.cooc_prob_dbm.MakeIterator()
    it.First()
    num_records = 0
    while True:
      record = it.GetStr()
      if not record: break
      word, cooc_expr = record
      word_prob = self.GetWordProb(word)
      if word_prob:
        word_idf = min(math.log(word_prob) * -1, tkrzw_dict.MAX_IDF_WEIGHT)
        word_score = word_idf ** tkrzw_dict.IDF_POWER
        if tkrzw_dict.IsNumericWord(word):
          word_score *= tkrzw_dict.NUMERIC_WORD_WEIGHT
        elif tkrzw_dict.IsStopWord(self.language, word):
          word_score *= tkrzw_dict.STOP_WORD_WEIGHT
        cooc_words = []
        for elem in cooc_expr.split("\t"):
          pair = elem.split(" ")
          if len(pair) != 2: continue
          cooc_word = pair[0]
          prob = min(float(pair[1]), tkrzw_dict.MAX_PROB_SCORE)
          cooc_prob = self.GetWordProb(cooc_word)
          if not cooc_prob: continue
          cooc_idf = min(math.log(cooc_prob) * -1, tkrzw_dict.MAX_IDF_WEIGHT)
          cooc_score = prob * (cooc_idf ** tkrzw_dict.IDF_POWER)
          if tkrzw_dict.IsNumericWord(cooc_word):
            cooc_score *= tkrzw_dict.NUMERIC_WORD_WEIGHT
          elif tkrzw_dict.IsStopWord(self.language, cooc_word):
            cooc_score *= tkrzw_dict.STOP_WORD_WEIGHT
          cooc_words.append((cooc_word, cooc_score))
        top_cooc_words = sorted(
          cooc_words, key=operator.itemgetter(1), reverse=True)[:MAX_COOC_PER_WORD]
        cooc_exprs = []
        cooc_exprs.append("{}".format(int(word_score * BASE_SCORE)))
        for cooc_word, score in top_cooc_words:
          cooc_exprs.append("{} {}".format(cooc_word, int(score * BASE_SCORE)))
        value = "\t".join(cooc_exprs)
        self.cooc_score_dbm.Set(word, value).OrDie()
      num_records += 1
      if num_records % 1000 == 0:
        logger.info("Making coocurrence scores: {} records".format(num_records))

      # hoge
      #if self.cooc_score_dbm.Count() >= 2500: break

      it.Next()
    self.Finish()
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def Start(self):
    self.start_time = time.time()
    word_prob_path = tkrzw_dict.GetWordProbPath(self.data_prefix)
    cooc_prob_path = tkrzw_dict.GetCoocProbPath(self.data_prefix)
    cooc_score_path = tkrzw_dict.GetCoocScorePath(self.data_prefix)
    self.word_prob_dbm = tkrzw.DBM()
    self.word_prob_dbm.Open(word_prob_path, False, dbm="HashDBM").OrDie()
    self.cooc_prob_dbm = tkrzw.DBM()
    self.cooc_prob_dbm.Open(cooc_prob_path, False, dbm="HashDBM").OrDie()
    num_buckets = self.word_prob_dbm.Count() * 2
    self.cooc_score_dbm = tkrzw.DBM()
    self.cooc_score_dbm.Open(
      cooc_score_path, True, dbm="HashDBM", truncate=True, num_buckets=num_buckets).OrDie()
    self.word_prob_cache = tkrzw.DBM()
    self.word_prob_cache.Open("", True, dbm="CacheDBM", cap_rec_num=PROB_CACHE_CAPACITY)
    logger.info(("Making coocurrence scores: word_prob_count={}, word_prob_size={:.0f}MB" +
                 ", cooc_prob_count={}, cooc_prob_size={:.0f}MB").format(
                   self.word_prob_dbm.Count(), self.word_prob_dbm.GetFileSize() / 1024 / 1024,
                   self.cooc_prob_dbm.Count(), self.cooc_prob_dbm.GetFileSize() / 1024 / 1024))

  def Finish(self):
    logger.info("Making coocurrence scores done: count={}, size={}, elapsed_time={:.2f}".format(
      self.cooc_score_dbm.Count(), self.cooc_score_dbm.GetFileSize(),
      time.time() - self.start_time))
    self.word_prob_cache.Close().OrDie()
    self.cooc_score_dbm.Close().OrDie()
    self.cooc_prob_dbm.Close().OrDie()
    self.word_prob_dbm.Close().OrDie()

  def GetWordProb(self, key):
    value = self.word_prob_cache.Get(key)
    if value:
      return float(value)
    value = self.word_prob_dbm.GetStr(key)
    if value:
      self.word_prob_cache.Set(key, value)
      return float(value)
    return None


def main():
  args = sys.argv[1:]
  data_prefix = tkrzw_dict.GetCommandFlag(args, "--data_prefix", 1) or "result"
  language = tkrzw_dict.GetCommandFlag(args, "--language", 1) or "en"
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  CoocScoreBatch(data_prefix, language).Run()


if __name__=="__main__":
  main()
