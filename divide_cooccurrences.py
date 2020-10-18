#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to divide word counts to calculate probability
#
# Usage:
#   divide_cooccurrences.py [--data_prefix str] [--language str]
#   (It reads and makes files in the data directory.)
#
# Example:
#   $ ./divide_cooccurrences.py --data_prefix enwiki --language en
#   $ ./divide_cooccurrences.py --data_prefix jawiki --language ja
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
import struct
import sys
import time
import tkrzw
import tkrzw_dict


MAX_COOC_PER_WORD = 256
PROB_CACHE_CAPACITY = 50000


logger = tkrzw_dict.GetLogger()


class DivideCountBatch:
  def __init__(self, data_prefix, language):
    self.data_prefix = data_prefix
    self.language = language

  def Run(self):
    start_time = time.time()
    logger.info("Process started: data_prefix={}, language={}".format(
      self.data_prefix, self.language))
    word_count_path = tkrzw_dict.GetWordCountPath(self.data_prefix)
    word_prob_path = tkrzw_dict.GetWordProbPath(self.data_prefix)
    cooc_count_path = tkrzw_dict.GetCoocCountPath(self.data_prefix)
    cooc_prob_path = tkrzw_dict.GetCoocProbPath(self.data_prefix)
    self.DivideWordCount(word_count_path, word_prob_path)
    self.DivideCoocCount(cooc_count_path, word_prob_path, cooc_prob_path)
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def DivideWordCount(self, word_count_path, word_prob_path):
    start_time = time.time()
    logger.info("Writing the word probability database: src={}, dest={}".format(
      word_count_path, word_prob_path))
    word_count_dbm = tkrzw.DBM()
    word_count_dbm.Open(word_count_path, False, dbm="SkipDBM").OrDie()
    word_prob_dbm = tkrzw.DBM()
    num_buckets = word_count_dbm.Count() * 2
    word_prob_dbm.Open(
      word_prob_path, True, dbm="HashDBM", truncate=True, num_buckets=num_buckets).OrDie()
    it = word_count_dbm.MakeIterator()
    it.First()
    record = it.GetStr()
    if not record or len(record[0]) != 0:
      raise RuntimeError("invalid first record")
    num_sentences = int(record[1])
    it.Next()
    num_records = 0
    while True:
      record = it.GetStr()
      if not record:
        break
      word = record[0]
      count = int(record[1])
      prob = count / num_sentences
      value = "{:.8f}".format(prob)
      value = regex.sub(r"^0\.", ".", value)
      word_prob_dbm.Set(word, value).OrDie()
      num_records += 1
      if num_records % 1000 == 0:
        logger.info("Dividing word counts: {} records".format(num_records))
      it.Next()
    word_prob_dbm.Close().OrDie()
    word_count_dbm.Close().OrDie()
    logger.info("Writing the word probability database done: elapsed_time={:.2f}s".format(
      time.time() - start_time))

  def DivideCoocCount(self, cooc_count_path, word_prob_path, cooc_prob_path):
    start_time = time.time()
    logger.info("Writing the coocccurrence probability database: src={}, dest={}".format(
      cooc_count_path, cooc_prob_path))
    cooc_count_dbm = tkrzw.DBM()
    cooc_count_dbm.Open(cooc_count_path, False, dbm="SkipDBM").OrDie()
    word_prob_dbm = tkrzw.DBM()
    word_prob_dbm.Open(word_prob_path, False, dbm="HashDBM").OrDie()
    cooc_prob_dbm = tkrzw.DBM()
    num_buckets = word_prob_dbm.Count() * 2
    cooc_prob_dbm.Open(
      cooc_prob_path, True, dbm="HashDBM",
      truncate=True, offset_width=4, num_buckets=num_buckets).OrDie()
    word_prob_cache = tkrzw.DBM()
    word_prob_cache.Open("", True, dbm="CacheDBM", cap_rec_num=PROB_CACHE_CAPACITY)
    def GetWordProb(key):
      value = word_prob_cache.Get(key)
      if value:
        return float(value)
      value = word_prob_dbm.GetStr(key)
      if value:
        word_prob_cache.Set(key, value)
        return float(value)
      return None
    it = cooc_count_dbm.MakeIterator()
    it.First()
    record = it.GetStr()
    if not record or len(record[0]) != 0:
      raise RuntimeError("invalid first record")
    num_sentences = int(record[1])
    it.Next()
    num_records = 0
    cur_word = None
    cur_word_prob = 0
    cooc_words = []
    while True:
      record = it.GetStr()
      if not record:
        break
      word_pair = record[0]
      count = int(record[1]) / tkrzw_dict.COOC_BASE_SCORE
      word, cooc_word = word_pair.split(" ")
      if cur_word != word:
        if cooc_words:
          self.SaveCoocWords(cur_word, cooc_words, cooc_prob_dbm)
          num_records += 1
          if num_records % 1000 == 0:
            logger.info("Dividing coocurrence counts: {} records".format(num_records))
        cur_word = word
        cur_word_prob = GetWordProb(cur_word)
        cooc_words = []
      if cur_word_prob:
        cooc_prob = GetWordProb(cooc_word)
        if cooc_prob:
          cooc_idf = min(math.log(cooc_prob) * -1, tkrzw_dict.MAX_IDF_WEIGHT)
          cur_word_count = max(round(cur_word_prob * num_sentences), 1)
          prob = count / cur_word_count
          score = prob * (cooc_idf ** tkrzw_dict.IDF_POWER)
          if tkrzw_dict.IsNumericWord(cooc_word):
            score *= tkrzw_dict.NUMERIC_WORD_WEIGHT
          elif tkrzw_dict.IsStopWord(self.language, cooc_word):
            score *= tkrzw_dict.STOP_WORD_WEIGHT
          cooc_words.append((cooc_word, prob, score))
      it.Next()
    if cur_word and cooc_words:
      self.SaveCoocWords(cur_word, cooc_words, cooc_prob_dbm)
    cooc_prob_dbm.Close().OrDie()
    word_prob_dbm.Close().OrDie()
    cooc_count_dbm.Close().OrDie()
    logger.info("Writing the cooccurrence probability database done: elapsed_time={:.2f}s".format(
      time.time() - start_time))

  def SaveCoocWords(self, word, cooc_words, dbm_cooc_prob):
    top_cooc_words = sorted(
      cooc_words, key=operator.itemgetter(2), reverse=True)[:MAX_COOC_PER_WORD]
    records = []
    for cooc_word, prob, score in top_cooc_words:
      rec_value = "{:.5f}".format(prob)
      rec_value = regex.sub(r"^0\.", ".", rec_value)
      records.append("{} {}".format(cooc_word, rec_value))
    value = "\t".join(records)
    dbm_cooc_prob.Set(word, value).OrDie()


def main():
  args = sys.argv[1:]
  data_prefix = tkrzw_dict.GetCommandFlag(args, "--data_prefix", 1) or "result"
  language = tkrzw_dict.GetCommandFlag(args, "--language", 1) or "en"
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  DivideCountBatch(data_prefix, language).Run()


if __name__=="__main__":
  main()
