#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to divide N-gram phrase counts to calculate probability
#
# Usage:
#   divide_ngram_phrases.py [--data_prefix str]
#   (It reads and makes files in the data directory.)
#
# Example:
#   $ ./divide_ngram_phrases.py --data_prefix enwiki
#   $ ./divide_ngram_phrases.py --data_prefix jawiki
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


MIN_PROB_SINGLE = 0.0000001
MIN_PROB_MULTI = 0.000001
STOP_WORD_PENALTY = 10.0

logger = tkrzw_dict.GetLogger()


class DivideCountBatch:
  def __init__(self, data_prefix):
    self.data_prefix = data_prefix

  def Run(self):
    start_time = time.time()
    logger.info("Process started: data_prefix={}".format(self.data_prefix))
    phrase_count_path = tkrzw_dict.GetPhraseCountPath(self.data_prefix)
    phrase_prob_path = tkrzw_dict.GetPhraseProbPath(self.data_prefix)
    self.DividePhraseCount(phrase_count_path, phrase_prob_path)
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def DividePhraseCount(self, phrase_count_path, phrase_prob_path):
    start_time = time.time()
    logger.info("Writing the phrase probability database: src={}, dest={}".format(
      phrase_count_path, phrase_prob_path))
    phrase_count_dbm = tkrzw.DBM()
    phrase_count_dbm.Open(phrase_count_path, False, dbm="SkipDBM").OrDie()
    it = phrase_count_dbm.MakeIterator()
    it.First()
    record = it.GetStr()
    if not record or len(record[0]) != 0:
      raise RuntimeError("invalid first record")
    num_sentences = int(record[1])
    it.Next()
    num_records = 0
    phrase_prob_dbm = tkrzw.DBM()
    num_buckets = phrase_count_dbm.Count() / 2
    phrase_prob_dbm.Open(
      phrase_prob_path, True, dbm="HashDBM", truncate=True,
      align_pow=0, num_buckets=num_buckets).OrDie()
    it.First()
    it.Next()
    num_records = 0
    while True:
      record = it.GetStr()
      if not record:
        break
      phrase = record[0]
      count = int(record[1])
      prob = count / num_sentences
      has_numeric = regex.search(r"\d", phrase)
      has_hyphen = regex.search(r"(^|\W)-", phrase) or regex.search(r"-($|\W)", phrase)
      min_prob = MIN_PROB_SINGLE
      if phrase.find(" ") >= 0:
        min_prob = MIN_PROB_MULTI
      if has_numeric or has_hyphen:
        min_prob *= STOP_WORD_PENALTY
      if prob >= min_prob:
        value = "{:.7f}".format(prob)
        value = regex.sub(r"^0\.", ".", value)
        phrase_prob_dbm.Set(phrase, value).OrDie()
      num_records += 1
      if num_records % 10000 == 0:
        logger.info("Dividing phrase counts: {} records".format(num_records))
      it.Next()
    logger.info("Optimizing the database")
    phrase_prob_dbm.Rebuild().OrDie()
    phrase_prob_dbm.Close().OrDie()
    phrase_count_dbm.Close().OrDie()
    logger.info("Writing the phrase probability database done: elapsed_time={:.2f}s".format(
      time.time() - start_time))


def main():
  args = sys.argv[1:]
  data_prefix = tkrzw_dict.GetCommandFlag(args, "--data_prefix", 1) or "result"
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  DivideCountBatch(data_prefix).Run()


if __name__=="__main__":
  main()
