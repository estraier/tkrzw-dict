#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to make an index of translations of a union dictionary
#
# Usage:
#   extract_union_tran_keys.py [--input str] [--output str] [--rev_prob str] [--quiet]
#
# Example:
#   ./extract_union_tran_keys.py --input wordnet-body.tkh --output union-keys.txt \
#     --rev_prob jawiki-word-prob.tkh
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

import json
import logging
import math
import os
import regex
import sys
import time
import tkrzw
import tkrzw_dict
import tkrzw_tokenizer


logger = tkrzw_dict.GetLogger()


class ExtractKeysBatch:
  def __init__(self, input_path, output_path, rev_prob_path):
    self.input_path = input_path
    self.rev_prob_path = rev_prob_path
    self.output_path = output_path
    self.tokenizer = tkrzw_tokenizer.Tokenizer()

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}".format(
      self.input_path, self.output_path))
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False, dbm="HashDBM").OrDie()
    rev_prob_dbm = None
    if self.rev_prob_path:
      rev_prob_dbm = tkrzw.DBM()
      rev_prob_dbm.Open(self.rev_prob_path, False, dbm="HashDBM").OrDie()
    it = input_dbm.MakeIterator()
    it.First()
    num_entries = 0
    scores = []
    while True:
      record = it.GetStr()
      if not record: break
      key, expr = record
      num_items = len(expr.split("\t"))
      rev_prob = 1.0
      if rev_prob_dbm:
        rev_prob = self.GetRevProb(rev_prob_dbm, key)
      score = (num_items * rev_prob) ** 0.5
      score -= len(key) * 0.0000001
      if regex.fullmatch(r"[\p{Hiragana}]+", key):
        score *= 0.2
      if len(key) == 1:
        score *= 0.5
      scores.append((key, score))
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Reading: entries={}".format(num_entries))
      it.Next()
    if rev_prob_dbm:
      rev_prob_dbm.Close().OrDie()
    input_dbm.Close().OrDie()
    logger.info("Reading done: entries={}".format(num_entries))
    scores = sorted(scores, key=lambda x: x[1], reverse=True)
    with open(self.output_path, "w") as out_file:
      num_entries = 0
      for key, score in scores:
        print(key, file=out_file)
        num_entries += 1
        if num_entries % 10000 == 0:
          logger.info("Writing: entries={}".format(num_entries))
      logger.info("Writing done: entries={}".format(num_entries))
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def GetRevProb(self, rev_prob_dbm, phrase):
    base_prob = 0.000000001
    tokens = self.tokenizer.Tokenize("ja", phrase, False, True)
    if not tokens: return base_prob
    max_ngram = min(3, len(tokens))
    fallback_penalty = 1.0
    for ngram in range(max_ngram, 0, -1):
      if len(tokens) <= ngram:
        cur_phrase = " ".join(tokens)
        prob = float(rev_prob_dbm.GetStr(cur_phrase) or 0.0)
        if prob:
          return max(prob, base_prob)
        fallback_penalty *= 0.1
      else:
        probs = []
        index = 0
        miss = False
        while index <= len(tokens) - ngram:
          cur_phrase = " ".join(tokens[index:index + ngram])
          cur_prob = float(rev_prob_dbm.GetStr(cur_phrase) or 0.0)
          if not cur_prob:
            miss = True
            break
          probs.append(cur_prob)
          index += 1
        if not miss:
          inv_sum = 0
          for cur_prob in probs:
            inv_sum += 1 / cur_prob
          prob = len(probs) / inv_sum
          prob *= 0.3 ** (len(tokens) - ngram)
          prob *= fallback_penalty
          return max(prob, base_prob)
        fallback_penalty *= 0.1
    return base_prob


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-tran-keys.txt"
  rev_prob_path = tkrzw_dict.GetCommandFlag(args, "--rev_prob", 1) or ""
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  ExtractKeysBatch(input_path, output_path, rev_prob_path).Run()


if __name__=="__main__":
  main()
