#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to make an index of inflections of a union dictionary
#
# Usage:
#   index_union_infl.py [--input str] [--output str] [--quiet]
#
# Example:
#   ./index_union_infl.py --input union-body.tkh --output union-infl-index.txt
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

import collections
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
  def __init__(self, input_path, output_path):
    self.input_path = input_path
    self.output_path = output_path

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}".format(
      self.input_path, self.output_path))
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False, dbm="HashDBM").OrDie()
    it = input_dbm.MakeIterator()
    it.First()
    num_entries = 0
    index = collections.defaultdict(list)
    infl_names = ("noun_plural",
             "verb_singular", "verb_present_participle",
             "verb_past", "verb_past_participle",
             "adjective_comparative", "adjective_superlative",
             "adverb_comparative", "adverb_superlative")
    while True:
      record = it.GetStr()
      if not record: break
      key, serialized = record
      entry = json.loads(serialized)
      for word_entry in entry:
        word = word_entry["word"]
        prob = max(float(word_entry.get("probability") or "0"), 0.0000001)
        score = prob * math.log2(len(word_entry["item"]))
        if "translation" in word_entry:
          score *= 2
        inflections = set()
        for infl_name in infl_names:
          inflection = word_entry.get(infl_name)
          if inflection:
            for infl_value in regex.split(r"[,|]", inflection):
              infl_value = tkrzw_dict.NormalizeWord(infl_value.strip())
              if not regex.search(r"\p{Latin}", infl_value): continue
              if infl_value == key: continue
              inflections.add(infl_value)
        for inflection in inflections:
          index[inflection].append((word, score))
        alternatives = word_entry.get("alternative")
        if alternatives:
          for alternative in alternatives:
            alternative = tkrzw_dict.NormalizeWord(alternative)
            if not regex.search(r"\p{Latin}", infl_value): continue
            if alternative == key: continue
            index[alternative].append((word, score * 0.1))
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Reading: entries={}".format(num_entries))
      it.Next()
    input_dbm.Close().OrDie()
    logger.info("Reading done: entries={}".format(num_entries))
    output_dbm = tkrzw.DBM()
    num_buckets = len(index) * 2
    output_dbm.Open(self.output_path, True, dbm="HashDBM", truncate=True,
                    align_pow=0, num_buckets=num_buckets).OrDie()
    num_entries = 0
    for inflection, scores in index.items():
      scores = sorted(scores, key=lambda x: x[1], reverse=True)
      words = []
      uniq_words = set()
      for base_word, score in scores:
        if base_word in uniq_words: continue
        words.append(base_word)
        uniq_words.add(base_word)
      output_dbm.Set(inflection, "\t".join(words)).OrDie()
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Writing: entries={}".format(num_entries))
    output_dbm.Close().OrDie()
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-keys.txt"
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  ExtractKeysBatch(input_path, output_path).Run()


if __name__=="__main__":
  main()
