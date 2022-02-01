#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to extract synonyms from a union dictionary
#
# Usage:
#   extract_union_synonyms.py [--input str] [--quiet]
#
# Example:
#   ./extract_union_synonyms.py --input union-body.tkh > union-synonyms.tsv
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


logger = tkrzw_dict.GetLogger()
inflection_names = ("noun_plural","verb_singular", "verb_present_participle",
                    "verb_past", "verb_past_participle",
                    "adjective_comparative", "adjective_superlative",
                    "adverb_comparative", "adverb_superlative")


class ExtractKeysBatch:
  def __init__(self, input_path):
    self.input_path = input_path

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}".format(self.input_path))
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False, dbm="HashDBM").OrDie()
    it = input_dbm.MakeIterator()
    it.First()
    num_entries = 0
    while True:
      record = it.GetStr()
      if not record: break
      key, serialized = record
      entry = json.loads(serialized)
      for word_entry in entry:
        word = word_entry["word"]
        infls = []
        for infl_name in inflection_names:
          infl_value = word_entry.get(infl_name)
          if infl_value:
            for infl in infl_value.split(","):
              infl = infl.strip()
              if infl and infl != word and infl not in infls:
                infls.append(infl_value)
        parents = word_entry.get("parent") or []
        children = word_entry.get("child") or []
        synonym_scores = collections.defaultdict(float)
        synonym_weight = 1.0
        for item in word_entry["item"]:
          text = item["text"]
          for part in text.split("[-]"):
            part = part.strip()
            match = regex.search(r"\[synonym\]: (.*)", part)
            if match:
              for synonym in match.group(1).split(","):
                synonym = synonym.strip()
                if synonym and synonym != word:
                  synonym_scores[synonym] += synonym_weight
                  synonym_weight *= 0.98
        synonym_scores = sorted(synonym_scores.items(), key=lambda x: x[1], reverse=True)
        synonyms = [x[0] for x in synonym_scores]
        if not infls and not parents and not children and not synonyms: continue
        print("{}\t{}\t{}\t{}\t{}".format(
          word, ",".join(infls), ",".join(parents), ",".join(children), ",".join(synonyms)))
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Reading: entries={}".format(num_entries))
      it.Next()
    input_dbm.Close().OrDie()
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  ExtractKeysBatch(input_path).Run()


if __name__=="__main__":
  main()
