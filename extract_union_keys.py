#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to make a list of keys of a union dictionary
#
# Usage:
#   extract_union_keys.py [--input str] [--output str] [--quiet]
#
# Example:
#   ./extract_union_keys.py --input union-body.tkh --output union-keys.txt
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
    scores = []
    while True:
      record = it.GetStr()
      if not record: break
      key, serialized = record
      entry = json.loads(serialized)
      max_score = 0
      for word_entry in entry:
        word = word_entry["word"]
        prob = float(word_entry.get("probability") or "0")
        prob_score = max(prob ** 0.5, 0.00001)
        aoa = float(word_entry.get("aoa") or word_entry.get("aoa_concept") or
                    word_entry.get("aoa_base") or sys.maxsize)
        aoa_score = (25 - min(aoa, 20.0)) / 10.0
        tran_score = 1.0 if "translation" in word_entry else 0.7
        item_score = math.log2(len(word_entry["item"]) + 2)
        labels = set()
        for item in word_entry["item"]:
          labels.add(item["label"])
        label_score = len(labels) + 1.5
        children = word_entry.get("child")
        child_score = math.log2((len(children) if children else 0) + 4)
        score = prob_score * aoa_score * tran_score * item_score * child_score
        if regex.fullmatch(r"\d+", word):
          score *= 0.1
        elif regex.match(r"\d", word):
          score *= 0.3
        elif regex.search(r"^[^\p{Latin}]", word) or regex.search(r"[^\p{Latin}]$", word):
          score *= 0.5
        elif regex.search(r".[\p{Lu}]", word):
          score *= 0.5
        max_score = max(max_score, score)
      scores.append((key, max_score))
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Reading: entries={}".format(num_entries))
      it.Next()
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
