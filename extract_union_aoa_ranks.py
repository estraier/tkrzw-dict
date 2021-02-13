#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to make a list of words, AOA and translationsof a union dictionary
#
# Usage:
#   extract_union_aoa_ranks.py [--input str] [--output str] [--quiet]
#
# Example:
#   ./extract_union_aoa_ranks.py --input union-body.tkh --output union-aoa-ranks.tks
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
import operator
import os
import regex
import sys
import time
import tkrzw
import tkrzw_dict
import tkrzw_tokenizer


logger = tkrzw_dict.GetLogger()


class ExtractAOABatch:
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
    logger.info("Getting AOA records")
    num_entries = 0
    records = []
    it.First()
    while True:
      record = it.GetStr()
      if not record: break
      key, serialized = record
      entry = json.loads(serialized)
      for word_entry in entry:
        word = word_entry["word"]
        trans = word_entry.get("translation")
        if not trans: continue
        trans = trans[:8]
        labels = set()
        poses = {}
        for item in word_entry["item"]:
          labels.add(item["label"])
          poses[item["pos"]] = True
        poses = poses.keys()
        aoa = (word_entry.get("aoa") or word_entry.get("aoa_concept") or
               word_entry.get("aoa_base"))
        if aoa:
          aoa = float(aoa)
        else:
          if len(labels) < 2:
            continue
          prob = word_entry.get("probability")
          if not prob: continue
          prob = float(prob)
          if word.count(" "):
            token_probs = []
            for token in word.split(" "):
              token_serialized = input_dbm.GetStr(token.lower())
              token_prob = 0.0
              if token_serialized:
                for token_entry in json.loads(token_serialized):
                  token_word = token_entry["word"]
                  if token_word != token: continue
                  token_prob = float(token_entry.get("probability") or 0.0)
              token_probs.append(token_prob)
            min_token_prob = min(token_probs)
            if min_token_prob > prob:
              prob = (prob * min_token_prob) ** 0.5
          aoa = math.log(prob + 0.00000001) * -1 + 3.5
        record = (word, aoa, poses, trans)
        records.append(record)
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Getting AOA records: entries={}".format(num_entries))
      it.Next()
    logger.info("Reading done: entries={}".format(num_entries))
    input_dbm.Close().OrDie()
    records = sorted(records, key=lambda x: x[1])
    output_dbm = tkrzw.DBM()
    output_dbm.Open(self.output_path, True, dbm="SkipDBM", truncate=True,
                    insert_in_order=True).OrDie()
    num_entries = 0
    for word, aoa, poses, trans in records:
      key = "{:05d}".format(num_entries)
      fields = [word]
      fields.append("{:.2f}".format(aoa))
      fields.append(",".join(poses))
      fields.append(",".join(trans))
      output_dbm.Set(key, "\t".join(fields)).OrDie()
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Writing: entries={}".format(num_entries))
      if num_entries >= 100000:
        break
    logger.info("Writing done: entries={}".format(num_entries))
    output_dbm.Close().OrDie()
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-aoa-rank.tks"
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  ExtractAOABatch(input_path, output_path).Run()


if __name__=="__main__":
  main()
