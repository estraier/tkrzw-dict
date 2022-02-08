#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to make a list of keys of a union dictionary
#
# Usage:
#   extract_union_keys.py [--input str] [--output str] [--tran_prob str] [--quiet]
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


class ExtractKeysBatch:
  def __init__(self, input_path, output_path, tran_prob_path):
    self.input_path = input_path
    self.output_path = output_path
    self.tran_prob_path = tran_prob_path

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}".format(
      self.input_path, self.output_path))
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False, dbm="HashDBM").OrDie()
    tran_prob_dbm = None
    if self.tran_prob_path:
      tran_prob_dbm = tkrzw.DBM()
      tran_prob_dbm.Open(self.tran_prob_path, False, dbm="HashDBM").OrDie()
    it = input_dbm.MakeIterator()
    logger.info("Getting AOA records")
    it.First()
    num_entries = 0
    aoa_records = {}
    real_aoa_probs = collections.defaultdict(list)
    while True:
      record = it.GetStr()
      if not record: break
      key, serialized = record
      entry = json.loads(serialized)
      for word_entry in entry:
        word = word_entry["word"]
        aoa = (word_entry.get("aoa") or word_entry.get("aoa_concept") or
               word_entry.get("aoa_base"))
        if aoa:
          aoa_records[word] = float(aoa)
        real_aoa = word_entry.get("aoa")
        prob = word_entry.get("probability")
        if real_aoa and prob:
          real_aoa_probs[int(float(real_aoa))].append(float(prob))
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Getting AOA records: entries={}".format(num_entries))
      it.Next()
    aoa_prob_map = {}
    min_aoa_prob = 0.0001
    for aoa_age, probs in sorted(list(real_aoa_probs.items())):
      if aoa_age < 3 or aoa_age > 20: continue
      prob_mean = sum(probs) / len(probs)
      min_aoa_prob = min(prob_mean, min_aoa_prob)
      aoa_prob_map[aoa_age] = min_aoa_prob
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
        aoa_prob = 0
        real_aoa = word_entry.get("aoa")
        if real_aoa:
          aoa_prob = float(aoa_prob_map.get(int(float(real_aoa))) or 0)
          prob += aoa_prob
        prob_score = max(prob ** 0.5, 0.00001)
        aoa = (word_entry.get("aoa") or word_entry.get("aoa_concept") or
               word_entry.get("aoa_base"))
        if aoa:
          aoa = float(aoa)
        else:
          aoa = sys.maxsize
          tokens = word.split(" ")
          if len(tokens) > 1:
            max_aoa = 0
            for token in tokens:
              token_aoa = aoa_records.get(token)
              if token_aoa:
                max_aoa = max(max_aoa, float(token_aoa))
              else:
                max_aoa = sys.maxsize
            if max_aoa < sys.maxsize:
              aoa = max_aoa + len(tokens) - 1
        aoa_score = (25 - min(aoa, 20.0)) / 10.0
        tran_score = 1.0
        if "translation" in word_entry:
          tran_score += 1.0
        if tran_prob_dbm:
          tsv = tran_prob_dbm.GetStr(key)
          if tsv:
            fields = tsv.split("\t")
            max_tran_prob = 0.0
            for i in range(0, len(fields), 3):
              tran_src, tran_trg, tran_prob = fields[i], fields[i + 1], float(fields[i + 2])
              if tran_src != word: continue
              if not regex.search(r"[\p{Han}]", tran_trg):
                prob *= 0.5
              max_tran_prob = max(max_tran_prob, tran_prob)
            tran_score += max_tran_prob
        item_score = math.log2(len(word_entry["item"]) + 1)
        labels = set()
        for item in word_entry["item"]:
          labels.add(item["label"])
        label_score = math.log2(len(labels) + 1)
        children = word_entry.get("child")
        child_score = math.log2((len(children) if children else 0) + 4)
        score = prob_score * aoa_score * tran_score * item_score * label_score * child_score
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
    if tran_prob_dbm:
      tran_prob_dbm.Close().OrDie()
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
  tran_prob_path = tkrzw_dict.GetCommandFlag(args, "--tran_prob", 1) or ""
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  ExtractKeysBatch(input_path, output_path, tran_prob_path).Run()


if __name__=="__main__":
  main()
