#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to count parallel phrases by the domain
#
# Usage:
#   extract_para_domain_trans.py [--data_prefix str]
#   (It reads the standard input and makes files in the data directory.)
#
# Example:
#   $ extract_para_domain_trans.py --data_prefix para-domain
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
import logging
import math
import MeCab
import os
import regex
import struct
import sys
import time
import tkrzw
import tkrzw_dict


logger = tkrzw_dict.GetLogger()


class ExtractTransBatch:
  def __init__(self, data_prefix, min_count, min_score, enough_count, base_count):
    self.data_prefix = data_prefix
    self.min_count = min_count
    self.min_score = min_score
    self.enough_count = enough_count
    self.base_count = base_count

  def Run(self):
    start_time = time.time()
    logger.info("Process started: data_prefix={}".format(self.data_prefix))
    phrase_count_path = "{}-count.tks".format(self.data_prefix)
    phrase_count_dbm = tkrzw.DBM()
    phrase_count_dbm.Open(phrase_count_path, False).OrDie()
    it = phrase_count_dbm.MakeIterator()
    it.First()
    record = it.GetStr()
    if not record or len(record[0]) != 0:
      raise RuntimeError("invalid first record")
    num_domains = int(record[1])
    it.Next()
    logger.info("Processing phrase counts")
    num_target_records = 0
    num_pair_records = 0
    last_source = ""
    last_source_count = 0
    targets = []
    source_counts = {}
    target_counts = {}
    while True:
      record = it.GetStr()
      if not record:
        break
      source, target = record[0].split("\t")
      count = int(record[1])
      if source:
        if source != last_source:
          if last_source_count and targets:
            self.ProcessRecord(
              last_source, last_source_count, targets, target_counts)
          targets = []
          last_source = source
          num_pair_records += 1
          if num_pair_records % 10000 == 0:
            logger.info("Processing phrase pair counts: {} records".format(num_pair_records))
        if target:
          targets.append((target, count))
        else:
          last_source_count = count
          source_counts[source] = count
      else:
        target_counts[target] = count
        num_target_records += 1
        if num_target_records % 100000 == 0:
          logger.info("Reading target counts: {} records".format(num_target_records))
      it.Next()
    if last_source_count and targets:
      self.ProcessRecord(
        last_source, last_source_count, targets, target_counts)
    logger.info("Process done: elapsed_time={:.2f}s".format(
      time.time() - start_time))

  def ProcessRecord(self, source, source_count, targets, target_counts):
    scored_targets = []
    for target, pair_count in targets:
      if pair_count < self.min_count:
        continue
      target_count = target_counts.get(target) or 0
      if not target_count:
        continue
      ef_prob = pair_count / (source_count + self.base_count)
      fe_prob = pair_count / (target_count + self.base_count)
      score = (ef_prob * fe_prob)
      if score < self.min_score:
        if len(target) >= 3 and target_count >= self.enough_count:
          pass
        else:
          continue
      scored_targets.append((target, score, ef_prob, fe_prob))
    scored_targets = sorted(scored_targets, key=lambda x: x[1], reverse=True)
    outputs = []
    for target, score, ef_prob, fe_prob in scored_targets:
      target_s = regex.sub(r" *([\p{Han}\p{Hiragana}\p{Katakana}ー]) *", r"\1", target)
      ef_prob_s = regex.sub(r"^0\.", ".", "{:.4f}".format(ef_prob))
      fe_prob_s = regex.sub(r"^0\.", ".", "{:.4f}".format(fe_prob))
      outputs.append("{}|{}|{}".format(target_s, ef_prob_s, fe_prob_s))
    if outputs:
      print("{}\t{}\t{}".format(source, source_count, "\t".join(outputs)), flush=True)


def main():
  args = sys.argv[1:]
  data_prefix = tkrzw_dict.GetCommandFlag(args, "--data_prefix", 1) or "result-para"
  min_count = int(tkrzw_dict.GetCommandFlag(args, "--min_count", 1) or 2)
  min_score = float(tkrzw_dict.GetCommandFlag(args, "--min_score", 1) or 0.01)
  enough_count = int(tkrzw_dict.GetCommandFlag(args, "--enough_count", 1) or 10)
  base_count = int(tkrzw_dict.GetCommandFlag(args, "--base_count", 1) or 2)
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  ExtractTransBatch(data_prefix, min_count, min_score, enough_count, base_count).Run()


if __name__=="__main__":
  main()
