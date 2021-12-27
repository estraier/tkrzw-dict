#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to make a thesaurus of translations of a union dictionary
#
# Usage:
#   extract_union_tran_thes.py [--input str] [--wnsyn str] [--quiet]
#
# Example:
#   ./extract_union_tran_thes.py --input wordnet-body.tkh > union-tran-thes.txt
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
  def __init__(self, input_path, wnsyn_path):
    self.input_path = input_path
    self.wnsyn_path = wnsyn_path

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}, wnsyn_path={}".format(
      self.input_path, self.wnsyn_path))
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False, dbm="HashDBM").OrDie()
    it = input_dbm.MakeIterator()
    it.First()
    num_entries = 0
    word_dict = collections.defaultdict(list)
    while True:
      record = it.GetStr()
      if not record: break
      if not record: break
      key, serialized = record
      entry = json.loads(serialized)
      for word_entry in entry:
        trans = word_entry.get("translation")
        if not trans: continue
        trans = set(trans)
        for tran in trans:
          for cmp_tran in trans:
            if cmp_tran == tran: continue
            word_dict[tran].append(cmp_tran)
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Reading: entries={}".format(num_entries))
      it.Next()
    input_dbm.Close().OrDie()
    if self.wnsyn_path:
      with open(self.wnsyn_path) as input_file:
        for line in input_file:
          fields = line.strip().split("\t")
          if len(fields) != 4: continue
          sid, source, tid, target = fields
          word_dict[source].append(target)
          word_dict[target].append(source)
    logger.info("Reading done: entries={}".format(num_entries))
    for source, targets in word_dict.items():
      counts = collections.Counter(targets)
      mod_counts = []
      for target, count in counts.items():
        count += len(regex.sub(r"[^\p{Han}]", "", target)) * 0.01
        count += len(regex.sub(r"[^\p{Katakana}]", "", target)) * 0.001
        count += len(target) * 0.0001
        mod_counts.append((target, count))
      mod_counts = sorted(mod_counts, key=lambda x: x[1], reverse=True)
      final_targets = []
      for target, count in mod_counts:
        if count < 2: continue
        if len(final_targets) > 2 and count < len(final_targets) / 2 + 1: continue
        final_targets.append(target)
      if final_targets:
        print("{}\t{}".format(source, "\t".join(final_targets)))
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  wnsyn_path = tkrzw_dict.GetCommandFlag(args, "--wnsyn", 1) or "wnjpn-synonyms.tab"
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  ExtractKeysBatch(input_path, wnsyn_path).Run()


if __name__=="__main__":
  main()
