#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to make an index of translations of a WordNet dictionary
#
# Usage:
#   index_wordnet_tran.py [--input str] [--output str] [--quiet]
#
# Example:
#   ./index_wordnet_tran.py --input wordnet-body.tkh --output wordnet-tran-index.tkh
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
import operator
import os
import sys
import time
import tkrzw
import tkrzw_dict
import tkrzw_tokenizer


logger = tkrzw_dict.GetLogger()


class IndexTranslationsBatch:
  def __init__(self, input_path, output_path):
    self.input_path = input_path
    self.output_path = output_path

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}".format(
      self.input_path, self.output_path))
    mem_index = tkrzw.DBM()
    mem_index.Open("", True, dbm="BabyDBM").OrDie()
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False, dbm="HashDBM").OrDie()
    it = input_dbm.MakeIterator()
    it.First()
    num_entries = 0
    num_translations = 0
    while True:
      record = it.GetStr()
      if not record: break
      key, serialized = record
      entry = json.loads(serialized)
      for item in entry["item"]:
        translations = item.get("translation")
        if translations:
          for tran in translations:
            norm_tran = tkrzw_tokenizer.RemoveDiacritic(tran.lower())
            mem_index.Append(norm_tran, key, "\t").OrDie()
          num_translations += len(translations)
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Reading: entries={}, translationss={}".format(
          num_entries, num_translations))
      it.Next()
    input_dbm.Close().OrDie()
    logger.info("Reading done: entries={}, translationss={}".format(
      num_entries, num_translations))
    output_dbm = tkrzw.DBM()
    num_buckets = mem_index.Count() * 2
    output_dbm.Open(
      self.output_path, True, dbm="HashDBM", truncate=True,
      align_pow=0, num_buckets=num_buckets).OrDie()
    it = mem_index.MakeIterator()
    it.First()
    num_records = 0
    while True:
      record = it.GetStr()
      if not record: break
      key, value = record
      value = "\t".join(list(set(value.split("\t"))))
      output_dbm.Set(key, value).OrDie()
      num_records += 1
      if num_records % 10000 == 0:
        logger.info("Writing: records={}".format(num_records))
      it.Next()
    output_dbm.Close().OrDie()
    logger.info("Writing done: records={}".format(num_records))
    mem_index.Close().OrDie()
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "wordnet-body.tkh"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "wordnet-tran-index.tkh"
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  IndexTranslationsBatch(input_path, output_path).Run()
 

if __name__=="__main__":
  main()
