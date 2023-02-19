#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to make a list of example sentence pairs of a union dictionary
#
# Usage:
#   extract_union_examples.py [--input str] [--output str] [--quiet]
#
# Example:
#   ./extract_union_examples.py --input union-body.tkh --output_en union-examples.tsv
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


class ExtractExamplesBatch:
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
    all_examples = []
    while True:
      record = it.GetStr()
      if not record: break
      key, serialized = record
      entry = json.loads(serialized)
      for word_entry in entry:
        word = word_entry["word"]
        examples = word_entry.get("example")
        if not examples: continue
        for example in examples:
          all_examples.append((word, example["e"], example["j"]))
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Reading: entries={}".format(num_entries))
      it.Next()
    input_dbm.Close().OrDie()
    logger.info("Reading done: entries={}, examples={}".format((num_entries), len(all_examples)))
    with open(self.output_path, "w") as out_file:
      num_entries = 0
      for word, en, ja in all_examples:
        print(word + "\t" + en + "\t" + ja, file=out_file)
        num_entries += 1
        if num_entries % 100000 == 0:
          logger.info("Writing: entries={}".format(num_entries))
      logger.info("Writing done: entries={}".format(num_entries))
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-examples.tsv"
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  ExtractExamplesBatch(input_path, output_path).Run()


if __name__=="__main__":
  main()
