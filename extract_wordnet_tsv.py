#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to export word information from a Wordnet DBM
#
# Usage:
#   extract_wordnet_tsv.py [--input str] [--quiet]
#   (It reads the input file and prints the result on the standard output.)
#
# Example:
#   $ ./extract_wordnet_tsv.py --input wordnet-body.tkh > wordnet.tsv
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
import re
import sys
import time
import tkrzw
import tkrzw_dict


logger = tkrzw_dict.GetLogger()


class ExtractWordNetTSVBatch:
  def __init__(self, input_path):
    self.input_path = input_path

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}".format(self.input_path))
    word_dbm = tkrzw.DBM()
    word_dbm.Open(self.input_path, False).OrDie()
    it = word_dbm.MakeIterator()
    it.First()
    num_records = 0
    while True:
      record = it.GetStr()
      if not record: break
      self.PrintRecord(json.loads(record[1]))
      num_records += 1
      if num_records % 10000 == 0:
        logger.info("Processing: records={}".format(num_records))
      it.Next()   
    word_dbm.Close().OrDie()
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def PrintRecord(self, record):
    words = collections.defaultdict(list)
    for item in record["item"]:
      words[item["word"]].append(item)
    attributes = ("translation", "synonym", "antonym", "hypernym", "hyponym",
                  "similar", "derivative")
    for word, items in words.items():
      output = []
      output.append("word={}".format(word))
      for item in items:
        pos = item["pos"]
        gross = item["gross"]
        gross = re.sub(r'; "(.*?)"', r" [-] e.g.: \1", gross)
        for attribute in attributes:
          values = item.get(attribute)
          if values:
            gross += " [-] [{}]: {}".format(attribute, ", ".join(values[:5]))
        output.append("{}={}".format(pos, gross))
        output.append("synset={}".format(item["synset"]))
      print("\t".join(output))


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "wordnet-body.tkh"
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  ExtractWordNetTSVBatch(input_path).Run()


if __name__=="__main__":
  main()
