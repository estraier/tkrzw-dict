#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to append example sentences to the WordNet database
#
# Usage:
#   append_wordnet_example.py [--input str] [--output str] [--example str] [--quiet]
#
# Example:
#   ./append_wordnet_example.py --input wordnet-tran.tkh --output wordnet-body.tkh \
#     --example synset-example.tsv
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
import operator
import os
import regex
import sys
import time
import tkrzw
import tkrzw_dict
import tkrzw_tokenizer
import unicodedata


MAX_TRANSLATIONS_PER_WORD = 10


logger = tkrzw_dict.GetLogger()


def MakeSentenceKey(text):
  text = regex.sub(r"[^ \p{Latin}]", "", text)
  tokens = [x for x in text.lower().split(" ") if x]
  chars = []
  for token in tokens:
    chars.append(chr(hash(token) % 0xD800))
  return "".join(chars)


class AppendWordnetExampleBatch:
  def __init__(self, input_path, output_path, example_paths):
    self.input_path = input_path
    self.output_path = output_path
    self.example_paths = example_paths

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}, example_paths={}".format(
                  self.input_path, self.output_path, self.example_paths))
    examples = self.ReadExamples()
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False, dbm="HashDBM").OrDie()
    num_buckets = input_dbm.Count() * 2
    output_dbm = tkrzw.DBM()
    output_dbm.Open(
      self.output_path, True, dbm="HashDBM", truncate=True,
      align_pow=0, num_buckets=num_buckets).OrDie()
    it = input_dbm.MakeIterator()
    it.First()
    num_records = 0
    while True:
      record = it.GetStr()
      if not record: break
      key, serialized = record
      entry = json.loads(serialized)
      self.AppendExamples(entry, examples)
      serialized = json.dumps(entry, separators=(",", ":"), ensure_ascii=False)
      output_dbm.Set(key, serialized).OrDie()
      num_records += 1
      if num_records % 10000 == 0:
        logger.info("Processing: records={}".format(num_records))
      it.Next()
    output_dbm.Close().OrDie()
    input_dbm.Close().OrDie()
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def ReadExamples(self):
    examples = collections.defaultdict(list)
    for path in self.example_paths:
      path = path.strip()
      if not path: continue
      with open(path) as input_file:
        for line in input_file:
          fields = line.strip().split("\t")
          if len(fields) != 3: continue
          synset, word, example = fields
          examples[word].append((synset, example))
    return examples

  def AppendExamples(self, entry, examples):
    old_examples = []
    old_keys = []
    for item in entry["item"]:
      word = item["word"]
      synset = item["synset"]
      gloss = item["gloss"]
      gloss_fields = gloss.split(";")
      gloss_main = gloss_fields[0].strip().lower()
      old_examples.append(gloss_main)
      old_keys.append(MakeSentenceKey(gloss_main))
      num_examples = 0
      for example in gloss_fields[1:]:
        example = example.strip()
        match = regex.fullmatch(r'"(.*)"', example)
        if not match: continue
        example = match.group(1).strip().lower()
        if not example: continue
        old_examples.append(example)
        old_keys.append(MakeSentenceKey(example))
        num_examples += 1
      synset_examples = []
      for rec_synset, rec_example in examples.get(word) or []:
        if rec_synset != synset: continue
        synset_examples.append(rec_example)
        MakeSentenceKey(rec_example)
      synset_examples = sorted(synset_examples, key=lambda x: abs(60 - len(x)))
      new_examples = []
      for example in synset_examples:
        if num_examples >= 1: continue
        norm_example = example.lower()
        key = MakeSentenceKey(example)
        is_dup = False
        for old_example in old_examples:
          if old_example in norm_example:
            is_dup = True
          if example in old_example:
            is_dup = True
        for old_key in old_keys:
          dist = tkrzw.Utility.EditDistanceLev(old_key, key)
          dist /= max(len(old_key), len(key))
          if dist < 0.4:
            is_dup = True
        if is_dup: continue
        old_examples.append(norm_example)
        old_keys.append(key)
        new_examples.append('"' + example + '"')
        num_examples += 1
      if new_examples:
        item["gloss"] = gloss + "; " + "; ".join(new_examples)


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "wordnet-tran.thk"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "wordnet-body.tkh"
  example_paths = set((tkrzw_dict.GetCommandFlag(args, "--example", 1) or ",").split(","))
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  AppendWordnetExampleBatch(input_path, output_path, example_paths).Run()


if __name__=="__main__":
  main()
