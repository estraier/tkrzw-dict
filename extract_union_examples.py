#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to make a list of example sentence pairs of a union dictionary
#
# Usage:
#   extract_union_examples.py [--input str] [--output str] [--aux_example str] [--quiet]
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
  def __init__(self, input_path, output_path, aux_example_paths, max_examples):
    self.input_path = input_path
    self.output_path = output_path
    self.aux_example_paths = aux_example_paths
    self.max_examples = max_examples

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
        for i, example in enumerate(examples):
          all_examples.append((i, num_entries, example["e"], example["j"]))
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Reading: entries={}".format(num_entries))
      it.Next()
    input_dbm.Close().OrDie()
    logger.info("Reading done: entries={}, examples={}".format((num_entries), len(all_examples)))
    aux_examples = self.ReadAuxExamples()
    for i, (source, target) in enumerate(aux_examples):
      all_examples.append((10000, i, source, target))
    all_examples = sorted(all_examples)
    with open(self.output_path, "w") as out_file:
      num_entries = 0
      source_hashes = set()
      for rank, ent_id, source, target in all_examples:
        if num_entries >= self.max_examples: break
        bare_source = regex.sub("[^\p{Latin} ]+", "", source.lower())
        source_hash = hash(bare_source)
        if source_hash in source_hashes: continue
        source_hashes.add(source_hash)
        print(source + "\t" + target, file=out_file)
        num_entries += 1
        if num_entries % 100000 == 0:
          logger.info("Writing: entries={}".format(num_entries))
      logger.info("Writing done: entries={}".format(num_entries))
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def ReadAuxExamples(self):
    best_source_length = 60
    best_target_length = 30
    aux_examples = []
    file_score = 1.0
    for aux_path in self.aux_example_paths:
      if not aux_path: continue
      logger.info("Reading auxiliary examples: path={}".format(aux_path))
      with open(aux_path) as input_file:
        for line in input_file:
          fields = line.strip().split("\t")
          if len(fields) < 2: continue
          source, target = fields[:2]
          if len(source) < best_source_length / 2 or len(source) > best_source_length * 2:
            continue
          if len(target) < best_target_length / 2 or len(target) > best_target_length * 2:
            continue
          source_len_score = 1 / math.exp(abs(math.log(len(source) / best_source_length)))
          target_len_score = 1 / math.exp(abs(math.log(len(target) / best_target_length)))
          length_score = (source_len_score * target_len_score) ** 0.5
          aux_examples.append((source, target, length_score * file_score))
      file_score *= 0.9
    aux_examples = sorted(aux_examples, key=lambda x: (-x[2], source, target))
    logger.info("Reading auxiliary examples done: num={}".format(len(aux_examples)))
    return [(x[0], x[1]) for x in aux_examples]


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-examples.tsv"
  aux_example_paths = (tkrzw_dict.GetCommandFlag(args, "--aux_example", 1) or "").split(",")
  max_examples = int(tkrzw_dict.GetCommandFlag(args, "--max_examples", 1) or 700000)
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  ExtractExamplesBatch(input_path, output_path, aux_example_paths, max_examples).Run()


if __name__=="__main__":
  main()
