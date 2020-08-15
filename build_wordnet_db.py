#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to parse WordNet dictionary and build databases
#
# Usage:
#   build_wordnet_db.py [--dict str] [--output str] [--prob str] [--quiet]
#
# Example:
#   ./build_wordnet_db.py --dict WordNet-3.0/dict  --output wordnet.tkh
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


IGNORED_POINTERS = set(("holonym", "meronym", "topic", "region", "usage"))
MIN_PROB = 0.000001


logger = tkrzw_dict.GetLogger()


class BuildWordNetDBBatch:
  def __init__(self, dict_dir, output_path, prob_path):
    self.dict_dir = dict_dir
    self.output_path = output_path
    self.prob_path = prob_path

  def Run(self):
    start_time = time.time()
    logger.info("Process started: dict_dir={}, output_path={}".format(
      self.dict_dir, self.output_path))
    synsets = self.ReadSynsets()
    words = self.OrnigizeWords(synsets)
    self.SaveWords(words)
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def ReadSynsets(self):
    start_time = time.time()
    logger.info("Reading synsets: dict_dir={}".format(self.dict_dir))
    num_word_entries = 0
    synsets = {}
    input_confs = (
      ('data.noun', 'n'), ('data.verb', 'v'), ('data.adj', 'a'), ('data.adv', 'r'))
    for input_name, input_pos in input_confs:
      input_path = os.path.join(self.dict_dir, input_name)
      with open(input_path) as input_file:
        for line in input_file:
          if line.startswith(" "): continue
          line = line.strip()
          gross_parts = line.split(" | ")
          if len(gross_parts) != 2: continue
          gross = gross_parts[1].strip()
          if not gross: continue
          fields = gross_parts[0].split(" ")
          if len(fields) < 6: continue
          synset_id = fields[0] + "-" + input_pos
          pos = self.GetPOSType(fields[2])
          num_words = int(fields[3], 16)
          field_index = 4
          words = []
          for i in range(0, num_words):
            word = fields[field_index]
            word = word.replace("_", " ")
            words.append(word)
            field_index += 2
            num_word_entries += 1
          num_ptrs = int(fields[field_index])
          field_index += 1
          ptrs = []
          for i in range(0, num_ptrs):
            ptr_type = self.GetPointerType(fields[field_index])
            ptr_dest = fields[field_index + 1] + "-" + fields[field_index + 2]
            if ptr_type not in IGNORED_POINTERS:
              ptrs.append((ptr_type, ptr_dest))
            field_index += 4
          synsets[synset_id] = (words, pos, gross, ptrs)
          if len(synsets) % 10000 == 0:
            logger.info("Reading synsets: synsets={}, word_entries={}".format(
              len(synsets), num_word_entries))
    logger.info(
      "Reading synsets done: synsets={}, word_entriess={}, elapsed_time={:.2f}s".format(
        len(synsets), num_word_entries, time.time() - start_time))
    return synsets

  def GetPOSType(self, symbol):
    if symbol == "n": return "noun"
    if symbol == "v": return "verb"
    if symbol == "a": return "adjective"
    if symbol == "r" or symbol == "s": return "adverb"
    raise RuntimeError("unknown:" + symbol)
    return "misc"

  def GetPointerType(self, symbol):
    if symbol.startswith("!"): return "antonym"
    if symbol.startswith("@"): return "hypernym"
    if symbol.startswith("~"): return "hyponym"
    if symbol.startswith("#"): return "holonym"
    if symbol.startswith("%"): return "meronym"
    if symbol.startswith("="): return "attribute"
    if symbol.startswith("+"): return "derivative"
    if symbol.startswith("*"): return "entailment"
    if symbol.startswith(">"): return "cause"
    if symbol.startswith("^"): return "seealso"
    if symbol.startswith("$"): return "group"
    if symbol.startswith("&"): return "similar"
    if symbol.startswith("<"): return "perticiple"
    if symbol.startswith("\\"): return "pertainym"
    if symbol == ";c" or symbol == "-c": return "topic"
    if symbol == ";r" or symbol == "-r": return "region"
    if symbol == ";u" or symbol == "-u": return "usage"
    raise RuntimeError("unknown:" + symbol)
    return symbol

  def OrnigizeWords(self, synsets):
    start_time = time.time()
    logger.info("Organizing words")
    words = collections.defaultdict(list)
    num_synsets = 0
    for synset_id, synset in synsets.items():
      syn_words, pos, gross, ptrs = synset
      for word in syn_words:
        key = word.lower()
        item = {}
        item["synset"] = synset_id
        item["surface"] = word
        item["pos"] = pos
        item["gross"] = gross
        if len(words) > 1:
          synonyms = []
          for syn_word in syn_words:
            if syn_word != word:
              synonyms.append(syn_word)
          if synonyms:
            item["synonym"] = synonyms
        rel_words = collections.defaultdict(set)
        for ptr_type, ptr_dest in ptrs:
          dest = synsets.get(ptr_dest)
          if dest:
            for dest_word in dest[0]:
              if dest_word.lower() == key: continue
              rel_words[ptr_type].add(dest_word)
        for rel_symbol, rel_word_set in rel_words.items():
          item[rel_symbol] = list(rel_word_set)
        words[key].append(item)
      num_synsets += 1
      if num_synsets % 10000 == 0:
        logger.info("Organizing words: synsets={}, words={}".format(
          num_synsets, len(words)))
    logger.info(
      "Organizing words done: synsets={}, words={}, elapsed_time={:.2f}s".format(
        num_synsets, len(words), time.time() - start_time))
    return words

  def SaveWords(self, words):
    start_time = time.time()
    logger.info("Saving words: output_path={}".format(self.output_path))
    prob_dbm = None
    if self.prob_path:
      prob_dbm = tkrzw.DBM()
      prob_dbm.Open(self.prob_path, False, dbm="HashDBM").OrDie()
    word_dbm = tkrzw.DBM()
    num_buckets = len(words) * 2
    word_dbm.Open(
      self.output_path, True, dbm="HashDBM", truncate=True,
      align_pow=0, num_buckets=num_buckets).OrDie()
    num_words = 0
    for key, items in words.items():
      entry = {}
      if prob_dbm:
        prob = self.GetPhraseProb(prob_dbm, key)
        if prob > MIN_PROB:
          entry["score"] = "{:.6f}".format(prob).replace("0.", ".")
        for item in items:
          for attr_name, attr_value in item.items():
            if isinstance(attr_value, list) and len(attr_value) > 1:
              item[attr_name] = self.SortWordsByProb(prob_dbm, attr_value)
      entry["items"] = items        
      serialized = json.dumps(entry, separators=(",", ":"), ensure_ascii=False)
      word_dbm.Set(key, serialized).OrDie()
      num_words += 1
      if num_words % 10000 == 0:
        logger.info("Saving words: words={}".format(num_words))
    word_dbm.Close().OrDie()
    if prob_dbm:
      prob_dbm.Close().OrDie()
    logger.info(
      "Saving words done: words={}, elapsed_time={:.2f}s".format(
        num_words, time.time() - start_time))

  def GetPhraseProb(self, prob_dbm, word):
    min_prob = 1.0
    tokens = word.lower().split(" ")
    for token in tokens:
      prob = float(prob_dbm.GetStr(token) or 0.0)
      min_prob = min(min_prob, prob)
    min_prob = max(min_prob, MIN_PROB)
    min_prob *= 0.3 ** (len(tokens) - 1)
    return min_prob

  def SortWordsByProb(self, prob_dbm, words):
    prob_words = []
    for word in words:
      prob = self.GetPhraseProb(prob_dbm, word)
      prob_words.append((word, prob))
    prob_words = sorted(prob_words, key=operator.itemgetter(1), reverse=True)
    return [x[0] for x in  prob_words]
    

def main():
  args = sys.argv[1:]
  dict_dir = tkrzw_dict.GetCommandFlag(args, "--dict", 1) or "dict"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "wordnet.tkh"
  prob_path = tkrzw_dict.GetCommandFlag(args, "--prob", 1) or ""
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  BuildWordNetDBBatch(dict_dir, output_path, prob_path).Run()
 

if __name__=="__main__":
  main()
