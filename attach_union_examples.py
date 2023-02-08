#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to attach example sentences to a union database
#
# Usage:
#   attach_union_examples.py [--input str] [--output str] [--index str] [--max_examples]
#
# Example:
#   ./attach_union_examples.py --input union-body.tkh --output union-body-final.tkh \
#     --index tanaka-index.tkh
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
import tkrzw_pron_util
import tkrzw_tokenizer


logger = tkrzw_dict.GetLogger()


def MakeSentenceKey(text):
  text = regex.sub(r"[^ \p{Latin}]", "", text)
  tokens = [x for x in text.split(" ") if x]
  chars = []
  for token in tokens:
    chars.append(chr(hash(token) % 0xD800))
  return "".join(chars)


class AttachExamplesBatch:
  def __init__(self, input_path, output_path, index_paths, max_examples, min_examples):
    self.input_path = input_path
    self.output_path = output_path
    self.index_paths = index_paths
    self.max_examples = max_examples
    self.min_examples = min_examples
    self.count_all_entries = 0
    self.count_hit_entries = 0
    self.count_examples = 0
    self.tokenizer = tkrzw_tokenizer.Tokenizer()

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}, index_path={}".format(
      self.input_path, self.output_path, ":".join(self.index_paths)))
    word_dict = self.AttachExamples()
    self.SaveEntries(word_dict)
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def AttachExamples(self):
    start_time = time.time()
    logger.info("Attaching started: input_path={}, index_path={}".format(
      self.input_path, ":".join(self.index_paths)))
    word_dict = {}
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False).OrDie()
    indices = []
    for index_path in self.index_paths:
      is_aux = False
      if index_path.startswith("+"):
        is_aux = True
        index_path = index_path[1:]
      index_dbm = tkrzw.DBM()
      index_dbm.Open(index_path, False).OrDie()
      indices.append((index_dbm, is_aux))
    it = input_dbm.MakeIterator()
    it.First().OrDie()
    word_dict = {}
    while True:
      record = it.GetStr()
      if not record: break;
      key, data = record
      entries = json.loads(data)
      first_entry = True
      for entry in entries:
        self.count_all_entries += 1
        if self.count_all_entries % 10000 == 0:
          logger.info("Attaching: entries={}, hit_entries={}, examples={}".format(
            self.count_all_entries, self.count_hit_entries, self.count_examples))
        strict = not first_entry
        self.AttachExamplesEntry(entry, indices, strict)
        first_entry = False
      serialized = json.dumps(entries, separators=(",", ":"), ensure_ascii=False)
      word_dict[key] = serialized
      it.Next()
    index_dbm.Close().OrDie()
    input_dbm.Close().OrDie()
    logger.info("Attaching done: elapsed_time={:.2f}s,"
                " entries={}, hit_entries={}, examples={}".format(
                  time.time() - start_time,
                  self.count_all_entries, self.count_hit_entries, self.count_examples))
    return word_dict

  def AttachExamplesEntry(self, entry, indices, strict):
    entry.pop("example", None)
    word = entry["word"]
    core_words = collections.defaultdict(float)
    core_words[word] = 1.0
    inflections = [
      ("noun_plural", 0.8),
      ("verb_singular", 0.8),
      ("verb_present_participle", 0.7),
      ("verb_past", 0.7),
      ("verb_past_participle", 0.6),
      ("adjective_comparative", 0.3),
      ("adjective_superlative", 0.2),
      ("adverb_comparative", 0.3),
      ("adverb_superlative", 0.2),
    ]
    best_source_length = 60
    best_target_length = 30
    for infl_name, weight in inflections:
      infls = entry.get(infl_name)
      if infls:
        infl = infls[0]
        core_words[infl] = max(core_words[infl], weight)
    docs = []
    for core_word, weight in core_words.items():
      for i, index in enumerate(indices):
        index_dbm, is_aux = index
        index_weight = 0.95 ** i
        if is_aux:
          index_weight *= 0.5
        expr = index_dbm.GetStr(core_word.lower())
        if not expr: continue
        for doc_id in expr.split(","):
          docs.append((i, doc_id, weight * index_weight))
    docs = sorted(docs, key=lambda x: (-x[2], x[0], x[1]))
    uniq_docs = []
    uniq_keys = set()
    for index_id, doc_id, weight in docs:
      key = "{}:{}".format(index_id, doc_id)
      if key in uniq_keys: continue
      uniq_keys.add(key)
      uniq_docs.append((index_id, doc_id, weight))
    if not uniq_docs:
      return
    tran_cands = entry.get("translation") or []
    for item in entry.get("item") or []:
      text = item["text"]
      match = regex.search(r"\[translation\]: ([^\[]+)", text)
      if match:
        tran = match.group(1)
        tran = regex.sub(r"\([^)]+\)", "", tran)
        for tran in tran.split(","):
          tran = tran.strip()
          if regex.search("\p{Han}", tran) and len(tran) >= 2:
            tran_cands.append(tran)
    trans = {}
    uniq_trans = {}
    tran_weight_base = 1.0
    for tran in tran_cands:
      norm_tran = tran.lower()
      if norm_tran in uniq_trans: continue
      tran_score = 1.0
      if regex.search(r"\p{Han}", tran):
        tran_score *= 1.2
      elif regex.search(r"\p{Katakana}", tran):
        tran_score *= 1.1
      if len(tran) == 1:
        tran_score *= 0.5
      elif len(tran) == 2:
        tran_score *= 0.8
      elif len(tran) == 3:
        tran_score *= 0.95
      old_score = trans.get(tran) or 0
      trans[tran] = max(old_score, tran_weight_base * tran_score)
      tran_weight_base *= 0.97
    scored_records = []
    tran_hit_counts = {}    
    for index_id, doc_id, weight in uniq_docs:
      index_dbm, is_aux = indices[index_id]
      doc_key = "[" + doc_id + "]"
      value = index_dbm.GetStr(doc_key)
      if not value: continue
      fields = value.split("\t")
      if len(fields) < 2: continue
      source, target = fields[:2]
      if len(source) < best_source_length / 2 or len(target) < best_target_length / 2: continue
      if len(source) > best_source_length * 3 or len(target) > best_target_length * 3: continue
      source_hit = False
      for core_word, weight in core_words.items():
        if regex.search(r"[A-Z]", core_word):
          loc = source.find(core_word)
          if (not strict and loc == 0) or loc > 0:
            source_hit = True
        else:
          if source.find(core_word) > 0:
            source_hit = True
          elif not strict:
            cap_word = core_word[0].upper() + core_word[1:]
            if source.startswith(cap_word):
              source_hit = True
      if not source_hit: continue
      source_len_score = 1 / math.exp(abs(math.log(len(source) / best_source_length)))
      target_len_score = 1 / math.exp(abs(math.log(len(target) / best_target_length)))
      length_score = (source_len_score * target_len_score) ** 0.2
      target_tokens = self.tokenizer.GetJaPosList(target)
      tran_score = 0
      for tran, tran_weight in trans.items():
        tran_hit_count = tran_hit_counts.get(tran) or 0
        tran_weight *= 0.9 ** tran_hit_count
        if self.CheckTranMatch(target, target_tokens, tran):
          if tran_weight > tran_score:
            tran_score = tran_weight
            tran_hit_counts[tran] = tran_hit_count + 1
      if (strict or is_aux) and tran_score == 0: continue
      tran_score += 0.1
      final_score = (tran_score * tran_score * length_score * weight) ** (1 / 4)
      scored_records.append((final_score, source, target))
    scored_records = sorted(scored_records, reverse=True)
    examples = []
    reserve_examples = []
    uniq_keys = []
    for score, source, target in scored_records:
      if len(examples) >= self.max_examples: break
      uniq_key = MakeSentenceKey(source)
      is_dup = False
      for old_uniq_key in uniq_keys:
        dist = tkrzw.Utility.EditDistanceLev(old_uniq_key, uniq_key)
        dist /= max(len(old_uniq_key), len(uniq_key))
        if dist < 0.5:
          is_dup = True
          break
      if is_dup:
        continue
      uniq_keys.append(uniq_key)
      example = {"e": source, "j": target}
      if len(source) > best_source_length * 2 or len(target) > best_target_length * 2:
        reserve_examples.append(example)
      else:
        examples.append(example)
    for example in reserve_examples:
      if len(examples) >= self.min_examples: break
      examples.append(example)
    if examples:
      self.count_examples += len(examples)
      entry["example"] = examples
      self.count_hit_entries += 1

  def CheckTranMatch(self, target, tokens, query):
    if len(query) >= 2 and target.find(query) >= 0: return True
    match = regex.search(r"(.*[\p{Han}]{2,})(する|される|される|させる|をする|している)", query)
    if match:
      stem_query = match.group(1)
      if target.find(stem_query) >= 0: return True
    start_index = 0
    while start_index < len(tokens):
      i = start_index
      end_index = min(start_index + 4, len(tokens))
      tmp_tokens = []
      while i < end_index:
        token = tokens[i]
        surface = token[0] if i < end_index - 1 else token[3]
        tmp_tokens.append(surface)
        token = "".join(tmp_tokens)
        if query == token:
          return True
        i += 1
      start_index += 1
    return False

  def SaveEntries(self, word_dict):
    start_time = time.time()
    logger.info("Saving started: output_path={}".format(self.output_path))
    output_dbm = tkrzw.DBM()
    num_buckets = len(word_dict) * 2
    output_dbm.Open(self.output_path, True, dbm="HashDBM", truncate=True,
                    align_pow=0, num_buckets=num_buckets)
    num_records = 0
    for key, serialized in word_dict.items():
      num_records += 1
      if num_records % 100000 == 0:
        logger.info("Saving: records={}".format(num_records))
      output_dbm.Set(key, serialized)
    output_dbm.Close().OrDie()
    logger.info("Saving done: elapsed_time={:.2f}s, records={}".format(
      time.time() - start_time, num_records))


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-body-final.tkh"
  index_paths = (tkrzw_dict.GetCommandFlag(args, "--index", 1) or "tanaka-index.tkh").split(",")
  max_examples = int(tkrzw_dict.GetCommandFlag(args, "--max_examples", 1) or 5)
  min_examples = int(tkrzw_dict.GetCommandFlag(args, "--min_examples", 1) or 1)
  if not input_path:
    raise RuntimeError("the input path is required")
  if not output_path:
    raise RuntimeError("the output path is required")
  if not index_paths:
    raise RuntimeError("the index path is required")
  AttachExamplesBatch(input_path, output_path, index_paths, max_examples, min_examples).Run()


if __name__=="__main__":
  main()
