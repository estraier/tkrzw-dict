#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to make an index of translations of a union dictionary
#
# Usage:
#   index_union_tran.py [--input str] [--output str] [--quiet]
#     [--word_prob str]  [--tran_prob str]
#
# Example:
#   ./index_union_tran.py --input union-body.tkh --output union-tran-index.tkh
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

import json
import logging
import operator
import os
import regex
import sys
import time
import tkrzw
import tkrzw_dict
import tkrzw_tokenizer


logger = tkrzw_dict.GetLogger()


class IndexTranslationsBatch:
  def __init__(self, input_path, output_path, word_prob_path, tran_prob_path):
    self.input_path = input_path
    self.output_path = output_path
    self.word_prob_path = word_prob_path
    self.tran_prob_path = tran_prob_path
    self.tokenizer = tkrzw_tokenizer.Tokenizer()

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
      uniq_trans = set()
      translations = []
      for word_entry in entry:
        word_trans = word_entry.get("translation")
        if word_trans:
          dup_word_trans = word_trans
          for word_tran in word_trans:
            match = regex.search(
              r"([\p{Han}\p{Katakana}ー]{2,})(する|すること|される|されること|をする)$", word_tran)
            if match:
              short_word_tran = word_tran[:-len(match.group(2))]
              if short_word_tran:
                dup_word_trans.append(short_word_tran)
            match = regex.search(
              r"([\p{Han}\p{Katakana}ー]{2,})(的|的な|的に)$", word_tran)
            if match:
              short_word_tran = word_tran[:-len(match.group(2))]
              if short_word_tran:
                dup_word_trans.append(short_word_tran)
            match = regex.search(
              r"([\p{Han}]{2,})(が|の|を|に|へ|と|より|から|で|や|な|なる|たる)$", word_tran)
            if match:
              short_word_tran = word_tran[:-len(match.group(2))]
              if short_word_tran:
                dup_word_trans.append(short_word_tran)
          for tran in dup_word_trans:
            norm_tran = tkrzw_dict.NormalizeWord(tran)
            if norm_tran in uniq_trans: continue
            uniq_trans.add(norm_tran)
            translations.append(norm_tran)
      if translations:
        for i, tran in enumerate(translations):
          pair = "{}\t{}".format(key, str(i))
          mem_index.Append(tran, pair, "\t").OrDie()
        num_translations += len(translations)
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Reading: entries={}, translations={}".format(
          num_entries, num_translations))
      it.Next()
    input_dbm.Close().OrDie()
    logger.info("Reading done: entries={}, translations={}".format(
      num_entries, num_translations))
    output_dbm = tkrzw.DBM()
    num_buckets = mem_index.Count() * 2
    output_dbm.Open(
      self.output_path, True, dbm="HashDBM", truncate=True,
      align_pow=0, num_buckets=num_buckets).OrDie()
    word_prob_dbm =None
    if self.word_prob_path:
      word_prob_dbm = tkrzw.DBM()
      word_prob_dbm.Open(self.word_prob_path, False, dbm="HashDBM").OrDie()
    tran_prob_dbm =None
    if self.tran_prob_path:
      tran_prob_dbm = tkrzw.DBM()
      tran_prob_dbm.Open(self.tran_prob_path, False, dbm="HashDBM").OrDie()
    it = mem_index.MakeIterator()
    it.First()
    num_records = 0
    while True:
      record = it.GetStr()
      if not record: break
      key, value = record
      scored_trans = []
      fields = value.split("\t")
      for i in range(0, len(fields), 2):
        word = fields[i]
        score = 1 / (i / 2 + 100000)
        if word_prob_dbm:
          prob = self.GetPhraseProb(word_prob_dbm, "en", word)
          score += prob
        if tran_prob_dbm:
          prob = self.GetTranProb(tran_prob_dbm, word, key)
          score = (score * max(prob, 0.000001)) ** 0.5
        scored_trans.append((word, score))
      scored_trans = sorted(scored_trans, key=lambda x: x[1], reverse=True)
      value = "\t".join([x[0] for x in scored_trans])
      output_dbm.Set(key, value).OrDie()
      num_records += 1
      if num_records % 10000 == 0:
        logger.info("Writing: records={}".format(num_records))
      it.Next()
    if tran_prob_dbm:
      tran_prob_dbm.Close().OrDie()
    if word_prob_dbm:
      word_prob_dbm.Close().OrDie()
    output_dbm.Close().OrDie()
    logger.info("Writing done: records={}".format(num_records))
    mem_index.Close().OrDie()
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def GetPhraseProb(self, word_prob_dbm, language, word):
    tokens = self.tokenizer.Tokenize(language, word, True, True)
    probs = []
    for token in tokens:
      token = tkrzw_dict.NormalizeWord(token)
      prob = float(word_prob_dbm.GetStr(token) or 0.0)
      probs.append(prob)
    probs = sorted(probs)
    min_prob = 0.0
    if probs:
      min_prob =probs[0]
    for prob in probs[1:]:
      min_prob *= min(prob ** 0.5, 0.2)
    min_prob = max(min_prob, 0.000001)
    return min_prob

  def GetTranProb(self, tran_prob_dbm, src_text, trg_text):
    src_text = tkrzw_dict.NormalizeWord(src_text)
    tsv = tran_prob_dbm.GetStr(src_text)
    max_prob = 0.0
    if tsv:
      trg_text = tkrzw_dict.NormalizeWord(trg_text)
      fields = tsv.split("\t")
      for i in range(0, len(fields), 3):
        src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
        norm_trg = tkrzw_dict.NormalizeWord(trg)
        if norm_trg == trg_text:
          max_prob = max(max_prob, prob)
        elif len(norm_trg) >= 2 and trg_text.startswith(norm_trg):
          max_prob = max(max_prob, prob * 0.01)
        elif len(trg_text) >= 2 and norm_trg.startswith(trg_text):
          max_prob = max(max_prob, prob * 0.01)
    return max_prob


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-tran-index.tkh"
  word_prob_path = tkrzw_dict.GetCommandFlag(args, "--word_prob", 1) or ""
  tran_prob_path = tkrzw_dict.GetCommandFlag(args, "--tran_prob", 1) or ""
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  IndexTranslationsBatch(input_path, output_path, word_prob_path, tran_prob_path).Run()


if __name__=="__main__":
  main()
