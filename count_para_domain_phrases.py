#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to count parallel phrases by the domain
#
# Usage:
#   count_para_domain_phrases.py [--data_prefix str]
#   (It reads the standard input and makes files in the data directory.)
#
# Example:
#   $ bzcat para-domain-sorted-phrases.tsv.bz2 |
#     ./count_para_domain_phrases.py --data_prefix para-domain
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

import logging
import math
import MeCab
import operator
import os
import regex
import struct
import sys
import time
import tkrzw
import tkrzw_dict


#BATCH_MAX_SENTENCES = 1000  # for testing
BATCH_MAX_SENTENCES = 300000 # for 12GB RAM usage
MIN_PHRASE_COUNT_IN_BATCH = 2
MERGE_DB_UNIT = 16
MAX_TOKENS = 64
MAX_SENTENCES_IN_DOMAIN = 1000
MAX_TARGETS_IN_BATCH = 64


logger = tkrzw_dict.GetLogger()


class WordCountBatch:
  def __init__(self, data_prefix, keyword_path):
    self.data_prefix = data_prefix
    self.keyword_path = keyword_path
    self.keywords = set()
    self.num_batches = 0
    self.tagger = MeCab.Tagger(r"--node-format=%m\t%f[0]\t%f[1]\t%f[6]\n")

  def ReadKeywords(self):
    logger.info("Reading keyword: paths={}".format(self.keyword_path))
    with open(self.keyword_path) as input_file:
      for line in input_file:
        line = line.strip()
        if line:
          self.keywords.add(line)
    logger.info("Reading keyword done: num_keywords={}".format(len(self.keywords)))

  def Run(self):
    start_time = time.time()
    logger.info("Process started: data_prefix={}".format(self.data_prefix))
    if self.keyword_path:
      self.ReadKeywords()
    self.Start()
    num_domains, num_sentences = 0, 0
    last_domain = ""
    domain_records = []
    for line in sys.stdin:
      line = line.strip()
      fields = line.split("\t")
      if len(fields) != 4: continue
      domain, score, source, target = fields
      if domain != last_domain:
        if domain_records:
          self.FeedDomain(last_domain, domain_records)
          num_domains += 1
          num_sentences += len(domain_records)
          logger.info(
            "Processing: domains={}, sentences={}, RSS={:.2f}MB".format(
              num_domains, num_sentences,
              tkrzw.Utility.GetMemoryUsage() / 1024.0 / 1024))
        domain_records = []
        last_domain = domain
      domain_records.append((score, source, target))
    if domain_records:
      self.FeedDomain(last_domain, domain_records)
      num_domains += 1
      num_sentences += len(domain_records)
    domain_records = []
    self.Finish(num_domains)
    logger.info(
      "Process done: domains={}, sentences={}, elapsed_time={:.2f}s".format(
        num_domains, num_sentences, time.time() - start_time))

  def Start(self):
    self.mem_phrase_count = tkrzw.DBM()
    self.mem_phrase_count.Open("", True, dbm="BabyDBM").OrDie()
    self.num_domains = 0
    self.num_sentences = 0
    self.start_time = time.time()

  def FeedDomain(self, domain, records):
    if len(records) > MAX_SENTENCES_IN_DOMAIN:
      records = sorted(records, key=lambda x: x[0], reverse=True)[:MAX_SENTENCES_IN_DOMAIN]
    self.num_domains += 1
    self.num_sentences += len(records)
    re_split = regex.compile(r"\s+")
    re_latin = regex.compile(r"\p{Latin}[-\p{Latin}]*")
    uniq_src_phrases = set()
    uniq_trg_phrases = set()
    uniq_phrase_pairs = set()
    for score, source, target in records:
      src_tokens = re_split.split(source)[:MAX_TOKENS]
      trg_phrases = self.ExtractTargetPhrases(target, MAX_TOKENS)
      for i, src_token in enumerate(src_tokens):
        if re_latin.fullmatch(src_token):
          if not self.keywords or src_token.lower() in self.keywords:
            for trg_phrase in trg_phrases:
              uniq_src_phrases.add(src_token)
              uniq_trg_phrases.add(trg_phrase)
              uniq_phrase_pairs.add(src_token + "\t" + trg_phrase)
          if i < len(src_tokens) - 1:
            second = src_tokens[i + 1]
            if re_latin.fullmatch(second):
              concat = src_token + " " + second
              if not self.keywords or concat.lower() in self.keywords:
                for trg_phrase in trg_phrases:
                  uniq_src_phrases.add(concat)
                  uniq_phrase_pairs.add(concat + "\t" + trg_phrase)
    for src_phrase in uniq_src_phrases:
      self.mem_phrase_count.Increment(src_phrase + "\t", 1)
    for trg_phrase in uniq_trg_phrases:
      self.mem_phrase_count.Increment("\t" + trg_phrase, 1)
    for phrase_pair in uniq_phrase_pairs:
      self.mem_phrase_count.Increment(phrase_pair, 1)
    if self.num_sentences >= BATCH_MAX_SENTENCES:
      self.Dump()
      self.Start()

  def Finish(self, total_num_sentences):
    if self.num_sentences:
      self.Dump()
    self.mem_phrase_count = None
    phrase_count_paths = []
    for index in range(0, self.num_batches):
      phrase_count_path = "{}-count-{:08d}.tks".format(self.data_prefix, index)
      if os.path.isfile(phrase_count_path):
        logger.info("Detected merging ID {}".format(index))
        phrase_count_paths.append(phrase_count_path)
    if len(phrase_count_paths) > 1:
      logger.info("Merging word count databases")
      src_phrase_count_paths = phrase_count_paths[:-1]
      dest_phrase_count_path = phrase_count_paths[-1]
      self.MergeDatabases(src_phrase_count_paths, dest_phrase_count_path)
    else:
      dest_phrase_count_path = phrase_count_paths[0]
    phrase_count_path = "{}-count.tks".format(self.data_prefix)
    logger.info("Finishing {} batches: phrase_count_path={}".format(
      self.num_batches, phrase_count_path))
    os.rename(dest_phrase_count_path, phrase_count_path)

  def Dump(self):
    logger.info("Batch {} aggregation done: elapsed_time={:.2f}s, RSS={:.2f}MB".format(
      self.num_batches + 1, time.time() - self.start_time,
      tkrzw.Utility.GetMemoryUsage() / 1024.0 / 1024))
    logger.info(
      "Batch {} dumping: sentences={}, unique_phrases={}".format(
        self.num_batches + 1, self.num_sentences,
        self.mem_phrase_count.Count()))
    start_time = time.time()
    fill_ratio = min(self.num_sentences / BATCH_MAX_SENTENCES, 1.0)
    dbm_phrase_count_path = "{}-count-{:08d}.tks".format(self.data_prefix, self.num_batches)
    dbm_phrase_count = tkrzw.DBM()
    dbm_phrase_count.Open(
      dbm_phrase_count_path, True, dbm="SkipDBM",
      truncate=True, insert_in_order=True, offset_width=4, step_unit=4, max_level=12).OrDie()
    logger.info("Batch {} word count dumping: dest={}".format(
      self.num_batches + 1, dbm_phrase_count_path))
    dbm_phrase_count.Set("", self.num_domains).OrDie()
    it = self.mem_phrase_count.MakeIterator()
    it.First()
    min_phrase_count = max(math.ceil(MIN_PHRASE_COUNT_IN_BATCH * fill_ratio), 2)
    re_symbol = regex.compile(r"[\p{S}\p{P}]")
    re_double_particle = regex.compile(r"^[\p{Hiragana}ー]+ [\p{Hiragana}ー]+")
    re_hiragana_only = regex.compile(r"[ \p{Hiragana}ー]+")
    particles = set(["を", "に", "が", "へ", "や", "の", "と", "から", "で", "より", "な", "は",
                     "です", "ます", "この", "その", "あの", "こと", "する", "される", "た", "て", "と",
                     "ある", "いる", "これ", "それ", "あれ", "れる", "という", "として", "だ", "など"])
    prefixes = [x + " " for x in particles]
    def Output(src_phrase, trg_phrases):
      scored_targets = []
      for trg_phrase, count in trg_phrases:
        score = count
        if re_symbol.search(trg_phrase):
          continue
        if re_double_particle.search(trg_phrase):
          score *= 0.5
        elif trg_phrase in particles:
          score *= 0.5
        else:
          hit = False
          for prefix in prefixes:
            if trg_phrase.startswith(prefix):
              hit = True
              break
          if hit:
            score *= 0.8
        if re_hiragana_only.fullmatch(trg_phrase):
          score *= 0.5
        if len(trg_phrase) <= 1:
          score *= 0.5
        elif len(trg_phrase) <= 2:
          score *= 0.8
        scored_targets.append((trg_phrase, count, score))
      scored_targets = sorted(scored_targets, key=lambda x: x[2], reverse=True)
      if src_phrase and trg_phrase:
        scored_targets = scored_targets[:MAX_TARGETS_IN_BATCH]
      outputs = []
      for trg_phrase, count, score in scored_targets:
        key = src_phrase + "\t" + trg_phrase
        outputs.append((key, count))
      outputs = sorted(outputs)
      for key, value in outputs:
        dbm_phrase_count.Set(key, value).OrDie()      
    last_src_phrase = ""
    trg_phrases = []
    while True:
      record = it.Get()
      if not record:
        break
      src_phrase, trg_phrase = record[0].decode().split("\t")
      count = struct.unpack(">q", record[1])[0]
      if src_phrase != last_src_phrase:
        if trg_phrases:
          Output(last_src_phrase, trg_phrases)
        trg_phrases = []
      if count >= min_phrase_count:
        trg_phrases.append((trg_phrase, count))
      last_src_phrase = src_phrase
      it.Remove()
    if trg_phrases:
      Output(last_src_phrase, trg_phrases)
    dbm_phrase_count.Close().OrDie()
    logger.info("Dumping done: elapsed_time={:.2f}s".format(time.time() - start_time))
    self.num_batches += 1
    merge_db_unit = 1
    while self.num_batches % (merge_db_unit * MERGE_DB_UNIT) == 0:
      merge_db_unit *= MERGE_DB_UNIT
      self.ReduceDatabases(merge_db_unit)

  def ReduceDatabases(self, merge_db_unit):
    step = int(merge_db_unit / MERGE_DB_UNIT)
    index = self.num_batches - merge_db_unit + step - 1
    dest_index = self.num_batches - 1
    src_phrase_count_paths = []
    while index < dest_index:
      logger.info("Detected merging source ID {}".format(index))
      src_phrase_count_paths.append("{}-count-{:08d}.tks".format(self.data_prefix, index))
      index += step
    dest_phrase_count_path = "{}-count-{:08d}.tks".format(self.data_prefix, dest_index)
    logger.info("Merging word count DBM files to {}".format(dest_phrase_count_path))
    start_time = time.time()
    self.MergeDatabases(src_phrase_count_paths, dest_phrase_count_path)
    logger.info("Merging done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def MergeDatabases(self, src_paths, dest_path):
    dbm = tkrzw.DBM()
    dbm.Open(dest_path, True, dbm="SkipDBM").OrDie()
    merge_expr = ':'.join(src_paths)
    dbm.Synchronize(False, merge=merge_expr, reducer="total").OrDie()
    dbm.Close().OrDie()
    for src_path in src_paths:
      os.remove(src_path)

  def ExtractTargetPhrases(self, sentence, max_tokens):
    tokens = []
    for token in self.tagger.parse(sentence).split("\n"):
      fields = token.split("\t")
      if len(fields) != 4: continue
      tokens.append((fields[0], fields[3] or fields[0]))
    result = []
    for i, token in enumerate(tokens):
      if i > max_tokens:
        break
      result.append(token[1])
      if i < len(tokens) - 1:
        second_token = tokens[i+1]
        result.append(token[0] + " " + second_token[1])
        if i < len(tokens) - 2:
          third_token = tokens[i+2]
          result.append(token[0] + " " + second_token[0] + " " + third_token[1])
    return result


def main():
  args = sys.argv[1:]
  data_prefix = tkrzw_dict.GetCommandFlag(args, "--data_prefix", 1) or "result-para"
  keyword_path = tkrzw_dict.GetCommandFlag(args, "--keyword", 1) or ""
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  WordCountBatch(data_prefix, keyword_path).Run()


if __name__=="__main__":
  main()
