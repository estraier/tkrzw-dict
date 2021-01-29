#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to count parallel phrases by the domain
#
# Usage:
#   count_para_domain_phrases.py [--data_prefix str] [--keyword str] [--dict str] [--thes str]
#     [--source_ngram num] [--target_ngram num] [--target_stem] [--quiet]
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

import collections
import logging
import math
import MeCab
import os
import regex
import struct
import sys
import time
import tkrzw
import tkrzw_dict
import unicodedata
import zlib


#BATCH_MAX_RECORDS = 10000  # for testing
BATCH_MAX_RECORDS = 230000000 # for 12GB RAM usage
MIN_PHRASE_COUNT_IN_BATCH = 2
MERGE_DB_UNIT = 16
MAX_SRC_TOKENS = 64
MAX_TRG_TOKENS = 96
MAX_SENTENCES_IN_DOMAIN = 10000
MAX_TARGETS_IN_BATCH = 64


logger = tkrzw_dict.GetLogger()


class WordCountBatch:
  def __init__(self, data_prefix, keyword_path, dict_path, thes_path,
               source_ngram, target_ngram, target_stem):
    self.data_prefix = data_prefix
    self.keyword_path = keyword_path
    self.keywords = set()
    self.dict_path = dict_path
    self.dict_words = collections.defaultdict(list)
    self.dict_targets = set()
    self.thes_path = thes_path
    self.source_ngram = source_ngram
    self.target_ngram = target_ngram
    self.target_stem = target_stem
    self.num_batches = 0
    self.tagger = MeCab.Tagger(r"--node-format=%m\t%f[0]\t%f[1]\t%f[6]\n")

  def ReadKeywords(self):
    logger.info("Reading keyword: path={}".format(self.keyword_path))
    with open(self.keyword_path) as input_file:
      for line in input_file:
        line = line.strip()
        if line:
          self.keywords.add(line)
    logger.info("Reading keyword done: num_keywords={}".format(len(self.keywords)))

  def ReadDict(self):
    thes_words = collections.defaultdict(list)
    if self.thes_path:
      logger.info("Reading thesaurus: path={}".format(self.thes_path))
      with open(self.thes_path) as input_file:
        for line in input_file:
          fields = line.strip().split("\t")
          if len(fields) < 1: continue
          source = fields[0]
          if not source: continue
          for target in fields[1:]:
            thes_words[source].append(target)
      logger.info("Reading thesaurus done: num_thes_words={}".format(len(thes_words)))
    logger.info("Reading dictionary: path={}".format(self.dict_path))
    with open(self.dict_path) as input_file:
      for line in input_file:
        fields = line.strip().split("\t")
        if len(fields) < 1: continue
        source = fields[0]
        if not source: continue
        for target in fields[1:]:
          self.dict_words[source].append(target)
      num_pure_targets = 0
      num_all_targets = 0
      for key in list(self.dict_words.keys()):
        all_targets = set()
        for target in set(self.dict_words[key]):
          num_pure_targets += 1
          all_targets.add(target)
          thes_targets = thes_words.get(target)
          if thes_targets:
            for thes_target in thes_targets:
              all_targets.add(thes_target)
        num_all_targets += len(all_targets)
        self.dict_words[key] = list(all_targets)
      for source, targets in self.dict_words.items():
        for target in targets:
          self.dict_targets.add(target)
    logger.info(
      ("Reading dictionary done: num_dict_words={}" +
       ", num_pure_targets={}, num_all_targets={}, num_uniq_targets={}").format(
         len(self.dict_words), num_pure_targets, num_all_targets, len(self.dict_targets)))

  def Run(self):
    start_time = time.time()
    logger.info("Process started: data_prefix={}".format(self.data_prefix))
    if self.keyword_path:
      self.ReadKeywords()
    if self.dict_path:
      self.ReadDict()
    self.Start()
    num_domains, num_sentences = 0, 0
    last_domain = ""
    domain_records = []
    for line in sys.stdin:
      line = unicodedata.normalize('NFKC', line).strip()
      fields = line.split("\t")
      if len(fields) != 4: continue
      domain, score, source, target = fields
      if domain != last_domain:
        if domain_records:
          self.FeedDomain(last_domain, domain_records)
          num_domains += 1
          num_sentences += len(domain_records)
          logger.info(
            "Processing: domains={}, sentences={}, records={}, uniq_records={}, RSS={:.2f}MB"
            .format(num_domains, num_sentences, self.num_records, self.mem_phrase_count.Count(),
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
    self.prefix_hashes = set()
    self.suffix_hashes = set()
    self.num_domains = 0
    self.num_sentences = 0
    self.num_records = 0
    self.num_duplications = 0
    self.start_time = time.time()

  re_norm_target = regex.compile(r" *([\p{Han}\p{Hiragana}\p{Katakana}ー]) *")
  def FeedDomain(self, domain, records):
    if len(records) > MAX_SENTENCES_IN_DOMAIN:
      records = sorted(records, key=lambda x: x[0], reverse=True)[:MAX_SENTENCES_IN_DOMAIN]
    self.num_domains += 1
    self.num_sentences += len(records)
    uniq_src_phrases = set()
    uniq_trg_phrases = set()
    uniq_phrase_pairs = set()
    for score, source, target in records:
      prefix_hash = zlib.adler32((source[:32] + "\t" + target[:32]).encode())
      if prefix_hash in self.prefix_hashes:
        self.num_duplications += 1
        continue
      self.prefix_hashes.add(prefix_hash)
      suffix_hash = zlib.adler32((source[-32:] + "\t" + target[-32:]).encode())
      if suffix_hash in self.suffix_hashes:
        self.num_duplications += 1
        continue
      self.suffix_hashes.add(suffix_hash)
      norm_target = self.re_norm_target.sub(r"\1", target)
      src_phrases = self.ExtractSourcePhrases(source, MAX_SRC_TOKENS)
      if not src_phrases: continue
      trg_phrases = self.ExtractTargetPhrases(norm_target, MAX_TRG_TOKENS)
      if self.dict_targets:
        new_trg_phrases = []
        for trg_phrase in trg_phrases:
          if trg_phrase in self.dict_targets:
            new_trg_phrases.append(trg_phrase)
        trg_phrases = new_trg_phrases
      if not trg_phrases: continue
      for src_phrase in src_phrases:
        if self.keywords:
          if not src_phrase.lower() in self.keywords:
            continue
        if self.dict_words:
          dict_targets = self.dict_words.get(src_phrase)
          if not dict_targets: continue
          cmp_trg_phrases = []
          for dict_target in dict_targets:
            if dict_target in norm_target:
              cmp_trg_phrases.append(dict_target)
          for trg_phrase in trg_phrases:
            if trg_phrase in dict_targets:
              cmp_trg_phrases.append(trg_phrase)
        else:
          cmp_trg_phrases = trg_phrases
        uniq_src_phrases.add(src_phrase)
        for trg_phrase in cmp_trg_phrases:
          uniq_trg_phrases.add(trg_phrase)
          uniq_phrase_pairs.add(src_phrase + "\t" + trg_phrase)
      for trg_phrase in trg_phrases:
        uniq_trg_phrases.add(trg_phrase)
    for src_phrase in uniq_src_phrases:
      self.mem_phrase_count.Increment(src_phrase + "\t", 1)
      self.num_records += 1
    for trg_phrase in uniq_trg_phrases:
      self.mem_phrase_count.Increment("\t" + trg_phrase, 1)
      self.num_records += 1
    for phrase_pair in uniq_phrase_pairs:
      self.mem_phrase_count.Increment(phrase_pair, 1)
      self.num_records += 1
    if self.num_records >= BATCH_MAX_RECORDS:
      self.Dump()
      self.Start()

  def Finish(self, total_num_sentences):
    if self.num_records:
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
      "Batch {} dumping: sentences={}, records={}, dup={}, unique_phrases={}".format(
        self.num_batches + 1, self.num_sentences, self.num_records, self.num_duplications,
        self.mem_phrase_count.Count()))
    start_time = time.time()
    fill_ratio = min(self.num_records / BATCH_MAX_RECORDS, 1.0)
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
        if trg_phrase:
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
        else:
          score += 1
        scored_targets.append((trg_phrase, count, score))
      scored_targets = sorted(scored_targets, key=lambda x: x[2], reverse=True)
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
      if src_phrase:
        if src_phrase != last_src_phrase:
          if trg_phrases:
            Output(last_src_phrase, trg_phrases)
          trg_phrases = []
        if count >= min_phrase_count:
          trg_phrases.append((trg_phrase, count))
        last_src_phrase = src_phrase
      else:
        if count >= min_phrase_count:
          dbm_phrase_count.Set("\t" + trg_phrase, count).OrDie()
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

  re_latin_word = regex.compile(r"[\p{Latin}\d][-_'’\p{Latin}]*")
  re_latin_word_head = regex.compile(r"[\p{Latin}\d]")
  re_poss_suffix = regex.compile(r"['’]s?$")
  def ExtractSourcePhrases(self, sentence, max_tokens):
    spans = []
    cursor = 0
    for match in self.re_latin_word.finditer(sentence):
      start, end = match.span()
      if start > cursor:
        region = sentence[cursor:start]
        spans.append(region)
      region = sentence[start:end]
      spans.append(region)
      cursor = end
    phrases = set()
    num_tokens = 0
    for start_index in range(0, len(spans)):
      if not self.re_latin_word_head.match(spans[start_index]):
        continue
      tokens = []
      for index in range(start_index, len(spans)):
        token = spans[index]
        if self.re_latin_word_head.match(token):
          token = self.re_poss_suffix.sub("", token)
          tokens.append(token)
          phrase = " ".join(tokens)
          phrases.add(phrase)
          hit = True
          if len(tokens) >= self.source_ngram:
            break
        elif regex.search(r"[^\s]", token):
          break
      num_tokens += 1
      if num_tokens >= max_tokens:
        break
    return list(phrases)

  def ExtractTargetPhrases(self, sentence, max_tokens):
    tokens = []
    for token in self.tagger.parse(sentence).split("\n"):
      fields = token.split("\t")
      if len(fields) != 4: continue
      tokens.append((fields[0], fields[3] or fields[0]))
    result = []
    for start_index in range(0, min(max_tokens, len(tokens))):
      end_index = min(len(tokens), start_index + self.target_ngram)
      phrase_tokens = []
      for i in range(start_index, end_index):
        token = tokens[i]
        if i < end_index - 1 or not self.target_stem:
          phrase_tokens.append(token[0])
        else:
          phrase_tokens.append(token[1])
        phrase = " ".join(phrase_tokens)
        phrase = self.re_norm_target.sub(r"\1", phrase)
        result.append(phrase)
    return list(set(result))


def main():
  args = sys.argv[1:]
  data_prefix = tkrzw_dict.GetCommandFlag(args, "--data_prefix", 1) or "result-para"
  keyword_path = tkrzw_dict.GetCommandFlag(args, "--keyword", 1) or ""
  dict_path = tkrzw_dict.GetCommandFlag(args, "--dict", 1) or ""
  thes_path = tkrzw_dict.GetCommandFlag(args, "--thes", 1) or ""
  source_ngram = int(tkrzw_dict.GetCommandFlag(args, "--source_ngram", 1) or "3")
  target_ngram = int(tkrzw_dict.GetCommandFlag(args, "--target_ngram", 1) or "3")
  target_stem = tkrzw_dict.GetCommandFlag(args, "--target_stem", 0)
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  WordCountBatch(data_prefix, keyword_path, dict_path, thes_path,
                 source_ngram, target_ngram, target_stem).Run()


if __name__=="__main__":
  main()
