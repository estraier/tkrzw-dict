#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to count phrases by the N-gram
#
# Usage:
#   count_ngram_phrases.py [--data_prefix str]
#   (It reads the standard input and makes files in the data directory.)
#
# Example:
#   $ bzcat enwiki-tokenized.tsv.bz2 |
#     ./count_ngram_phrases.py --data_prefix enwiki
#   $ bzcat jawiki-tokenized.tsv.bz2 |
#     ./count_ngram_phrases.py --data_prefix jawiki
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
import operator
import os
import regex
import struct
import sys
import time
import tkrzw
import tkrzw_dict


MAX_SENTENCES_PER_DOC = 64
#BATCH_MAX_WORDS = 150000  # for testing
BATCH_MAX_WORDS = 20000000  # for 1GB RAM usage
#BATCH_MAX_WORDS = 500000000  # for 10GB RAM usage
BATCH_CUTOFF_FREQ = 3
MIN_PHRASE_COUNT_IN_BATCH = 5
MERGE_DB_UNIT = 16


logger = tkrzw_dict.GetLogger()


class WordCountBatch:
  def __init__(self, data_prefix, num_ngrams):
    self.data_prefix = data_prefix
    self.num_ngrams = num_ngrams
    self.num_batches = 0

  def Run(self):
    start_time = time.time()
    logger.info("Process started: data_prefix={}, ngrams={}".format(
      self.data_prefix, self.num_ngrams))
    self.Start()
    num_documents, num_sentences, num_words = 0, 0, 0
    for line in sys.stdin:
      line = line.strip()
      sentences = line.split("\t")
      if not sentences: continue
      sentences = sentences[:MAX_SENTENCES_PER_DOC]
      num_documents += 1
      for sentence in sentences:
        words = []
        for word in sentence.split(" "):
          if word:
            words.append(word)
        if words:
          num_sentences += 1
          num_words += len(words)
          self.FeedSentence(words)
          if num_sentences % 1000 == 0:
            logger.info(
              "Processing: documents={}, sentences={}, words={}, RSS={:.2f}MB".format(
                num_documents, num_sentences, num_words,
                tkrzw.Utility.GetMemoryUsage() / 1024.0 / 1024))
    self.Finish(num_sentences)
    logger.info(
      "Process done: documents={}, sentences={}, words={}, elapsed_time={:.2f}s".format(
        num_documents, num_sentences, num_words, time.time() - start_time))

  def Start(self):
    self.mem_phrase_count = tkrzw.DBM()
    self.mem_phrase_count.Open("", True, dbm="BabyDBM").OrDie()
    self.num_sentences = 0
    self.num_words = 0
    self.num_words_since_cutoff = 0
    self.start_time = time.time()

  def FeedSentence(self, words):
    self.num_sentences += 1
    self.num_words += len(words)
    self.num_words_since_cutoff += len(words)
    uniq_phrases = set()
    start_index = 0
    while start_index < len(words):
      end_index = min(start_index + self.num_ngrams, len(words))
      if start_index == 0:
        word = words[0]
        if regex.fullmatch(r"\p{Lu}[-\p{Ll}]+", word):
          tokens = [word.lower()]
          uniq_phrases.add(" ".join(tokens))
          index = 1
          while index < end_index:
            tokens.append(words[index])
            uniq_phrases.add(" ".join(tokens))
            index += 1
      tokens = []
      index = start_index
      while index < end_index:
        tokens.append(words[index])
        uniq_phrases.add(" ".join(tokens))
        index += 1
      start_index += 1
    for phrase in uniq_phrases:
      self.mem_phrase_count.Increment(phrase, 1)
    if self.num_words >= BATCH_MAX_WORDS:
      self.Dump()
      self.Start()
    elif self.num_words_since_cutoff >= BATCH_MAX_WORDS / BATCH_CUTOFF_FREQ:
      self.DoCutOff()

  def Finish(self, total_num_sentences):
    if self.num_words:
      self.Dump()
    self.mem_phrase_count = None
    phrase_count_paths = []
    for index in range(0, self.num_batches):
      phrase_count_path = "{}-phrase-count-{:08d}.tks".format(self.data_prefix, index)
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
    phrase_count_path = tkrzw_dict.GetPhraseCountPath(self.data_prefix)
    logger.info("Finishing {} batches: phrase_count_path={}".format(
      self.num_batches, phrase_count_path))
    os.rename(dest_phrase_count_path, phrase_count_path)

  def Dump(self):
    logger.info("Batch {} aggregation done: elapsed_time={:.2f}s, RSS={:.2f}MB".format(
      self.num_batches + 1, time.time() - self.start_time,
      tkrzw.Utility.GetMemoryUsage() / 1024.0 / 1024))
    logger.info(
      "Batch {} dumping: sentences={}, words={}, unique_words={}".format(
        self.num_batches + 1, self.num_sentences, self.num_words,
        self.mem_phrase_count.Count()))
    start_time = time.time()
    fill_ratio = min(self.num_words / BATCH_MAX_WORDS, 1.0)
    dbm_phrase_count_path = "{}-phrase-count-{:08d}.tks".format(self.data_prefix, self.num_batches)
    dbm_phrase_count = tkrzw.DBM()
    dbm_phrase_count.Open(
      dbm_phrase_count_path, True, dbm="SkipDBM",
      truncate=True, insert_in_order=True, offset_width=4, step_unit=4, max_level=12).OrDie()
    logger.info("Batch {} word count dumping: dest={}".format(
      self.num_batches + 1, dbm_phrase_count_path))
    dbm_phrase_count.Set("", self.num_sentences).OrDie()
    it = self.mem_phrase_count.MakeIterator()
    it.First()
    min_phrase_count = max(math.ceil(MIN_PHRASE_COUNT_IN_BATCH * fill_ratio), 2)
    while True:
      record = it.Get()
      if not record:
        break
      phrase = record[0]
      count = struct.unpack(">q", record[1])[0]
      if count >= min_phrase_count:
        dbm_phrase_count.Set(phrase, count).OrDie()
      it.Remove()
    dbm_phrase_count.Close().OrDie()
    logger.info("Dumping done: elapsed_time={:.2f}s".format(time.time() - start_time))
    self.num_batches += 1
    merge_db_unit = 1
    while self.num_batches % (merge_db_unit * MERGE_DB_UNIT) == 0:
      merge_db_unit *= MERGE_DB_UNIT
      self.ReduceDatabases(merge_db_unit)
    self.num_words_since_cutoff = 0

  def DoCutOff(self):
    start_time = time.time()
    logger.info(
      "Batch {} cutoff: sentences={}, words={}, unique_words={}".format(
        self.num_batches + 1, self.num_sentences, self.num_words,
        self.mem_phrase_count.Count()))
    it = self.mem_phrase_count.MakeIterator()
    it.First()
    min_count = math.ceil(MIN_PHRASE_COUNT_IN_BATCH / BATCH_CUTOFF_FREQ)
    while True:
      record = it.Get()
      if not record: break
      phrase = record[0].decode()
      count = struct.unpack(">q", record[1])[0]
      if count < min_count:
        it.Remove()
      else:
        it.Next()
    logger.info("Cutoff done: elapsed_time={:.2f}s, unique_phrases={}".format(
      time.time() - start_time, self.mem_phrase_count.Count()))
    self.num_words_since_cutoff = 0

  def ReduceDatabases(self, merge_db_unit):
    step = int(merge_db_unit / MERGE_DB_UNIT)
    index = self.num_batches - merge_db_unit + step - 1
    dest_index = self.num_batches - 1
    src_phrase_count_paths = []
    while index < dest_index:
      logger.info("Detected merging source ID {}".format(index))
      src_phrase_count_paths.append("{}-phrase-count-{:08d}.tks".format(self.data_prefix, index))
      index += step
    dest_phrase_count_path = "{}-phrase-count-{:08d}.tks".format(self.data_prefix, dest_index)
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


def main():
  args = sys.argv[1:]
  data_prefix = tkrzw_dict.GetCommandFlag(args, "--data_prefix", 1) or "result"
  num_ngrams = int(tkrzw_dict.GetCommandFlag(args, "--ngram", 1) or 3)
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  WordCountBatch(data_prefix, num_ngrams).Run()


if __name__=="__main__":
  main()
