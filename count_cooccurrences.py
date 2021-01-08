#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to count words and their cooccurrences
#
# Usage:
#   count_cooccurrences.py [--data_prefix str] [--language str]
#   (It reads the standard input and makes files in the data directory.)
#
# Example:
#   $ bzcat enwiki-tokenized.tsv.bz2 |
#     ./count_cooccurrences.py --data_prefix enwiki --language en
#   $ bzcat jawiki-tokenized.tsv.bz2 |
#     ./count_cooccurrences.py --data_prefix jawiki --language ja
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
SCORE_DECAY = 0.95
SENTENCE_GAP_PENALTY = 0.5
WINDOW_SIZE = 20
#BATCH_MAX_WORDS = 5000000  # for 1GB RAM usage
BATCH_MAX_WORDS = 100000000  # for 10GB RAM usage
BATCH_CUTOFF_FREQ = 4
MIN_WORD_COUNT_IN_BATCH = 16
MIN_COOC_COUNT_IN_BATCH = 4
MAX_COOC_PER_WORD = 256
MERGE_DB_UNIT = 16
PROB_CACHE_CAPACITY = 50000


logger = tkrzw_dict.GetLogger()


class WordCountBatch:
  def __init__(self, data_prefix, language):
    self.data_prefix = data_prefix
    self.language = language
    self.num_batches = 0

  def Run(self):
    start_time = time.time()
    logger.info("Process started: data_prefix={}, language={}".format(
      self.data_prefix, self.language))
    self.Start()
    num_documents, num_sentences, num_words = 0, 0, 0
    for line in sys.stdin:
      line = line.strip()
      sentences = line.split("\t")
      if not sentences: continue
      sentences = sentences[:MAX_SENTENCES_PER_DOC]
      num_documents += 1
      num_sentences += len(sentences)
      document = []
      for sentence in sentences:
        sentence = sentence.lower()
        words = sentence.split(" ")
        num_words += len(words)
        document.append(words)
      if num_documents % 100 == 0:
        logger.info(
          "Processing: documents={}, sentences={}, words={}, RSS={:.2f}MB".format(
            num_documents, num_sentences, num_words,
            tkrzw.Utility.GetMemoryUsage() / 1024.0 / 1024))
      self.FeedDocument(document)
    self.Finish(num_sentences)
    logger.info(
      "Process done: documents={}, sentences={}, words={}, elapsed_time={:.2f}s".format(
        num_documents, num_sentences, num_words, time.time() - start_time))

  def Start(self):
    self.mem_word_count = tkrzw.DBM()
    self.mem_word_count.Open("", True, dbm="BabyDBM").OrDie()
    self.mem_cooc_count = tkrzw.DBM()
    self.mem_cooc_count.Open("", True, dbm="BabyDBM").OrDie()
    self.num_documents = 0
    self.num_sentences = 0
    self.num_words = 0
    self.num_words_since_cutoff = 0
    self.start_time = time.time()

  def FeedDocument(self, document):
    words = self.MartializeWords(document)
    self.num_documents += 1
    self.num_sentences += len(document)
    self.num_words += len(words)
    self.num_words_since_cutoff += len(words)
    uniq_word_pairs = set()
    for word_index, word_pair in enumerate(words):
      if word_pair in uniq_word_pairs:
        continue
      uniq_word_pairs.add(word_pair)
      word, sentence_index = word_pair
      cooc_word_index = max(word_index - WINDOW_SIZE, 0)
      max_word_index = min(word_index + WINDOW_SIZE, len(words) - 1)
      scores = {}
      while cooc_word_index <= max_word_index:
        cooc_word, cooc_sentence_index = words[cooc_word_index]
        if cooc_word != word:
          diff = abs(word_index - cooc_word_index) - 1
          score = tkrzw_dict.COOC_BASE_SCORE * (SCORE_DECAY ** diff)
          if cooc_sentence_index != sentence_index:
            score *= SENTENCE_GAP_PENALTY
          scores[cooc_word] = max((scores.get(cooc_word) or 0), int(score))
        cooc_word_index += 1
      self.RegisterWords(word, scores)
    if self.num_words >= BATCH_MAX_WORDS:
      self.Dump()
      self.Start()
    elif self.num_words_since_cutoff >= BATCH_MAX_WORDS / BATCH_CUTOFF_FREQ:
      self.DoCutOff()

  def Finish(self, total_num_sentences):
    if self.num_words:
      self.Dump()
    self.mem_word_count = None
    self.mem_cooc_count = None
    word_count_paths = []
    cooc_count_paths = []
    for index in range(0, self.num_batches):
      word_count_path = "{}-word-count-{:08d}.tks".format(self.data_prefix, index)
      cooc_count_path = "{}-cooc-count-{:08d}.tks".format(self.data_prefix, index)
      if os.path.isfile(word_count_path):
        logger.info("Detected merging ID {}".format(index))
        word_count_paths.append(word_count_path)
        cooc_count_paths.append(cooc_count_path)
    if len(word_count_paths) > 1:
      logger.info("Merging word count databases")
      src_word_count_paths = word_count_paths[:-1]
      dest_word_count_path = word_count_paths[-1]
      self.MergeDatabases(src_word_count_paths, dest_word_count_path)
    else:
      dest_word_count_path = word_count_paths[0]
    if len(cooc_count_paths) > 1:
      logger.info("Merging cooccurrence count databases")
      src_cooc_count_paths = cooc_count_paths[:-1]
      dest_cooc_count_path = cooc_count_paths[-1]
      self.MergeDatabases(src_cooc_count_paths, dest_cooc_count_path)
    else:
      dest_cooc_count_path = cooc_count_paths[0]
    word_count_path = tkrzw_dict.GetWordCountPath(self.data_prefix)
    cooc_count_path = tkrzw_dict.GetCoocCountPath(self.data_prefix)
    logger.info("Finishing {} batches: word_count_path={}, cooc_count_path={}".format(
      self.num_batches, word_count_path, cooc_count_path))
    os.rename(dest_word_count_path, word_count_path)
    os.rename(dest_cooc_count_path, cooc_count_path)

  def MartializeWords(self, document):
    words = []
    for i, doc_words in enumerate(document):
      for word in doc_words:
        words.append((word, i))
    return words

  def RegisterWords(self, word, scores):
    self.mem_word_count.Increment(word)
    for score in scores.items():
      pair_key = word + " " + score[0]
      self.mem_cooc_count.Increment(pair_key, score[1])

  def Dump(self):
    logger.info("Batch {} aggregation done: elapsed_time={:.2f}s, RSS={:.2f}MB".format(
      self.num_batches + 1, time.time() - self.start_time,
      tkrzw.Utility.GetMemoryUsage() / 1024.0 / 1024))
    logger.info(
      ("Batch {} dumping: documents={}, sentences={}, words={}," +
       " unique_words={}, unique_cooc={}").format(
         self.num_batches + 1, self.num_documents, self.num_sentences, self.num_words,
         self.mem_word_count.Count(), self.mem_cooc_count.Count()))
    start_time = time.time()
    fill_ratio = min(self.num_words / BATCH_MAX_WORDS, 1.0)
    dbm_cooc_count_path = "{}-cooc-count-{:08d}.tks".format(self.data_prefix, self.num_batches)
    dbm_cooc_count = tkrzw.DBM()
    dbm_cooc_count.Open(
      dbm_cooc_count_path, True, dbm="SkipDBM",
      truncate=True, insert_in_order=True, offset_width=5, step_unit=16, max_level=8).OrDie()
    logger.info("Batch {} cooc count dumping: dest={}".format(
      self.num_batches + 1, dbm_cooc_count_path))
    dbm_cooc_count.Set("", self.num_sentences).OrDie()
    it = self.mem_cooc_count.MakeIterator()
    it.First()
    min_word_count = math.ceil(MIN_WORD_COUNT_IN_BATCH * fill_ratio)
    if MIN_WORD_COUNT_IN_BATCH >= 2:
      min_word_count = max(min_word_count, 2)
    min_count = math.ceil(tkrzw_dict.COOC_BASE_SCORE * MIN_COOC_COUNT_IN_BATCH * fill_ratio)
    cur_word = None
    cur_word_count = 0
    cur_word_weight = 1.0
    cooc_words = []
    while True:
      record = it.Get()
      if not record: break
      word_pair = record[0].decode()
      count = struct.unpack(">q", record[1])[0]
      word, cooc_word = word_pair.split(" ")
      if cur_word != word:
        if cur_word and cooc_words:
          self.DumpCoocWords(cur_word, cooc_words, dbm_cooc_count)
        cur_word = word
        cur_word_count = struct.unpack(">q", self.mem_word_count.Get(cur_word))[0]
        cur_word_weight = 1.0
        if tkrzw_dict.IsNumericWord(cur_word):
          cur_word_weight = tkrzw_dict.NUMERIC_WORD_WEIGHT
        elif tkrzw_dict.IsStopWord(self.language, cur_word):
          cur_word_weight = tkrzw_dict.STOP_WORD_WEIGHT
        cooc_words = []
      if cur_word_count * cur_word_weight >= min_word_count:
        cooc_count = struct.unpack(">q", self.mem_word_count.Get(cooc_word))[0]
        cooc_weight = 1.0
        if tkrzw_dict.IsNumericWord(cooc_word):
          cooc_weight = tkrzw_dict.NUMERIC_WORD_WEIGHT
        elif tkrzw_dict.IsStopWord(self.language, cooc_word):
          cooc_weight = tkrzw_dict.STOP_WORD_WEIGHT
        cooc_prob = cooc_count / self.num_sentences
        cooc_idf = min(math.log(cooc_prob) * -1, tkrzw_dict.MAX_IDF_WEIGHT)
        score = count * (cooc_idf ** tkrzw_dict.IDF_POWER)
        score *= cur_word_weight * cooc_weight
        if (cooc_count * cooc_weight >= min_word_count and
            count * cur_word_weight * cooc_weight >= min_count):
          cooc_words.append((cooc_word, count, score))
      it.Remove()
    if cur_word and cooc_words:
      self.DumpCoocWords(cur_word, cooc_words, dbm_cooc_count)
    dbm_cooc_count.Close().OrDie()
    dbm_word_count_path = "{}-word-count-{:08d}.tks".format(self.data_prefix, self.num_batches)
    dbm_word_count = tkrzw.DBM()
    dbm_word_count.Open(
      dbm_word_count_path, True, dbm="SkipDBM",
      truncate=True, insert_in_order=True, offset_width=4, step_unit=4, max_level=12).OrDie()
    logger.info("Batch {} word count dumping: dest={}".format(
      self.num_batches + 1, dbm_word_count_path))
    dbm_word_count.Set("", self.num_sentences).OrDie()
    it = self.mem_word_count.MakeIterator()
    it.First()
    while True:
      record = it.Get()
      if not record:
        break
      word = record[0]
      count = struct.unpack(">q", record[1])[0]
      if count >= min_word_count:
        dbm_word_count.Set(word, count).OrDie()
      it.Remove()
    dbm_word_count.Close().OrDie()
    logger.info("Dumping done: elapsed_time={:.2f}s".format(time.time() - start_time))
    self.num_batches += 1
    merge_db_unit = 1
    while self.num_batches % (merge_db_unit * MERGE_DB_UNIT) == 0:
      merge_db_unit *= MERGE_DB_UNIT
      self.ReduceDatabases(merge_db_unit)
    self.num_words_since_cutoff = 0

  def DumpCoocWords(self, word, cooc_words, dbm_cooc_count):
    top_cooc_words = sorted(
      cooc_words, key=operator.itemgetter(2), reverse=True)[:MAX_COOC_PER_WORD]
    records = []
    for cooc_word, count, score in top_cooc_words:
      pair_key = (word + " " + cooc_word).encode()
      records.append((pair_key, int(count)))
    for pair_key, count in sorted(records):
      dbm_cooc_count.Set(pair_key, count).OrDie()

  def DoCutOff(self):
    start_time = time.time()
    logger.info(
      ("Batch {} cutoff: documents={}, sentences={}, words={}," +
       " unique_words={}, unique_cooc={}").format(
         self.num_batches + 1, self.num_documents, self.num_sentences, self.num_words,
         self.mem_word_count.Count(), self.mem_cooc_count.Count()))
    it = self.mem_cooc_count.MakeIterator()
    it.First()
    min_count = math.ceil(tkrzw_dict.COOC_BASE_SCORE * MIN_COOC_COUNT_IN_BATCH / BATCH_CUTOFF_FREQ)
    cur_word = None
    cur_word_weight = 1.0
    while True:
      record = it.Get()
      if not record: break
      word_pair = record[0].decode()
      count = struct.unpack(">q", record[1])[0]
      word, cooc_word = word_pair.split(" ")
      if cur_word != word:
        cur_word = word
        cur_word_weight = 1.0
        if tkrzw_dict.IsNumericWord(cur_word):
          cur_word_weight = tkrzw_dict.NUMERIC_WORD_WEIGHT
        elif tkrzw_dict.IsStopWord(self.language, cur_word):
          cur_word_weight = tkrzw_dict.STOP_WORD_WEIGHT
      cooc_word_weight = 1.0
      if tkrzw_dict.IsNumericWord(cooc_word):
        cur_word_weight = tkrzw_dict.NUMERIC_WORD_WEIGHT
      elif tkrzw_dict.IsStopWord(self.language, cooc_word):
        cur_word_weight = tkrzw_dict.STOP_WORD_WEIGHT
      if count * cur_word_weight * cooc_word_weight < min_count:
        it.Remove()
      else:
        it.Next()
    logger.info("Cutoff done: elapsed_time={:.2f}s, unique_cooc={}".format(
      time.time() - start_time, self.mem_cooc_count.Count()))
    self.num_words_since_cutoff = 0

  def ReduceDatabases(self, merge_db_unit):
    step = int(merge_db_unit / MERGE_DB_UNIT)
    index = self.num_batches - merge_db_unit + step - 1
    dest_index = self.num_batches - 1
    src_word_count_paths = []
    src_cooc_count_paths = []
    while index < dest_index:
      logger.info("Detected merging source ID {}".format(index))
      src_word_count_paths.append("{}-word-count-{:08d}.tks".format(self.data_prefix, index))
      src_cooc_count_paths.append("{}-cooc-count-{:08d}.tks".format(self.data_prefix, index))
      index += step
    dest_word_count_path = "{}-word-count-{:08d}.tks".format(self.data_prefix, dest_index)
    dest_cooc_count_path = "{}-cooc-count-{:08d}.tks".format(self.data_prefix, dest_index)
    logger.info("Merging word count DBM files to {}".format(dest_word_count_path))
    start_time = time.time()
    self.MergeDatabases(src_word_count_paths, dest_word_count_path)
    logger.info("Merging done: elapsed_time={:.2f}s".format(time.time() - start_time))
    logger.info("Merging cooccurrence count DBM files to {}".format(dest_cooc_count_path))
    start_time = time.time()
    self.MergeDatabases(src_cooc_count_paths, dest_cooc_count_path)
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
  language = tkrzw_dict.GetCommandFlag(args, "--language", 1) or "en"
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  WordCountBatch(data_prefix, language).Run()


if __name__=="__main__":
  main()
