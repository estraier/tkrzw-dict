#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to append WordNet Japanese translation to the WordNet database
#
# Usage:
#   append_wordnet_jpn.py [--input str] [--output str] [--wnjpn str]
#     [--word_prob str] [--tran_prob str] [--quiet]
#
# Example:
#   ./append_wordnet_jpn.py --input wordnet.tkh --output wordnet-body.tkh --wnjpn wnjpn-ok.tab
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
import regex
import sys
import time
import tkrzw
import tkrzw_dict
import tkrzw_tokenizer
import unicodedata


MAX_TRANSLATIONS_PER_WORD = 10
MIN_PROB = 0.000001


logger = tkrzw_dict.GetLogger()


class AppendWordnetJPNBatch:
  def __init__(self, input_path, output_path, wnjpn_path, word_prob_path, tran_prob_path):
    self.input_path = input_path
    self.output_path = output_path
    self.wnjpn_path = wnjpn_path
    self.word_prob_path = word_prob_path
    self.tran_prob_path = tran_prob_path

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}, wnjpn_path={}".format(
      self.input_path, self.output_path, self.wnjpn_path))
    translations = self.ReadTranslations()
    self.AppendTranslations(translations)
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def ReadTranslations(self):
    start_time = time.time()
    logger.info("Reading translations: wnjpn_path={}".format(self.wnjpn_path))
    translations = collections.defaultdict(list)
    num_translations = 0
    with open(self.wnjpn_path) as input_file:
      for line in input_file:
        line = line.strip()
        fields = line.split("\t")
        if len(fields) != 3: continue
        synset_id, text, src = fields
        text = unicodedata.normalize('NFKC', text)
        translations[synset_id].append(text)
        num_translations += 1
        if num_translations % 10000 == 0:
          logger.info("Reading translations: synsets={}, word_entries={}".format(
            len(translations), num_translations))
    logger.info(
      "Reading translations done: synsets={}, translations={}, elapsed_time={:.2f}s".format(
        len(translations), num_translations, time.time() - start_time))
    return translations

  def AppendTranslations(self, translations):
    start_time = time.time()
    logger.info("Appending translations: input_path={}, output_path={}".format(
      self.input_path, self.output_path))
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False, dbm="HashDBM").OrDie()
    word_prob_dbm, tokenizer = None, None
    if self.word_prob_path:
      word_prob_dbm = tkrzw.DBM()
      word_prob_dbm.Open(self.word_prob_path, False, dbm="HashDBM").OrDie()
      tokenizer = tkrzw_tokenizer.Tokenizer()
    tran_prob_dbm =None
    if self.tran_prob_path:
      tran_prob_dbm = tkrzw.DBM()
      tran_prob_dbm.Open(self.tran_prob_path, False, dbm="HashDBM").OrDie()
    output_dbm = tkrzw.DBM()
    num_buckets = input_dbm.Count() * 2
    output_dbm.Open(
      self.output_path, True, dbm="HashDBM", truncate=True,
      align_pow=0, num_buckets=num_buckets).OrDie()
    num_words = 0
    it = input_dbm.MakeIterator()
    it.First()
    while True:
      record = it.GetStr()
      if not record: break
      key, serialized = record
      entry = json.loads(serialized)
      items = entry["item"]
      for item in items:
        word = item["word"]
        item_translations = translations.get(item["synset"])
        if item_translations:
          if item["pos"] == "verb":
            for i, tran in enumerate(item_translations):
              if tokenizer.IsJaWordSahenNoun(tran):
                item_translations[i] = tran + "する"
          item_score = 0.0
          if word_prob_dbm or tran_prob_dbm:
            item_translations, item_score, tran_scores = (self.SortWordsByScore(
              word, item_translations, word_prob_dbm, tokenizer, tran_prob_dbm))
          item["translation"] = item_translations[:MAX_TRANSLATIONS_PER_WORD]
          if tran_scores:
            tran_score_map = {}
            for tran, tran_score in tran_scores[:MAX_TRANSLATIONS_PER_WORD]:
              tran_score_map[tran] = "{:.6f}".format(tran_score).replace("0.", ".")
            item["translation_score"] = tran_score_map
          if item_score > 0.0:
            item["score"] = "{:.6f}".format(item_score).replace("0.", ".")
      if word_prob_dbm:
        entry["item"] = sorted(
          items, key=lambda item: float(item.get("score") or 0.0), reverse=True)
      serialized = json.dumps(entry, separators=(",", ":"), ensure_ascii=False)
      output_dbm.Set(key, serialized).OrDie()
      num_words += 1
      if num_words % 10000 == 0:
        logger.info("Saving words: words={}".format(num_words))
      it.Next()
    output_dbm.Close().OrDie()
    if tran_prob_dbm:
      tran_prob_dbm.Close().OrDie()
    if word_prob_dbm:
      word_prob_dbm.Close().OrDie()
    input_dbm.Close().OrDie()
    logger.info(
      "Aappending translations done: words={}, elapsed_time={:.2f}s".format(
        num_words, time.time() - start_time))

  def GetPhraseProb(self, word_prob_dbm, tokenizer, word):
    min_prob = 1.0
    tokens = tokenizer.Tokenize("ja", word, True, True)
    for token in tokens:
      prob = float(word_prob_dbm.GetStr(token) or 0.0)
      min_prob = min(min_prob, prob)
    min_prob = max(min_prob, MIN_PROB)
    min_prob *= 0.3 ** (len(tokens) - 1)
    return min_prob

  _regex_stop_word_katakana = regex.compile(r"^[\p{Katakana}ー]+$")
  def GetTranProb(self, tran_prob_dbm, word, tran):
    max_prob = 0.0
    key = tkrzw_dict.NormalizeWord(word)
    tsv = tran_prob_dbm.GetStr(key)
    norm_tran = tran.lower()
    if tsv:
      fields = tsv.split("\t")
      for i in range(0, len(fields), 3):
        src, trg, prob = fields[i], fields[i + 1], fields[i + 2]
        if src == word and trg.lower() == norm_tran:
          prob = float(prob)
          if self._regex_stop_word_katakana.search(trg):
            prob **= 1.2
          max_prob = max(max_prob, prob)
    return max_prob

  _regex_stop_word_hiragana = regex.compile(r"^[\p{Hiragana}ー]+$")
  _regex_stop_word_single = regex.compile(r"^.$")
  def SortWordsByScore(self, word, translations, word_prob_dbm, tokenizer, tran_prob_dbm):
    scored_translations = []
    pure_translation_scores = []
    max_score = 0.0
    for tran in translations:
      prob_score = 0.0
      if word_prob_dbm:
        prob_score = self.GetPhraseProb(word_prob_dbm, tokenizer, tran)
        if tokenizer.IsJaWordSahenVerb(tran):
          stem = regex.sub(r"する$", "", tran)
          stem_prob_score = self.GetPhraseProb(word_prob_dbm, tokenizer, stem)
          prob_score = max(prob_score, stem_prob_score)
        if self._regex_stop_word_hiragana.search(tran):
          prob_score *= 0.5
        elif self._regex_stop_word_single.search(tran):
          prob_score *= 0.5
      tran_score = 0.0
      if tran_prob_dbm:
        tran_score = self.GetTranProb(tran_prob_dbm, word, tran)
        if tran_score:
          pure_translation_scores.append((tran, tran_score))
      score = max(prob_score, tran_score)
      scored_translations.append((tran, score))
      max_score = max(max_score, score)
    scored_translations = sorted(scored_translations, key=operator.itemgetter(1), reverse=True)
    score_bias = 1000 / (1000 + min(10, len(translations) - 1))
    pure_translation_scores = sorted(
      pure_translation_scores, key=operator.itemgetter(1), reverse=True)
    return ([x[0] for x in scored_translations], max_score ** score_bias, pure_translation_scores)


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "wordnet.thk"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "wordnet-body.tkh"
  wnjpn_path = tkrzw_dict.GetCommandFlag(args, "--wnjpn", 1) or "wnjpn-ok.tab"
  word_prob_path = tkrzw_dict.GetCommandFlag(args, "--word_prob", 1) or ""
  tran_prob_path = tkrzw_dict.GetCommandFlag(args, "--tran_prob", 1) or ""
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  AppendWordnetJPNBatch(
    input_path, output_path, wnjpn_path, word_prob_path, tran_prob_path).Run()


if __name__=="__main__":
  main()
