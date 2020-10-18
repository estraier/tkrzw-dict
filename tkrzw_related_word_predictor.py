#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Predictor for related words by cooccurrences
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

import math
import operator
import tkrzw
import tkrzw_dict
import tkrzw_tokenizer


class RelatedWordsPredictor:
  TRACE_COOC_WORDS = 32
  CHECK_COOC_WORDS = 16
  CHECK_REL_WORDS = 128
  NUM_FEATURES = 256

  def __init__(self, data_prefix, language):
    self.language = language
    self.tokenizer = tkrzw_tokenizer.Tokenizer()
    word_score_path = tkrzw_dict.GetCoocScorePath(data_prefix);
    self.word_score_dbm = tkrzw.DBM()
    self.word_score_dbm.Open(word_score_path, False, dbm="HashDBM").OrDie()

  def __del__(self):
    self.word_score_dbm.Close().OrDie()

  def Predict(self, text):
    words = set(self.tokenizer.Tokenize(self.language, text, True, False))
    if len(words) > 1:
      words.add(tkrzw_dict.NormalizeWord(text))
    cooc_words = {}
    for word in words:
      for cooc_word, cooc_score in self.GetCoocWords(word):
        cooc_words[cooc_word] = (cooc_words.get(cooc_word) or 0) + cooc_score
    sorted_cooc_words = sorted(cooc_words.items(), key=operator.itemgetter(1), reverse=True)
    rel_words = {}
    num_traces = 0
    for cooc_word, cooc_score in sorted_cooc_words:
      if num_traces >= self.TRACE_COOC_WORDS: break
      if cooc_word in words: continue
      for rel_word, rel_score in self.GetCoocWords(cooc_word):
        if rel_word in words: continue
        rel_words[rel_word] = max(rel_words.get(rel_word) or 0, cooc_score * rel_score)
      num_traces += 1
    sorted_rel_words = sorted(rel_words.items(), key=operator.itemgetter(1), reverse=True)
    check_words = set(words)
    num_cooc_checked = 0
    for cooc_word, _ in sorted_cooc_words:
      if num_cooc_checked >= self.CHECK_COOC_WORDS: break
      if cooc_word in check_words: continue
      check_words.add(cooc_word)
      num_cooc_checked += 1
    num_rel_checked = 0
    for rel_word, _ in sorted_rel_words:
      if num_rel_checked >= self.CHECK_REL_WORDS: break
      if rel_word in check_words: continue
      check_words.add(rel_word)
      num_rel_checked += 1
    scored_rel_words = []
    for rel_word in check_words:
      rel_cooc_words = self.GetCoocWords(rel_word)
      score = self.GetSimilarity(sorted_cooc_words, rel_cooc_words)
      scored_rel_words.append((rel_word, score))
    scored_rel_words = sorted(scored_rel_words, key=operator.itemgetter(1), reverse=True)
    return scored_rel_words, sorted_cooc_words

  def GetCoocWords(self, word):
    cooc_words = []
    tsv = self.word_score_dbm.GetStr(word)
    if not tsv: return cooc_words
    fields = tsv.split("\t")
    idf = int(fields[0])
    score = tkrzw_dict.MAX_PROB_SCORE * idf * idf / (tkrzw_dict.COOC_BASE_SCORE ** 2)
    if tkrzw_dict.IsNumericWord(word):
      score *= tkrzw_dict.NUMERIC_WORD_WEIGHT
    elif tkrzw_dict.IsStopWord(self.language, word):
      score *= tkrzw_dict.STOP_WORD_WEIGHT
    cooc_words.append((word, score))
    for field in fields[1:]:
      cooc_word, cooc_score = field.split(" ")
      cooc_score = int(cooc_score) * idf / (tkrzw_dict.COOC_BASE_SCORE ** 2)
      if tkrzw_dict.IsNumericWord(cooc_word):
        cooc_score *= tkrzw_dict.NUMERIC_WORD_WEIGHT
      elif tkrzw_dict.IsStopWord(self.language, cooc_word):
        cooc_score *= tkrzw_dict.STOP_WORD_WEIGHT
      cooc_words.append((cooc_word, cooc_score))
    return cooc_words

  def GetSoftMax(self, scored_words):
    sum = 0.0
    for word, score in scored_words:
      sum += math.exp(score)
    result = {}
    if sum == 0:
      for word, score in scored_words:
        result[word] = 0.0
    else:
      for word, score in scored_words:
        result[word] = math.exp(score) / sum
    return result

  def GetSimilarity(self, seed_cooc_words, rel_cooc_words):
    rel_cooc_map = dict(rel_cooc_words)
    seed_norm, rel_norm = 0.0, 0.0
    product = 0.0
    for word, seed_score in seed_cooc_words[:self.NUM_FEATURES]:
      rel_score = rel_cooc_map.get(word) or 0.0
      product += seed_score * rel_score
      seed_norm += seed_score ** 2
      rel_norm += rel_score ** 2
    if seed_norm == 0 or rel_norm == 0: return 0.0
    score = min(product / ((seed_norm ** 0.5) * (rel_norm ** 0.5)), 1.0)
    if score >= 0.99999: score = 1.0
    return score
