#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Dictionary searcher
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
import math
import operator
import regex
import tkrzw
import tkrzw_dict
import tkrzw_tokenizer


_regex_predict_japanese = regex.compile(r"[\p{Hiragana}\p{Katakana}ー\p{Han}]")
def PredictLanguage(text):
  if _regex_predict_japanese.search(text):
    return "ja"
  return "en"


_regex_katakana_only = regex.compile(r"^[\p{Katakana}ー]+$")
def DeduplicateWords(words):
  uniq_words = []
  norm_uniq_words = []
  for word in words:
    norm_word = tkrzw_tokenizer.RemoveDiacritic(word.lower())
    dup = False
    uniq_min_dist_ratio = 0.21
    if _regex_katakana_only.search(word):
      uniq_min_dist_ratio = 0.41
    for norm_uniq_word in norm_uniq_words:
      dist = tkrzw.Utility.EditDistanceLev(norm_word, norm_uniq_word)
      dist_ratio = dist / max(len(norm_word), len(norm_uniq_word))
      if dist_ratio < uniq_min_dist_ratio:
        dup = True
    if not dup:
      uniq_words.append(word)
      norm_uniq_words.append(norm_word)
  return uniq_words


class DictionarySearcher:
  def __init__(self, data_prefix):
    body_path = data_prefix + "-body.tkh"
    self.body_dbm = tkrzw.DBM()
    self.body_dbm.Open(body_path, False, dbm="HashDBM").OrDie()
    tran_index_path = data_prefix + "-tran-index.tkh"
    self.tran_index_dbm = tkrzw.DBM()
    self.tran_index_dbm.Open(tran_index_path, False, dbm="HashDBM").OrDie()

  def __del__(self):
    self.body_dbm.Close().OrDie()

  _regex_spaces = regex.compile(r"[\s]+")
  def NormalizeText(self, text):
    return self._regex_spaces.sub(" ", text).strip().lower()

  def SearchBody(self, text):
    serialized = self.body_dbm.GetStr(text)
    if not serialized:
      return None
    return json.loads(serialized)

  def SearchTranIndex(self, text):
    tsv = self.tran_index_dbm.GetStr(text)
    result = []
    if tsv:
      result.extend(tsv.split("\t"))
    return result

  def SearchFullMatch(self, text):
    text = self.NormalizeText(text)
    result = []
    entry = self.SearchBody(text)
    if entry:
      result.append((text, entry))
    return result

  def SearchReverse(self, text):
    text = self.NormalizeText(text)
    result = []
    src_words = self.SearchTranIndex(text)
    if src_words:
      for src_word in src_words:
        entry = self.SearchBody(src_word)
        if entry:
          items = []
          for item in entry["item"]:
            hit = False
            translations = item.get("translation")
            if translations:
              for tran in translations:
                if tran.lower() == text:
                  hit = True
                  break
            if hit:
              items.append(item)
          if items:
            entry["item"] = items
            result.append((src_word, entry))
    if len(result) > 1:
      for record in result:
        entry = record[1]
        score = float(entry.get("score") or 0.0)
        for item in entry["item"]:
          tran_scores = item.get("translation_score")
          if tran_scores:
            value = tran_scores.get(text)
            if value:
              value = float(value)
              if value > score:
                score = value
        entry["search_score"] = score
      result = sorted(result, key=lambda rec: rec[1]["search_score"], reverse=True)
    return result
