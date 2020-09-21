#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Dictionary searcher of union database
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
import math
import operator
import regex
import tkrzw
import tkrzw_dict
import tkrzw_tokenizer


class UnionSearcher:
  def __init__(self, data_prefix):
    body_path = data_prefix + "-body.tkh"
    self.body_dbm = tkrzw.DBM()
    self.body_dbm.Open(body_path, False, dbm="HashDBM").OrDie()
    tran_index_path = data_prefix + "-tran-index.tkh"
    self.tran_index_dbm = tkrzw.DBM()
    self.tran_index_dbm.Open(tran_index_path, False, dbm="HashDBM").OrDie()

  def __del__(self):
    self.tran_index_dbm.Close().OrDie()
    self.body_dbm.Close().OrDie()

  _regex_spaces = regex.compile(r"[\s]+")
  def NormalizeText(self, text):
    return tkrzw_dict.NormalizeWord(self._regex_spaces.sub(" ", text).strip())

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

  def GetResultKeys(self, entries):
    keys = set()
    for entry in entries:
      keys.add(self.NormalizeText(entry["word"]))
    return keys

  def SearchExact(self, text):
    text = self.NormalizeText(text)
    result = []
    entries = self.SearchBody(text)
    norm_text = (text)
    if entries:
      result.extend(entries)
    return result

  def SearchReverse(self, text):
    text = self.NormalizeText(text)
    result = []
    src_words = self.SearchTranIndex(text)
    if src_words:
      for src_word in src_words:
        entries = self.SearchBody(src_word)
        if entries:
          for entry in entries:
            match = False
            translations = entry.get("translation")
            if translations:
              for tran in translations:
                tran = self.NormalizeText(tran)
                if tran.find(text) >= 0:
                  match = True
            if match:
              result.append(entry)
    return result

  def ExpandEntries(self, entries, capacity):
    result = []
    seeds = collections.deque()
    checked_words = set()
    for entry in entries:
      word = entry["word"]
      if word in checked_words: continue
      checked_words.add(word)
      seeds.append(entry)
    while seeds:
      entry = seeds.popleft()
      result.append(entry)
      rel_words = entry.get("related")
      if rel_words:
        for rel_word in rel_words:
          if len(checked_words) >= capacity: break;
          for child in self.SearchExact(rel_word):
            if len(checked_words) >= capacity: break;
            word = child["word"]
            if word in checked_words: continue
            checked_words.add(word)
            seeds.append(child)
      trans = entry.get("translation")
      if trans:
        for tran in trans:
          if len(checked_words) >= capacity: break;
          for child in self.SearchReverse(tran):
            if len(checked_words) >= capacity: break;
            word = child["word"]
            if word in checked_words: continue
            checked_words.add(word)
            seeds.append(child)
    return result

  def GetFeatures(self, entry):
    SCORE_DECAY = 0.95
    word = self.NormalizeText(entry["word"])
    features = {word: 1.0}
    score = 1.0
    rel_words = entry.get("related")
    if rel_words:
      for rel_word in rel_words[:20]:
        rel_word = self.NormalizeText(rel_word)
        if rel_word not in features:
          score *= SCORE_DECAY
          features[rel_word] = score
    trans = entry.get("translation")
    if trans:
      for tran in trans[:20]:
        tran = self.NormalizeText(tran)
        if tran not in features:
          score *= SCORE_DECAY
          features[tran] = score
    return features

  def GetSimilarity(self, seed_features, cand_features):
    seed_norm, cand_norm = 0.0, 0.0
    product = 0.0
    for seed_word, seed_score in seed_features.items():
      cand_score = cand_features.get(seed_word) or 0.0
      product += seed_score * cand_score
      seed_norm += seed_score ** 2
      cand_norm += cand_score ** 2
    if cand_norm == 0 or seed_norm == 0: return 0.0
    score = min(product / ((seed_norm ** 0.5) * (cand_norm ** 0.5)), 1.0)
    if score >= 0.99999: score = 1.0
    return score

  def SearchRelatedWithSeeds(self, seeds, capacity):
    seed_features = collections.defaultdict(int)
    base_weight = 1.0
    uniq_words = set()
    for seed in seeds:
      norm_word = self.NormalizeText(seed["word"])
      weight = base_weight
      if norm_word in uniq_words:
        weight *= 0.1
      uniq_words.add(norm_word)
      for word, score in self.GetFeatures(seed).items():
        seed_features[word] += score * weight
      base_weight *= 0.8
    scores = []
    bonus = 0.5
    for entry in self.ExpandEntries(seeds, min(capacity * 1.2, 100)):
      cand_features = self.GetFeatures(entry)
      score = self.GetSimilarity(seed_features, cand_features)
      score += bonus
      if "translation" not in entry:
        score -= 0.5
      scores.append((entry, score))
      bonus *= 0.95
    scores = sorted(scores, key=lambda x: x[1], reverse=True)[:capacity]
    return [x[0] for x in scores]

  def SearchRelated(self, text, capacity):
    seeds = []
    words = text.split(",")
    for word in words:
      if word:
        seeds.extend(self.SearchExact(word))
    return self.SearchRelatedWithSeeds(seeds, capacity)

  def SearchRelatedReverse(self, text, capacity):
    seeds = []
    words = text.split(",")
    for word in words:
      if word:
        seeds.extend(self.SearchReverse(word))
    return self.SearchRelatedWithSeeds(seeds, capacity)
