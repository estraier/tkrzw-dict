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
    infl_index_path = data_prefix + "-infl-index.tkh"
    self.infl_index_dbm = tkrzw.DBM()
    self.infl_index_dbm.Open(infl_index_path, False, dbm="HashDBM").OrDie()
    keys_path = data_prefix + "-keys.txt"
    self.keys_file = tkrzw.TextFile()
    self.keys_file.Open(keys_path).OrDie()
    tran_keys_path = data_prefix + "-tran-keys.txt"
    self.tran_keys_file = tkrzw.TextFile()
    self.tran_keys_file.Open(tran_keys_path).OrDie()
    
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
    text = self.NormalizeText(text).strip()
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

  def SearchInflections(self, text):
    result = []
    tsv = self.infl_index_dbm.GetStr(text)
    if tsv:
      result.extend(tsv.split("\t"))
    return result

  def SearchExact(self, text, capacity):
    result = []
    uniq_words = set()
    for word in text.split(","):
      if len(result) >= capacity: break
      word = self.NormalizeText(word)
      if not word: continue
      entries = self.SearchBody(word)
      if not entries: continue
      for entry in entries:
        if len(result) >= capacity: break
        word = entry["word"]
        if word in uniq_words: continue
        uniq_words.add(word)
        result.append(entry)
    return result

  def SearchExactReverse(self, text, capacity):
    ja_words = []
    ja_uniq_words = set()
    for ja_word in text.split(","):
      ja_word = self.NormalizeText(ja_word)
      if not ja_word: continue
      if ja_word in ja_uniq_words: continue
      ja_uniq_words.add(ja_word)
      ja_words.append(ja_word)
    en_words = []
    en_uniq_words = set()
    for ja_word in ja_words:
      for en_word in self.SearchTranIndex(ja_word):
        if en_word in en_uniq_words: continue
        en_uniq_words.add(en_word)
        en_words.append(en_word)
    result = []
    uniq_words = set()
    for en_word in en_words:
      if capacity < 1: break
      entries = self.SearchBody(en_word)
      if entries:
        for entry in entries:
          if capacity < 1: break
          word = entry["word"]
          if word in uniq_words: continue
          uniq_words.add(word)
          match = False
          translations = entry.get("translation")
          if translations:
            for tran in translations:
              tran = self.NormalizeText(tran)
              if tran in ja_words:
                match = True
                break
          if match:
            result.append(entry)
            capacity -= 1
    return result

  def ExpandEntries(self, entries, capacity):
    result = []
    seeds = collections.deque()
    checked_words = set()
    checked_trans = set()
    for entry in entries:
      word = entry["word"]
      if word in checked_words: continue
      checked_words.add(word)
      seeds.append(entry)
    while seeds:
      entry = seeds.popleft()
      result.append(entry)
      max_rel_words = max(int(16 / math.log2(len(result) + 1)), 4)
      max_trans = max(int(8 / math.log2(len(result) + 1)), 3)
      rel_words = entry.get("related")
      if rel_words:
        for rel_word in rel_words[:max_rel_words]:
          if len(checked_words) >= capacity: break;
          if rel_word in checked_words: continue
          for child in self.SearchExact(rel_word, capacity - len(checked_words)):
            if len(checked_words) >= capacity: break;
            word = child["word"]
            if word in checked_words: continue
            checked_words.add(word)
            seeds.append(child)
      trans = entry.get("translation")
      if trans:
        for tran in trans[:max_trans]:
          if len(checked_words) >= capacity: break;
          if tran in checked_trans: continue
          checked_trans.add(tran)
          for child in self.SearchExactReverse(tran, capacity - len(checked_words)):
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
    for entry in self.ExpandEntries(seeds, min(int(capacity * 1.2), 100)):
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
        seeds.extend(self.SearchExact(word, capacity))
    return self.SearchRelatedWithSeeds(seeds, capacity)

  def SearchRelatedReverse(self, text, capacity):
    seeds = []
    words = text.split(",")
    for word in words:
      if word:
        seeds.extend(self.SearchExactReverse(word, capacity))
    return self.SearchRelatedWithSeeds(seeds, capacity)

  def SearchPatternMatch(self, mode, text, capacity):
    text = self.NormalizeText(text)
    keys = self.keys_file.Search(mode, text, capacity, True)
    result = []
    for key in keys:
      if len(result) >= capacity: break
      for entry in self.SearchExact(key, capacity - len(result)):
        result.append(entry)
    return result

  def SearchPatternMatchReverse(self, mode, text, capacity):
    text = self.NormalizeText(text)
    keys = self.tran_keys_file.Search(mode, text, capacity, True)
    result = []
    uniq_words = set()
    for key in keys:
      if len(result) >= capacity: break
      for entry in self.SearchExactReverse(key, capacity - len(result) + 10):
        if len(result) >= capacity: break
        word = entry["word"]
        if word in uniq_words: continue
        uniq_words.add(word)
        result.append(entry)
    return result
