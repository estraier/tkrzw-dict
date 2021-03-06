#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Dictionary searcher of WordNet
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


class WordNetSearcher:
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

  def SearchExact(self, text):
    text = tkrzw_dict.NormalizeWord(text)
    result = []
    entry = self.SearchBody(text)
    if entry:
      result.append((text, entry))
    return result

  def SearchReverse(self, text):
    text = tkrzw_dict.NormalizeWord(text)
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
