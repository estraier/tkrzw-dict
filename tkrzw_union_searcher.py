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
