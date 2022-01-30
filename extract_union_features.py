#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to extract features of words
#
# Usage:
#   extract_union_features.py data_prefix phrasedbm
#   (It prints the result on the standard output.)
#
# Example
#   ./extract_union_feedback_tran.py union enunion-phrase-prob.tkh > union-features.tsv
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
import regex
import sys
import tkrzw
import tkrzw_union_searcher


def GetFeatures(searcher, entry):
  features = {}
  for label, score in searcher.GetFeatures(entry).items():
    if not regex.fullmatch("[a-z]+", label):
      score *= 0.5
    features[label] = score
  return features


def AddFeatures(searcher, word, weight, core_prob, features):
  entries = searcher.SearchBody(word)
  if not entries: return
  for entry in entries:
    if entry["word"] != word: continue
    prob = max(float(entry.get("probability") or 0.0), 0.000001)
    ratio = min(prob / core_prob, 1.0)
    for label, score in GetFeatures(searcher, entry).items():
      if label.startswith("__"): continue
      features[label] = (features.get(label) or 0) + score * weight * (ratio ** 0.5)


def main():
  args = sys.argv[1:]
  if len(args) < 2:
    raise ValueError("invalid arguments")
  data_prefix = args[0]
  phrase_path = args[1]
  searcher = tkrzw_union_searcher.UnionSearcher(data_prefix)
  phrase_dbm = tkrzw.DBM()
  phrase_dbm.Open(phrase_path, False, dbm="HashDBM").OrDie()
  page_index = 1
  while True:
    result = searcher.SearchByGrade(100, page_index, True)
    if not result: break
    for entry in result:
      word = entry["word"]
      prob = max(float(entry.get("probability") or 0.0), 0.000001)
      features = GetFeatures(searcher, entry)
      rel_words = {}
      normals = []
      alternatives = entry.get("alternative") or []
      suffix_pairs = [("se", "ze"), ("ence", "ense"), ("isation", "ization"),
                      ("our", "or"), ("og", "ogue"), ("re", "er"), ("l", "ll")]
      for gb_suffix, us_suffix in suffix_pairs:
        if word.endswith(gb_suffix):
          us_word = word[:-len(gb_suffix)] + us_suffix
          if us_word in normals: continue
          if us_word in alternatives and searcher.CheckExact(us_word):
            normals.append(us_word)
      for alt in alternatives:
        if alt in normals: continue
        if word.count(" ") == alt.count(" "): continue
        dist = tkrzw.Utility.EditDistanceLev(word, alt)
        similar = False
        if dist == 1 and word[:3] != alt[:3]:
          similar = True
        elif dist == 2 and word[:5] == alt[:5] and word[-2:] == alt[-2:]:
          similar = True
        if similar and searcher.CheckExact(alt):
          word_prob = float(phrase_dbm.GetStr(word) or "0")
          alt_prob = float(phrase_dbm.GetStr(alt) or "0")
          if alt_prob > word_prob * 2:
            normals.append(alt)
      parents = []
      for parent in entry.get("parent") or []:
        parent_entries = searcher.SearchBody(parent)
        if not parent_entries: continue
        parent_prob = 0
        for parent_entry in parent_entries:
          if parent_entry["word"] != parent: continue
          parent_prob = float(parent_entry["probability"] or "0")
        if parent_prob < prob * 0.1:
          continue
        parents.append(parent)
      if parents:
        weight = 1 / (min(len(parents) + 1, 5))
        for parent in parents:
          rel_words[parent] = max(rel_words.get(parent) or 0, weight)
          weight *= 0.9
      children = entry.get("child") or []
      if children:
        weight = 1 / (min(len(children) + 2, 5))
        for child in children:
          rel_words[child] = max(rel_words.get(child) or 0, weight)
          weight *= 0.9
      related = entry.get("related") or []
      if related:
        weight = 1 / (min(len(related) + 2, 5))
        for rel_word in related:
          rel_words[rel_word] = max(rel_words.get(rel_word) or 0, weight)
          weight *= 0.9
      for rel_word, weight in rel_words.items():
        AddFeatures(searcher, rel_word, weight, prob, features)
      features = [x for x in features.items() if not x[0].startswith("__")]
      max_score = max(features, key=lambda x: x[1])[1]
      mod_features = []
      for label, score in features[:100]:
        score /= max_score
        mod_features.append((label, score))
      mod_features = sorted(mod_features, key=lambda x: x[1], reverse=True)
      fields = [word]
      fields.append(",".join(normals))
      fields.append(",".join(parents))
      fields.append(",".join(children))
      for label, score in mod_features[:100]:
        fields.append(label)
        fields.append("{:.3f}".format(score))
      print("\t".join(fields))
    page_index += 1
  phrase_dbm.Close().OrDie()


if __name__=="__main__":
  main()
