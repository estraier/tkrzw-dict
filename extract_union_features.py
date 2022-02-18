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

import collections
import json
import math
import regex
import sys
import tkrzw
import tkrzw_tokenizer
import tkrzw_union_searcher


def GetFeatures(searcher, entry):
  features = {}
  for label, score in searcher.GetFeatures(entry).items():
    if not regex.fullmatch("[a-z]+", label):
      score *= 0.5
    features[label] = score
  return features


def AddFeatures(searcher, word, weight, features):
  entries = searcher.SearchBody(word)
  if not entries: return
  for entry in entries:
    if entry["word"] != word: continue
    for label, score in GetFeatures(searcher, entry).items():
      if label.startswith("__"): continue
      features[label] = (features.get(label) or 0) + score * weight


tokenizer = tkrzw_tokenizer.Tokenizer()
def NormalizeTran(text):
  parts = tokenizer.StripJaParticles(text)
  if parts[0]:
    text = parts[0]
  pos = tokenizer.GetJaLastPos(text)
  if text.endswith(pos[0]) and pos[3]:
    text = text[:-len(pos[0])] + pos[3]
  return text


def main():
  args = sys.argv[1:]
  if len(args) < 2:
    raise ValueError("invalid arguments")
  data_prefix = args[0]
  phrase_path = args[1]
  searcher = tkrzw_union_searcher.UnionSearcher(data_prefix)
  phrase_dbm = tkrzw.DBM()
  phrase_dbm.Open(phrase_path, False, dbm="HashDBM").OrDie()
  parent_index = collections.defaultdict(list)
  page_index = 1
  while True:
    result = searcher.SearchByGrade(100, page_index, True)
    if not result: break
    for entry in result:
      word = entry["word"]
      prob = max(float(entry.get("probability") or 0.0), 0.000001)
      item_labels = []
      for item in entry["item"]:
        label = item["label"]
        if not label in item_labels:
          item_labels.append(label)
      if "wn" not in item_labels: continue
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
        parents.append(parent)
      for parent in parent_index.get(word) or []:
        if parent not in parents:
          parents.append(parent)
      if parents:
        weight = 1 / (min(len(parents), 5) + 1)
        for parent in parents:
          rel_words[parent] = max(rel_words.get(parent) or 0, weight)
          weight *= 0.9
      children = entry.get("child") or []
      if len(word) >= 5:
        for phrase in entry.get("phrase") or []:
          phrase_word = phrase["w"]
          if not phrase_word.startswith(word): continue
          if phrase_word.endswith("ing") or phrase_word.endswith("ed"):
            children.append(phrase_word)
      if children:
        weight = 1 / (min(len(parents), 5) + 2)
        for child in children:
          rel_words[child] = max(rel_words.get(child) or 0, weight)
          parent_index[child].append(word)
          weight *= 0.9
      related = entry.get("related") or []
      if related:
        weight = 1 / (min(len(parents), 5) + 2)
        for rel_word in related:
          rel_words[rel_word] = max(rel_words.get(rel_word) or 0, weight)
          weight *= 0.9
      synonyms = {}
      hypernyms = {}
      hyponyms = {}
      antonyms = {}
      similars = {}
      item_weight = 1.0
      for item in entry["item"]:
        if item["label"] != "wn": continue
        hit = False
        text = item["text"]
        for part in text.split("[-]"):
          match = regex.search(r"\[([a-z]+)\]: (.*)", part.strip())
          if match:
            if match.group(1) == "synonym":
              res_words = synonyms
            elif match.group(1) == "hypernym":
              res_words = hypernyms
            elif match.group(1) == "hyponym":
              res_words = hyponyms
            elif match.group(1) == "antonym":
              res_words = antonyms
            elif match.group(1) == "similar":
              res_words = similars
            else:
              continue
            order_weight = 1.0
            for rel_word in match.group(2).split(","):
              rel_word = rel_word.strip()
              if rel_word:
                weight = item_weight * order_weight
                res_words[rel_word] = max(res_words.get(rel_word) or 0, weight)
                order_weight *= 0.95
                hit = True
        if hit:
          item_weight *= 0.95
      voted_words = set()
      for cand_words, penalty, propagate in [
          (synonyms, 2, True), (hypernyms, 2, True), (hyponyms, 3, False),
          (antonyms, 3, False), (similars, 3, False)]:
        if not cand_words: continue
        type_weight = 1 / (math.log(len(cand_words)) + penalty)
        for cand_word, cand_weight in cand_words.items():
          weight = cand_weight * type_weight
          if cand_word in voted_words: continue
          voted_words.add(cand_word)
          features[cand_word] = (features.get(cand_word) or 0) + weight * 0.5
          if propagate:
            rel_words[cand_word] = max(rel_words.get(cand_word) or 0, weight)
      for rel_word, weight in rel_words.items():
        AddFeatures(searcher, rel_word, weight, features)
      features.pop(word, None)
      features.pop("wikipedia", None)
      merged_features = {}
      for label, score in features.items():
        if regex.search(r"[\p{Han}\p{Katakana}\p{Hiragana}]", label):
          label = NormalizeTran(label)
          label = regex.sub(r"[\p{Hiragana}]*(\p{Han})[\p{Hiragana}]*(\p{Han}).*", r"\1\2", label)
          label = regex.sub(r"([\p{Katakana}ー]{2,})\p{Hiragana}.*", r"\1", label)
          label = regex.sub(r"\p{Hiragana}+([\p{Katakana}ー]{2,})", r"\1", label)
        merged_features[label] = max(merged_features.get(label) or 0, score)
      features = [x for x in merged_features.items() if not x[0].startswith("__")]
      gb_words = set()
      rel_words = [x[0] for x in features]
      rel_words.append(word)
      for rel_word in rel_words:
        for gb_suffix, us_suffix in suffix_pairs:
          if rel_word.endswith(us_suffix):
            gb_word = rel_word[:-len(us_suffix)] + gb_suffix
            gb_words.add(gb_word)
      if not features: continue
      max_score = max(features, key=lambda x: x[1])[1]
      mod_features = []
      for label, score in features:
        if len(mod_features) >= 128: break
        if label in gb_words: continue
        score /= max_score
        mod_features.append((label, score))
      mod_features = sorted(mod_features, key=lambda x: x[1], reverse=True)
      fields = [word]
      fields.append(",".join(normals))
      fields.append(",".join(parents))
      fields.append(",".join(children))
      fields.append("{:.6f}".format(prob))
      for label, score in mod_features[:100]:
        fields.append(label)
        fields.append("{:.3f}".format(score))
      print("\t".join(fields))
    page_index += 1
  phrase_dbm.Close().OrDie()


if __name__=="__main__":
  main()
