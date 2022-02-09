#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to classify words to clusters
#
# Usage:
#   classify_words.py [--feature str] [--cluster str] [--total_words num]
#     [--item_features num] [--cluster_features num] [--quiet]
#
# Example:
#   ./generate_union_kindle_vocab.py --clusters union-features.txt --clusters union-clusters.txt
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
import copy
import datetime
import html
import json
import logging
import math
import os
import pathlib
import regex
import sys
import time
import tkrzw
import tkrzw_dict
import tkrzw_tokenizer
import urllib
import uuid


STOP_WORDS = set([
  "aback", "about", "above", "abroad", "across", "after", "against", "ahead", "along",
  "amid", "among", "apart", "around", "as", "at", "away", "back", "before", "behind",
  "below", "beneath", "between", "beside", "beyond", "by", "despite", "during", "down",
  "except", "for", "forth", "from", "in", "inside", "into", "near", "of", "off", "on",
  "onto", "out", "outside", "over", "per", "re", "since", "than", "through", "throughout",
  "till", "to", "together", "toward", "under", "until", "up", "upon", "with", "within",
  "without", "via",
  "the", "a", "an", "I", "my", "me", "mine", "you", "your", "yours", "he", "his", "him",
  "she", "her", "hers", "it", "its", "they", "their", "them", "theirs",
  "myself", "yourself", "yourselves", "himself", "herself", "itself", "themselves",
  "we", "our", "us", "ours", "some", "any", "one", "someone", "something",
  "who", "whom", "whose", "what", "where", "when", "why", "how", "and", "but", "not", "no",
  "never", "ever", "time", "place", "people", "person", "this", "these", "that", "those",
  "other", "another", "yes",
  "back", "much", "many", "more", "most", "good", "well", "better", "best", "all",
  "bes", "is", "are", "was", "were", "being", "had", "grey", "towards",
  "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
  "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "nineteen",
  "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety", "hundred",
  "first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "ninth", "tenth",
  "eleventh", "twelfth", "thirteenth", "fourteenth", "fifteenth", "sixteenth", "seventeenth",
  "nineteenth", "twenties", "thirties", "forties", "fifties", "sixties", "seventies", "eighties",
  "nineties", "hundredth",
])


logger = tkrzw_dict.GetLogger()


def GetSimilarity(seed_features, cand_features):
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


class ClassifyBatch:
  def __init__(self, feature_path, cluster_path,
               num_total_words, num_item_features, num_cluster_features, num_extra_items):
    self.feature_path = feature_path
    self.cluster_path = cluster_path
    self.num_total_words = num_total_words
    self.num_item_features = num_item_features
    self.num_cluster_features = num_cluster_features
    self.num_extra_items = num_extra_items

  def Run(self):
    items = self.ReadFeatures()
    item_dict = {}
    for item in items:
      item_dict[item[0]] = item[1]
    clusters = self.ReadClusters(item_dict)
    self.AddItems(clusters, items)
    self.OutputClusters(clusters)
    
  def ReadFeatures(self):
    items = []
    with open(self.feature_path) as input_file:
      for line in input_file:
        if len(items) >= self.num_total_words: break
        fields = line.strip().split("\t")
        if len(fields) < 4: continue
        word = fields[0]
        normals = fields[1]
        parents = fields[2]
        children = fields[3]
        word_prob = fields[4]
        if normals or parents: continue
        fields = fields[5:]
        features = {}
        for i in range(0, len(fields), 2):
          if len(features) >= self.num_item_features: break
          label, score = fields[i], float(fields[i + 1])
          features[label] = score
        items.append((word, features))
    return items

  def ReadClusters(self, item_dict):
    clusters = []
    with open(self.cluster_path) as input_file:
      for line in input_file:
        fields = line.strip().split("\t")
        if not fields: continue
        words = []
        features = {}
        for word in fields:
          item_features = item_dict.get(word) or {}
          words.append(word)
          for label, score in item_features.items():
            features[label] = (features.get(label) or 0) + score
        features = sorted(features.items(), key=lambda x: x[1], reverse=True)
        mod_features = {}
        for label, score in features[:self.num_cluster_features]:
          mod_features[label] = score
        clusters.append((words, mod_features, []))
    return clusters

  def AddItems(self, clusters, items):
    uniq_words = set()
    for words, _, _ in clusters:
      for word in words:
        uniq_words.add(word)
    for word, item_features in items:
      if len(word) <= 2 and word not in ("go", "ax", "ox", "pi"): continue
      if word in STOP_WORDS: continue
      if not regex.fullmatch("[a-z]+", word): continue
      if word in uniq_words: continue
      uniq_words.add(word)
      best_id = -1
      best_score = -1
      for i, cluster in enumerate(clusters):
        if len(cluster[2]) >= self.num_extra_items: continue
        words, features, extra = cluster
        score = GetSimilarity(features, item_features)
        if score > best_score:
          best_id = i
          best_score = score
      if best_id >= 0:
        clusters[best_id][2].append(word)

  def OutputClusters(self, clusters):
    num_extra = 0
    for cluster in clusters:
      fields = []
      fields.extend(cluster[0])
      fields.append("|")
      fields.extend(cluster[2])
      num_extra += len(cluster[2])
      print("\t".join(fields))


def main():
  args = sys.argv[1:]
  feature_path = tkrzw_dict.GetCommandFlag(args, "--feature", 1) or ""
  cluster_path = tkrzw_dict.GetCommandFlag(args, "--cluster", 1) or ""
  num_total_words = float(tkrzw_dict.GetCommandFlag(args, "--total_words", 1) or 100000)
  num_item_features = int(tkrzw_dict.GetCommandFlag(args, "--item_features", 1) or 32)
  num_cluster_features = int(tkrzw_dict.GetCommandFlag(args, "--cluster_features", 1) or 128)
  num_extra_items = int(tkrzw_dict.GetCommandFlag(args, "--extra_items", 1) or 15)
  if not feature_path:
    raise RuntimeError("the feature path is required")
  if not cluster_path:
    raise RuntimeError("the cluster path is required")
  ClassifyBatch(
    feature_path, cluster_path,
    num_total_words, num_item_features, num_cluster_features, num_extra_items).Run()


if __name__=="__main__":
  main()
