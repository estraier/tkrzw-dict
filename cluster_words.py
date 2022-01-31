#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to cluster words by k-means and cosine distance of features
#
# Usage:
#   cluster_words.py [--clusters num] [--rounds num] [--items num] [--quiet]
#   (It reads the standard input and prints the result on the standard output.)
#
# Example
#   cat union-features.tsv | ./extract_union_feedback_tran.py > union-clusters.tsv
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
import logging
import math
import os
import regex
import struct
import sys
import time
import tkrzw
import tkrzw_dict


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
  "are", "grey", "towards",
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


class ClusterGenerator():
  def __init__(self, num_clusters, num_features):
    self.num_clusters = num_clusters
    self.num_features = num_features
    self.items = {}

  def AddItem(self, label, features):
    self.items[label] = features

  def InitClusters(self):
    self.clusters = []
    self.cluster_features = []
    for i in range(self.num_clusters):
      self.clusters.append([])
      self.cluster_features.append({})
    for i, item in enumerate(self.items.items()):
      self.clusters[i % self.num_clusters].append((item[0], item[1], 0.5))

  def MakeClusters(self):
    cap = math.ceil(len(self.items) / self.num_clusters)
    cluster_weights = []
    for cluster in self.clusters:
      if cluster:
        tail_items = cluster[-cap:]
        mean_score = sum([x[2] for x in tail_items]) / len(tail_items)
      else:
        mean_score = 0.5
      cluster_weight = mean_score + 0.2
      cluster_weights.append(cluster_weight)
    mid = cap / 2
    mid_decay = 1.0 - 1.0 / cap
    cluster_features = []
    for i, cluster in enumerate(self.clusters):
      features = collections.defaultdict(float)
      weight = 1.0
      num_items = 0
      for word, item, _ in cluster:
        if num_items >= cap: break
        if num_items > mid:
          weight *= mid_decay
        for label, score in item.items():
          features[label] += score * weight
        num_items += 1
      mod_features = collections.defaultdict(float)
      if features:
        max_score = max([x[1] for x in features.items()])
        for label, score in features.items():
          mod_features[label] = score / max_score
      for label, score in self.cluster_features[i].items():
        mod_features[label] += score * 0.2
      features = sorted(mod_features.items(),
                        key=lambda x: x[1], reverse=True)[:self.num_features]
      max_score = max([x[1] for x in features])
      top_features = {}
      for label, score in features:
        top_features[label] = score / max_score
      cluster_features.append(top_features)
    self.cluster_features = cluster_features
    self.clusters = []
    for i in range(self.num_clusters):
      self.clusters.append([])
    for word, item in self.items.items():
      best_id = 0
      best_score = -1
      best_mod_score = -1
      for i, features in enumerate(self.cluster_features):
        score = GetSimilarity(features, item)
        mod_score = score * cluster_weights[i]
        if mod_score > best_mod_score:
          best_id = i
          best_score = score
          best_mod_score = mod_score
      self.clusters[best_id].append((word, item, best_score))
    num_overs = 0
    for i, cluster in enumerate(self.clusters):
      if len(cluster) > cap:
        num_overs += len(cluster) - cap
      self.clusters[i] = sorted(cluster, key=lambda x: x[2], reverse=True)

  def SmoothClusters(self):
    cap = math.ceil(len(self.items) / self.num_clusters)
    mid = math.ceil(len(self.items) / self.num_clusters / 2)
    cluster_weights = []
    for cluster in self.clusters:
      if cluster:
        tail_items = cluster[-mid:]
        mean_score = sum([x[2] for x in tail_items]) / len(tail_items)
      else:
        mean_score = 0.5
      cluster_weight = mean_score + 0.2
      cluster_weights.append(cluster_weight)
    extra_items = []
    for i, cluster in enumerate(self.clusters):
      if len(cluster) <= cap: continue
      rank = 0
      for word, item_features, score in cluster[cap:]:
        extra_items.append((word, item_features, rank + score))
        rank += 1
      self.clusters[i] = cluster[:cap]
    extra_items = sorted(extra_items, key=lambda x: x[2])
    for word, item_features, _ in extra_items:
      best_id = 0
      best_score = -1
      best_mod_score = -1
      for i, cluster in enumerate(self.clusters):
        if len(cluster) >= cap: continue
        features =  self.cluster_features[i]
        score = GetSimilarity(features, item_features)
        mod_score = score * cluster_weights[i]
        if mod_score > best_mod_score:
          best_id = i
          best_score = score
          best_mod_score = mod_score
      self.clusters[best_id].append((word, item_features, best_score))

  def FinishClusters(self):
    for i, cluster in enumerate(self.clusters):
      if not cluster: continue
      features = collections.defaultdict(float)
      weight = 1.0
      for j in range(i, max(-1, i - 3), -1):
        for label, score in self.cluster_features[j].items():
          features[label] += score * weight
        weight *= 0.7
      features = sorted(features.items(), key=lambda x: x[1], reverse=True)[:self.num_features]
      top_features = {}
      for label, score in features:
        top_features[label] = score
      best_id = i + 1
      best_score = -1
      for j in range(i + 1, len(self.clusters)):
        cand_cluster = self.clusters[j]
        cand_features = self.cluster_features[j]
        score = GetSimilarity(top_features, cand_features)
        if score > best_score:
          best_id = j
          best_score = score
      if best_id != i + 1:
        tmp_cluster = self.clusters[best_id]
        self.clusters[best_id] = self.clusters[i + 1]
        self.clusters[i + 1] = tmp_cluster
        tmp_features = self.cluster_features[best_id]
        self.cluster_features[best_id] = self.cluster_features[i + 1]
        self.cluster_features[i + 1] = tmp_features
    for i, cluster in enumerate(self.clusters):
      if not cluster: continue
      cluster = sorted(cluster, key=lambda x: x[2], reverse=True)
      for j in range(len(cluster) - 1):
        features = collections.defaultdict(float)
        weight = 1.0
        for k in range(j, max(-1, j - 3), -1):
          _, item, _ = cluster[k]
          for label, score in item.items():
            features[label] += score * weight
          weight *= 0.7
        top_features = {}
        features = sorted(features.items(), key=lambda x: x[1], reverse=True)[:self.num_features]
        for label, score in features:
          top_features[label] = score
        best_id = j + 1
        best_score = -1
        for k in range(j + 1, len(cluster)):
          _, cand_item, cand_score = cluster[k]
          score = GetSimilarity(top_features, cand_item) + cand_score
          if score > best_score:
            best_id = k
            best_score = score
        if best_id != j + 1:
          cluster[best_id], cluster[j + 1] = cluster[j + 1], cluster[best_id]
      self.clusters[i] = cluster

  def GetClusterItems(self, cluster_id):
    return self.clusters[cluster_id]


class ClusterBatch():
  def __init__(self, num_clusters, num_rounds, num_items,
               num_item_features, num_cluster_features):
    self.num_clusters = num_clusters
    self.num_rounds = num_rounds
    self.num_items = num_items
    self.num_item_features = num_item_features
    self.num_cluster_features = num_cluster_features

  def Run(self):
    start_time = time.time()
    logger.info("Process started: clusters={}, rounds={}, items={}".format(
      self.num_clusters, self.num_rounds, self.num_items))
    generator = ClusterGenerator(self.num_clusters, self.num_cluster_features)
    max_read_items = self.num_items * 2.0 + 100
    all_items = []
    ranks = {}
    for line in sys.stdin:
      if len(all_items) >= max_read_items: break
      fields = line.strip().split("\t")
      if len(fields) < 6: continue
      word = fields[0]
      normals = fields[1]
      parents = fields[2]
      children = fields[3]
      if normals: continue
      fields = fields[4:]
      if len(word) <= 2 and word not in ("go", "ax", "ox", "pi"): continue
      if word in STOP_WORDS: continue
      if not regex.fullmatch("[a-z]+", word): continue
      features = {}
      for i in range(0, len(fields) - 1, 2):
        if len(features) >= self.num_item_features: break
        label = fields[i]
        score = float(fields[i + 1])
        features[label] = score
      all_items.append((word, parents, features))
      ranks[word] = len(all_items)
    num_items = 0
    num_skipped = 0
    adopted_words = set()
    for word, parents, features in all_items:
      if num_items >= self.num_items: break
      is_dup = False
      for parent in parents.split(","):
        parent = parent.strip()
        if not parent: continue
        if parent in adopted_words:
          is_dup = True
        parent_rank = ranks.get(parent)
        if parent_rank and parent_rank <= self.num_items + num_skipped:
          is_dup = True
      if is_dup:
        num_skipped += 1
        continue
      generator.AddItem(word, features)
      adopted_words.add(word)
      num_items += 1
    logger.info("Initializing")
    generator.InitClusters()
    for round_id in range(self.num_rounds):
      if (self.num_rounds > 9 and
          (round_id in (int(self.num_rounds / 3), int(self.num_rounds / 3 * 2)))):
        logger.info("Smoothing")
        generator.SmoothClusters()
      logger.info("Processing: Round {}".format(round_id + 1))
      generator.MakeClusters()
    logger.info("Smoothing")
    generator.SmoothClusters()
    logger.info("Finishing")
    generator.FinishClusters()
    logger.info("Outputing")
    num_output_words = 0
    for i in range(self.num_clusters):
      items = generator.GetClusterItems(i)
      if not items: continue
      words = []
      for word, features, score in items:
        words.append(word)
        num_output_words += 1
      print("\t".join(words))
    logger.info("Process done: elapsed_time={:.2f}s, words={}".format(
      time.time() - start_time, num_output_words))


def main():
  args = sys.argv[1:]
  num_clusters = int(tkrzw_dict.GetCommandFlag(args, "--clusters", 1) or 500)
  num_rounds = int(tkrzw_dict.GetCommandFlag(args, "--rounds", 1) or 30)
  num_items = int(tkrzw_dict.GetCommandFlag(args, "--items", 1) or 10000)
  num_item_features = int(tkrzw_dict.GetCommandFlag(args, "--item_features", 1) or 40)
  num_cluster_features = int(tkrzw_dict.GetCommandFlag(args, "--cluster_features", 1) or 160)
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  ClusterBatch(num_clusters, num_rounds, num_items, num_item_features, num_cluster_features).Run()


if __name__=="__main__":
  main()
