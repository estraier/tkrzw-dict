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
  "we", "our", "us", "ours", "some", "any", "one", "someone", "something",
  "who", "whom", "whose", "what", "where", "when", "why", "how", "and", "but", "not", "no",
  "never", "ever", "time", "place", "people", "person", "this", "that", "other", "another",
  "back", "much", "many", "more", "most", "good", "well", "better", "best", "all",
  "are",
  "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
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
    for i in range(self.num_clusters):
      self.clusters.append([])
    for i, item in enumerate(self.items.items()):
      self.clusters[i % self.num_clusters].append((item[0], item[1], 0))

  def MakeClusters(self):
    self.cluster_features = []
    for cluster in self.clusters:
      features = collections.defaultdict(float)
      for word, item, _ in cluster:
        for label, score in item.items():
          features[label] += score
      features = sorted(features.items(), key=lambda x: x[1], reverse=True)[:self.num_features]
      top_features = {}
      for label, score in features:
        top_features[label] = score
      self.cluster_features.append(top_features)
    self.clusters = []
    for i in range(self.num_clusters):
      self.clusters.append([])
    for word, item in self.items.items():
      best_id = 0
      best_score = -1
      for i, features in enumerate(self.cluster_features):
        score = GetSimilarity(features, item)
        if score > best_score:
          best_id = i
          best_score = score
      self.clusters[best_id].append((word, item, best_score))

  def SmoothClusters(self):
    cap = math.ceil(len(self.items) / self.num_clusters)
    extra_items = []
    for i, cluster in enumerate(self.clusters):
      if len(cluster) <= cap: continue
      cluster = sorted(cluster, key=lambda x: x[2], reverse=True)
      extra_items.extend(cluster[cap:])
      self.clusters[i] = cluster[:cap]
    for word, item, _ in extra_items:
      best_id = 0
      best_score = -1
      for i, cluster in enumerate(self.clusters):
        if len(cluster) > cap: continue
        features =  self.cluster_features[i]
        score = GetSimilarity(features, item)
        if score > best_score:
          best_id = i
          best_score = score
      self.clusters[best_id].append((word, item, best_score))
    for i, cluster in enumerate(self.clusters):
      if not cluster: continue
      cluster = sorted(cluster, key=lambda x: x[2], reverse=True)
      for j in range(len(cluster) - 1):
        word, item, score = cluster[j]
        best_id = 0
        best_score = -1
        for k in range(j + 1, len(cluster)):
          cand_word, cand_item, cand_score = cluster[k]
          score = GetSimilarity(item, cand_item) + cand_score
          if score > best_score:
            best_id = k
            best_score = score
        if best_id != j + 1:
          cluster[best_id], cluster[j + 1] = cluster[j + 1], cluster[best_id]
      self.clusters[i] = cluster

  def GetClusterItems(self, cluster_id):
    return self.clusters[cluster_id]


class ClusterBatch():
  def __init__(self, num_clusters, num_rounds, num_items, num_item_features, num_cluster_features):
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
    num_items = 0
    for line in sys.stdin:
      if num_items >= self.num_items: break
      fields = line.strip().split("\t")
      if len(fields) < 4: continue
      word = fields[0]
      top_word = fields[1]
      if word != top_word: continue
      fields = fields[2:]
      if len(word) <= 2: continue
      if word in STOP_WORDS: continue
      if not regex.fullmatch("[a-z]+", word): continue
      fields = fields[:self.num_item_features]
      features = {}
      for i in range(0, len(fields) - 1, 2):
        label = fields[i]
        score = float(fields[i + 1])
        features[label] = score
      generator.AddItem(word, features)
      num_items += 1
    logger.info("Initializing")
    generator.InitClusters()
    for round_id in range(self.num_rounds):
      if (self.num_rounds > 9 and
          (round_id in (int(self.num_rounds / 3), int(self.num_rounds / 3 * 2)))):
        logger.info("Smooting")
        generator.SmoothClusters()
      logger.info("Processing: Round {}".format(round_id + 1))
      generator.MakeClusters()
    logger.info("Smooting")
    generator.SmoothClusters()
    logger.info("Outputing")
    for i in range(self.num_clusters):
      items = generator.GetClusterItems(i)
      if not items: continue
      words = []
      for word, features, score in items:
        words.append(word)
      print("\t".join(words))
    logger.info("Process done: elapsed_time={:.2f}s".format(
      time.time() - start_time))


def main():
  args = sys.argv[1:]
  num_clusters = int(tkrzw_dict.GetCommandFlag(args, "--clusters", 1) or 200)
  num_rounds = int(tkrzw_dict.GetCommandFlag(args, "--rounds", 1) or 100)
  num_items = int(tkrzw_dict.GetCommandFlag(args, "--items", 1) or 10000)
  num_item_features = int(tkrzw_dict.GetCommandFlag(args, "--item_features", 1) or 32)
  num_cluster_features = int(tkrzw_dict.GetCommandFlag(args, "--cluster_features", 1) or 128)
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  ClusterBatch(num_clusters, num_rounds, num_items, num_item_features, num_cluster_features).Run()


if __name__=="__main__":
  main()
