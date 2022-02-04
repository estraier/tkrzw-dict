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
  "is", "are", "was", "were", "being", "had", "grey", "towards",
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
  def __init__(self, num_clusters, num_item_features, num_cluster_features):
    self.num_clusters = num_clusters
    self.num_item_features = num_item_features
    self.num_cluster_features = num_cluster_features
    self.items = {}

  def AddItem(self, label, features):
    self.items[label] = features

  def InitClusters(self):
    start_time = time.time()
    logger.info("Initializing")
    ideal_num_items = len(self.items) / self.num_clusters * 2
    items = self.items
    for cap_features in [int(self.num_item_features * 1.5), int(self.num_item_features * 1.2),
                         int(self.num_item_features * 1.1), self.num_item_features]:
      label_counts = collections.defaultdict(int)
      for word, features in items.items():
        for label in features:
          label_counts[label] += 1
      new_items = {}
      for word, features in items.items():
        max_score = 0
        adopted_features = []
        for label, score in features.items():
          count = label_counts[label]
          if count < 2: continue
          max_score = max(max_score, score)
          weight = 1 / (abs(math.log(count / ideal_num_items)) + 1)
          adopted_features.append((label, score, score * weight))
        adopted_features = sorted(
          adopted_features, key=lambda x: x[2], reverse=True)[:cap_features - 1]
        adopted_features = sorted(adopted_features, key=lambda x: x[1], reverse=True)
        new_features = {}
        new_features[word] = 0.5
        for label, score, _ in adopted_features:
          new_features[label] = score / max_score
        new_items[word] = new_features
      items = new_items
    self.items = items
    self.clusters = []
    self.cluster_features = []
    self.cluster_frozen_items = []
    for i in range(self.num_clusters):
      self.clusters.append([])
      self.cluster_features.append({})
      self.cluster_frozen_items.append({})
    scale_items = []
    for item in self.items.items():
      sum_score = 0
      for label, score in item[1].items():
        count = label_counts[label]
        weight = 1 / (abs(math.log(count / ideal_num_items)) + 1)
        sum_score += (score + 0.05) * weight
      scale_items.append((item, sum_score))
    scale_items = sorted(scale_items, key=lambda x: x[1], reverse=True)
    scale_items = [x[0] for x in scale_items]
    cands = scale_items[:self.num_clusters * 4]
    seeds = []
    seeds.append(cands[0])
    seed_words = {cands[0][0]}
    while len(seeds) < self.num_clusters:
      best_cand = None
      min_cost = None
      for cand in cands:
        cand_word, cand_features = cand
        if cand_word in seed_words: continue

        cost = 0
        for seed in seeds:
          seed_word, seed_features = seed
          cost += GetSimilarity(seed_features, cand_features)
        if min_cost == None or cost < min_cost:
          best_cand = cand
          min_cost = cost
      cand_word, cand_features = best_cand
      seeds.append(best_cand)
      seed_words.add(cand_word)
    num_items = 0
    for item in seeds:
      word, features = item
      clustr_id = num_items % self.num_clusters
      self.clusters[clustr_id].append((word, features, 0.5))
      num_items += 1
    for item in scale_items:
      word, features = item
      if word in seed_words: continue
      clustr_id = num_items % self.num_clusters
      self.clusters[clustr_id].append((word, features, 0.5))
      num_items += 1
    elapsed_time = time.time() - start_time
    logger.info("Initializing done: {:.3f} sec".format(elapsed_time))

  def UpdateClusterFeatures(self, first_mode):
    for i, cluster in enumerate(self.clusters):
      self.clusters[i] = sorted(cluster, key=lambda x: x[2], reverse=True)
    cap = math.ceil(len(self.items) / self.num_clusters)
    if first_mode:
      cap = 1
    mid = cap / 2
    mid_decay = 1.0 - 1.0 / cap
    cluster_features = []
    for i, cluster in enumerate(self.clusters):
      if self.cluster_frozen_items[i]:
        cluster_features.append(self.cluster_features[i])
        continue
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
                        key=lambda x: x[1], reverse=True)[:self.num_cluster_features]
      max_score = max([x[1] for x in features])
      top_features = {}
      for label, score in features:
        top_features[label] = score / max_score
      cluster_features.append(top_features)
    self.cluster_features = cluster_features

  def MakeClusters(self, round_id):
    start_time = time.time()
    logger.info("Clustering: round={}".format(round_id + 1))
    cap = math.ceil(len(self.items) / self.num_clusters)
    self.UpdateClusterFeatures(round_id == 0)
    cluster_weights = []
    for cluster in self.clusters:
      if cluster:
        tail_items = cluster[-cap:]
        mean_score = sum([x[2] for x in tail_items]) / len(tail_items)
      else:
        mean_score = 0.5
      cluster_weight = mean_score + 0.2
      cluster_weights.append(cluster_weight)
    self.clusters = []
    for i in range(self.num_clusters):
      self.clusters.append([])
    for word, item in self.items.items():
      best_id = 0
      best_score = -1
      best_mod_score = -1
      for i, features in enumerate(self.cluster_features):
        frozen_items = self.cluster_frozen_items[i]
        if frozen_items:
          score = frozen_items.get(word)
          if score != None:
            best_id = i
            best_score = score
            break
          else:
            continue
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
    elapsed_time = time.time() - start_time
    logger.info("Clustering done: {:.3f} sec".format(elapsed_time))

  def SmoothClusters(self, freeze):
    start_time = time.time()
    logger.info("Smoothing: freeze={}".format(freeze))
    self.UpdateClusterFeatures(False)
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
      if freeze:
        frozen_items = self.cluster_frozen_items[i]
        if not frozen_items:
          for word, _, score in cluster[:cap]:
            frozen_items[word] = score
          self.cluster_frozen_items[i] = frozen_items
      rank = 0
      for word, item_features, score in cluster[cap:]:
        extra_items.append((word, item_features, rank + score))
        rank += 1
      self.clusters[i] = cluster[:cap]
    extra_items = sorted(extra_items, key=lambda x: x[2])
    uniq_extra_words = set()
    for word, item_features, _ in extra_items:
      uniq_extra_words.add(word)
    scored_items = []
    for word, item_features, _ in extra_items:
      for i, cluster in enumerate(self.clusters):
        if len(cluster) >= cap: continue
        features =  self.cluster_features[i]
        score = GetSimilarity(features, item_features)
        mod_score = score * cluster_weights[i]
        scored_items.append((word, item_features, i, score, mod_score))
    scored_items = sorted(scored_items, key=lambda x: x[4], reverse=True)
    num_appends = 0
    for word, item_features, cluster_id, score, mod_score in scored_items:
      if word not in uniq_extra_words: continue
      cluster = self.clusters[cluster_id]
      if len(cluster) >= cap: continue
      cluster.append((word, item_features, score))
      uniq_extra_words.discard(word)
      num_appends += 1
    elapsed_time = time.time() - start_time
    logger.info("Smoothing done: {:.3f} sec".format(elapsed_time))

  def ShuffleClusters(self, round_id):
    start_time = time.time()
    logger.info("Shuffling: Round {}".format(round_id + 1))
    self.UpdateClusterFeatures(False)
    cap = math.ceil(len(self.items) / self.num_clusters)
    mid = math.ceil(len(self.items) / self.num_clusters * 0.666)
    score_cache = {}
    def GetCurrentScore(cluster_id, item_id):
      cache_id = cluster_id * self.num_clusters + item_id
      score = score_cache.get(cache_id)
      if score != None:
        return score
      score = GetSimilarity(
        self.cluster_features[cluster_id], self.clusters[cluster_id][item_id][1])
      score_cache[cache_id] = score
      return score
    done_words = set()
    for i, cluster in enumerate(self.clusters):
      features = self.cluster_features[i]
      for j in range(mid, len(cluster)):
        word, item_features, _ = self.clusters[i][j]
        if word in done_words: continue
        score = GetCurrentScore(i, j)
        plans = []
        for cand_i, cand_cluster in enumerate(self.clusters):
          cand_features = self.cluster_features[cand_i]
          move_score = GetSimilarity(cand_features, item_features)
          if cand_i == i: continue
          for cand_j in range(mid, len(cand_cluster)):
            cand_cluster = self.clusters[cand_i]
            cand_word, cand_item_features, _ = cand_cluster[cand_j]
            if cand_word in done_words: continue
            cand_score = GetCurrentScore(cand_i, cand_j)
            cand_move_score = GetSimilarity(features, cand_item_features)
            gain = move_score + cand_move_score
            loss = score + cand_score
            diff = gain - loss
            if diff >= 0.1:
              plans.append((cand_i, cand_j, diff, move_score, cand_move_score))
        if not plans: continue
        cand_i, cand_j, diff, move_score, cand_move_score = sorted(
          plans, key=lambda x: x[2], reverse=True)[0]
        cand_cluster = self.clusters[cand_i]
        cand_word, cand_item_features, _ = cand_cluster[cand_j]
        cluster[j] = (cand_word, cand_item_features, cand_move_score)
        cand_cluster[cand_j] = (word, item_features, move_score)
        done_words.add(word)
        done_words.add(cand_word)
    elapsed_time = time.time() - start_time
    logger.info("Shuffling done: {:.3f} sec".format(elapsed_time))

  def FinishClusters(self):
    start_time = time.time()
    logger.info("Finishing")
    self.UpdateClusterFeatures(False)
    for i, cluster in enumerate(self.clusters):
      if not cluster: continue
      features = collections.defaultdict(float)
      weight = 1.0
      for j in range(i, max(-1, i - 3), -1):
        for label, score in self.cluster_features[j].items():
          features[label] += score * weight
        weight *= 0.7
      features = sorted(
        features.items(), key=lambda x: x[1], reverse=True)[:self.num_cluster_features]
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
        features = sorted(
          features.items(), key=lambda x: x[1], reverse=True)[:self.num_cluster_features]
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
    elapsed_time = time.time() - start_time
    logger.info("Finishing done: {:.3f} sec".format(elapsed_time))

  def GetClusterItems(self, cluster_id):
    return self.clusters[cluster_id]


class ClusterBatch():
  def __init__(self, num_clusters, num_rounds, num_shuffles, num_items,
               num_item_features, num_cluster_features):
    self.num_clusters = num_clusters
    self.num_rounds = num_rounds
    self.num_shuffles = num_shuffles
    self.num_items = num_items
    self.num_item_features = num_item_features
    self.num_cluster_features = num_cluster_features

  def Run(self):
    start_time = time.time()
    logger.info("Process started: clusters={}, rounds={}, items={}".format(
      self.num_clusters, self.num_rounds, self.num_items))
    generator = ClusterGenerator(
      self.num_clusters, self.num_item_features, self.num_cluster_features)
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
        label = fields[i]
        score = max(float(fields[i + 1]), 0.001)
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
    generator.InitClusters()
    for round_id in range(self.num_rounds):
      if self.num_rounds >= 10:
        if round_id == int(self.num_rounds / 10 * 2):
          generator.SmoothClusters(False)
        elif round_id == int(self.num_rounds / 10 * 4):
          generator.SmoothClusters(False)
        elif round_id == int(self.num_rounds / 10 * 6):
          generator.SmoothClusters(True)
        elif round_id == int(self.num_rounds / 10 * 7):
          generator.SmoothClusters(True)
        elif round_id == int(self.num_rounds / 10 * 8):
          generator.SmoothClusters(True)
        elif round_id == int(self.num_rounds / 10 * 9):
          generator.SmoothClusters(True)
      generator.MakeClusters(round_id)
    generator.SmoothClusters(False)
    for round_id in range(self.num_shuffles):
      generator.ShuffleClusters(round_id)
    generator.FinishClusters()
    logger.info("Outputing")
    uniq_words = set()
    for i in range(self.num_clusters):
      items = generator.GetClusterItems(i)
      if not items: continue
      words = []
      for word, features, score in items:
        words.append(word)
        uniq_words.add(word)
      print("\t".join(words))
    logger.info("Process done: elapsed_time={:.2f}s, words={}".format(
      time.time() - start_time, len(uniq_words)))


def main():
  args = sys.argv[1:]
  num_clusters = int(tkrzw_dict.GetCommandFlag(args, "--clusters", 1) or 500)
  num_rounds = int(tkrzw_dict.GetCommandFlag(args, "--rounds", 1) or 100)
  num_shuffles = int(tkrzw_dict.GetCommandFlag(args, "--shuffles", 1) or 10)
  num_items = int(tkrzw_dict.GetCommandFlag(args, "--items", 1) or 10000)
  num_item_features = int(tkrzw_dict.GetCommandFlag(args, "--item_features", 1) or 40)
  num_cluster_features = int(tkrzw_dict.GetCommandFlag(args, "--cluster_features", 1) or 160)
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  ClusterBatch(num_clusters, num_rounds, num_shuffles,
               num_items, num_item_features, num_cluster_features).Run()


if __name__=="__main__":
  main()
