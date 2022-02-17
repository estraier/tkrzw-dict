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
import heapq
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
  "bes", "is", "are", "was", "were", "being", "had", "grey", "towards",
  "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
  "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "nineteen",
  "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety", "hundred",
  "first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "ninth", "tenth",
  "eleventh", "twelfth", "thirteenth", "fourteenth", "fifteenth", "sixteenth", "seventeenth",
  "nineteenth", "twentieth", "thirtieth", "fortieth", "fiftieth", "sixtieth", "seventieth",
  "eightieth", "ninetieth", "hundredth",
  "northeast", "northwest", "southeast", "southwest",
  "math", "maths", "disc",
])
no_parents = {
  "number", "ground", "red", "happen", "letter", "monitor", "feed", "winter", "brake",
  "partner", "sister", "environment", "moment", "gun", "shower", "trigger", "wound", "bound",
  "weed", "saw", "copper", "buffer", "lump", "wary", "stove", "doctor", "hinder", "crazy",
  "tower", "poetry", "parity", "fell", "lay", "bit", "drug", "grass", "shore",
  "butter", "slang", "grope", "feces", "left", "former", "found", "every", "scheme",
  "evening", "architecture", "hat", "slice", "bite", "tender", "bully", "translate",
  "fence", "liver", "special", "specific", "species", "statistics", "mathematics", "caution",
  "span", "fleet", "language",
  "shine", "dental", "irony", "transplant", "chemistry", "physics", "grocery",
  "gutter", "dove", "weary", "queer", "shove", "buggy", "twine", "tier", "rung", "spat",
  "pang", "jibe", "pent", "lode", "gelt", "plant", "plane", "pants", "craze", "grove",
  "downy", "musty", "mangy", "moped", "caper", "balmy", "tinny", "induce", "treaty",
  "chili", "chilli", "chile", "castor", "landry", "start", "baby", "means", "transfer",
  "interior", "exterior", "rabbit", "stripe", "fairy", "shunt", "clove", "abode", "bends",
  "molt", "holler", "feudal", "bounce", "livery", "wan", "sod", "dug", "het", "gat",
  "cover", "book", "cause", "quality", "process", "provide",
}
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
  def __init__(self, num_clusters, num_rounds, num_item_features, num_cluster_features):
    self.num_clusters = num_clusters
    self.num_rounds = num_rounds
    self.num_item_features = num_item_features
    self.num_cluster_features = num_cluster_features
    self.items = {}

  def AddItem(self, label, features):
    self.items[label] = features

  def Run(self):
    self.InitClusters()
    for round_id in range(self.num_rounds):
      num_init_items = None
      center_bias = False
      if round_id < self.num_rounds / 2 and round_id < 8:
        num_init_items = round_id % 4 + 1
        center_bias = True
      elif round_id < self.num_rounds / 3 and round_id < 16:
        center_bias = True
      self.MakeClusters(round_id, num_init_items, center_bias)
    self.FinishClusters()

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
        adopted_features = []
        for label, score in features.items():
          count = label_counts[label]
          if count < 2: continue
          if label.count(" ") > 0 and count < 5: continue
          weight = 1 / (abs(math.log(count / ideal_num_items)) + 1)
          adopted_features.append((label, score, score * weight))
        adopted_features = sorted(
          adopted_features, key=lambda x: x[2], reverse=True)[:cap_features - 1]
        scored_features = []
        for label, score, weighted_score in adopted_features:
          if cap_features == self.num_item_features:
            scored_features.append((label, weighted_score))
          else:
            scored_features.append((label, score))
        scored_features = sorted(scored_features, key=lambda x: x[1], reverse=True)
        max_score = scored_features[0][1]
        new_features = {}
        new_features[word] = 1.0
        for label, score in scored_features:
          if label == word: continue
          new_features[label] = score / max_score
        new_items[word] = new_features
      items = new_items
    self.items = items
    self.clusters = []
    self.cluster_features = []
    for i in range(self.num_clusters):
      self.clusters.append([])
      self.cluster_features.append({})
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
    max_cands = int(self.num_clusters * 4)
    cands = scale_items[:max_cands]
    seeds = []
    first_cand = cands[0]
    seeds.append(first_cand)
    seed_words = {first_cand[0]}
    seed_features = collections.defaultdict(float)
    for label, score in first_cand[1].items():
      seed_features[label] = score
    while len(seeds) < self.num_clusters:
      best_cand = None
      min_cost = None
      for cand in cands:
        cand_word, cand_features = cand
        if cand_word in seed_words: continue
        cost = GetSimilarity(seed_features, cand_features)
        if min_cost == None or cost < min_cost:
          best_cand = cand
          min_cost = cost
      cand_word, cand_features = best_cand
      seeds.append(best_cand)
      seed_words.add(cand_word)
      for label, score in cand_features.items():
        seed_features[label] += score
      if len(seeds) >= 100:
        _, del_features = seeds[len(seeds) - 100]
        for label, score in del_features.items():
          old_score = seed_features[label] - score
          if old_score < 0.01:
            del seed_features[label]
          else:
            seed_features[label] = score
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

  def UpdateClusterFeatures(self, num_init_items, center_bias):
    for i, cluster in enumerate(self.clusters):
      self.clusters[i] = sorted(cluster, key=lambda x: x[2], reverse=True)
    cap = math.ceil(len(self.items) / self.num_clusters)
    if num_init_items:
      cap = num_init_items
    cluster_features = []
    for i, cluster in enumerate(self.clusters):
      features = collections.defaultdict(float)
      num_items = 0
      for word, item, weight in cluster:
        if num_items >= cap: break
        if not center_bias:
          weight = 1.0
        for label, score in item.items():
          features[label] += score * (weight + 0.1)
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

  def MakeClusters(self, round_id, num_init_items, center_bias):
    start_time = time.time()
    num_init_items_label = num_init_items if num_init_items else "all"
    logger.info("Clustering: round={}, items={}, center_bias={}".format(
      round_id + 1, num_init_items_label, center_bias))
    cap = math.ceil(len(self.items) / self.num_clusters)
    self.UpdateClusterFeatures(num_init_items, center_bias)
    self.clusters = []
    for i in range(self.num_clusters):
      self.clusters.append([])
    items = self.items.items()
    score_matrix = []
    for word, item_features in items:
      item_scores = []
      for cluster_id, cluster_features in enumerate(self.cluster_features):
        score = GetSimilarity(cluster_features, item_features)
        item_scores.append((score, cluster_id))
      item_scores = sorted(item_scores, reverse=True)
      score_matrix.append(item_scores)
    queues = []
    for i in range(self.num_clusters):
      queues.append(([], set()))
    tasks = []
    for i, item in enumerate(items):
      tasks.append((i, item))
    while tasks:
      item_id, item = tasks.pop()
      item_scores = score_matrix[item_id]
      for score, cluster_id in item_scores:
        queue, checked = queues[cluster_id]
        if item_id in checked: continue
        checked.add(item_id)
        if len(queue) < cap:
          heapq.heappush(queue, (score, item_id, item))
          break
        if score > queue[0][0]:
          old_score, old_item_id, old_item = heapq.heappop(queue)
          tasks.append((old_item_id, old_item))
          heapq.heappush(queue, (score, item_id, item))
          break
    clusters = []
    for queue, checked in queues:
      sorted_items = []
      for score, item_id, item in queue:
        sorted_items.append((score, item))
      sorted_items = sorted(sorted_items, reverse=True)
      cluster = []
      for score, item in sorted_items:
        cluster.append((item[0], item[1], score))
      clusters.append(cluster)
    self.clusters = clusters
    sum_score = 0
    for i, cluster in enumerate(self.clusters):
      features = self.cluster_features[i]
      for item_word, item_features, item_score in cluster:
        score = GetSimilarity(features, item_features)
        sum_score += score
    mean_score = sum_score / len(self.items)
    elapsed_time = time.time() - start_time
    logger.info("Clustering done: {:.3f} sec, score={:.4f}".format(
      elapsed_time, mean_score))

  def FinishClusters(self):
    start_time = time.time()
    logger.info("Finishing")
    self.UpdateClusterFeatures(None, False)
    cluster_records = []
    for i, cluster in enumerate(self.clusters):
      if not cluster: continue
      features = self.cluster_features[i]
      sum_score = 0
      new_items = []
      for j, item in enumerate(cluster):
        item_word, item_features, _ = item
        score = GetSimilarity(features, item_features)
        sum_score += score
        new_items.append((item_word, item_features, score))
      new_items = sorted(new_items, key=lambda x: x[2], reverse=True)
      mean_score = sum_score / len(new_items)
      cluster_records.append((mean_score, new_items, features))
    cluster_records = sorted(cluster_records, key=lambda x: x[0], reverse=True)
    self.clusters = []
    self.cluster_features = []
    cluster_scores = []
    for score, cluster, features in cluster_records:
      self.clusters.append(cluster)
      self.cluster_features.append(features)
      cluster_scores.append(score)
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
        score += cluster_scores[j] * 0.2
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
        tmp_score = cluster_scores[best_id]
        cluster_scores[best_id] = cluster_scores[i + 1]
        cluster_scores[i + 1] = tmp_score
    for i, cluster in enumerate(self.clusters):
      if not cluster: continue
      if i > 0:
        sum_features = collections.defaultdict(float)
        for features in [self.cluster_features[i], self.cluster_features[i - 1]]:
          for label, score in features.items():
            sum_features[label] += score
        tmp_cluster = []
        for word, item, score in cluster:
          tmp_score = GetSimilarity(sum_features, item)
          tmp_cluster.append((word, item, score, tmp_score))
        tmp_cluster = sorted(tmp_cluster, key=lambda x: x[3], reverse=True)
        cluster = [(x[0], x[1], x[2]) for x in tmp_cluster]
      else:
        cluster = sorted(cluster, key=lambda x: x[2], reverse=True)
      for j in range(1, len(cluster) - 1):
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
    generator = ClusterGenerator(
      self.num_clusters, self.num_rounds, self.num_item_features, self.num_cluster_features)
    max_read_items = self.num_items * 3.0 + 5000
    words = []
    item_dict = {}
    rank_dict = {}
    for line in sys.stdin:
      if len(words) >= max_read_items: break
      fields = line.strip().split("\t")
      if len(fields) < 6: continue
      word = fields[0]
      normals = fields[1]
      parents = fields[2]
      children = fields[3]
      word_prob = fields[4]
      if normals: continue
      fields = fields[5:]
      if len(word) <= 2 and word not in ("go", "ax", "ox", "pi"): continue
      if word in STOP_WORDS: continue
      if not regex.fullmatch("[a-z]+", word): continue
      features = {}
      for i in range(0, len(fields) - 1, 2):
        label = fields[i]
        score = max(float(fields[i + 1]), 0.001)
        if label in STOP_WORDS:
          score *= 0.5
        features[label] = score
      words.append(word)
      item_dict[word] = (parents, features)
      rank_dict[word] = len(rank_dict)
    parent_index = collections.defaultdict(list)
    for rank, word in enumerate(words):
      parent_expr, features = item_dict[word]
      parents = []
      for parent in parent_expr.split(","):
        if not parent: continue
        parents.append(parent)
        parent_index[word].append((parent, 0, rank))
    for i in range(3):
      for child, parents in list(parent_index.items()):
        for parent, level, rank in parents:
          grand_parents = parent_index.get(parent)
          if grand_parents:
            for grand_parent, grand_level, grand_rank in grand_parents:
              parent_index[child].append((grand_parent, grand_level + 1, grand_rank))
    single_parent_index = {}
    for child, parents in parent_index.items():
      parents = sorted(parents, key=lambda x: x[1] * len(words) - x[2], reverse=True)
      single_parent_index[child] = parents[0][0]
    for nop_word in no_parents:
      single_parent_index.pop(nop_word, None)
    adopted_words = {}
    skipped_words = set()
    for word in words:
      if len(adopted_words) >= self.num_items: break
      _, features = item_dict[word]
      parent = single_parent_index.get(word)
      if parent:
        parent_features = item_dict.get(parent)
        if parent_features:
          _, parent_features = parent_features
          word = parent
          features = parent_features
      if word in adopted_words: continue
      adopted_words[word] = features
    for word, features in adopted_words.items():
      norm_features = {}
      for label, score in features.items():
        norm_label = single_parent_index.get(label) or label
        norm_features[norm_label] = max(norm_features.get(norm_label) or 0, score)
      generator.AddItem(word, norm_features)
    generator.Run()
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
  num_items = int(tkrzw_dict.GetCommandFlag(args, "--items", 1) or 10000)
  num_item_features = int(tkrzw_dict.GetCommandFlag(args, "--item_features", 1) or 40)
  num_cluster_features = int(tkrzw_dict.GetCommandFlag(args, "--cluster_features", 1) or 160)
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  ClusterBatch(num_clusters, num_rounds,
               num_items, num_item_features, num_cluster_features).Run()


if __name__=="__main__":
  main()
