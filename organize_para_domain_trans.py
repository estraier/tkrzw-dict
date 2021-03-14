#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to count parallel phrases by the domain
#
# Usage:
#   organize_para_domain_trans.py
#   (It reads the standard input and prints the result on the standard output.)
#
# Example:
#   $ organize_para_domain_trans.py < jpara-scores.txt > jpara-tran.tsv
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
import os
import regex
import sys
import time
import tkrzw
import tkrzw_dict
import tkrzw_tokenizer
import unicodedata

EF_WEIGHT = 1.0
FE_WEIGHT = 1.0
MIN_PROB = 0.000001


logger = tkrzw_dict.GetLogger()


def Run(rev_prob_path, min_count, enough_ef, enough_fe, omit_latin,
        min_score, min_score_large, min_score_stop, max_targets, tran_aux_paths):
  start_time = time.time()
  logger.info("Process started")
  aux_trans = collections.defaultdict(list)
  for tran_aux_path in tran_aux_paths:
    if not tran_aux_path: continue
    logger.info("Reading: " + tran_aux_path)
    with open(tran_aux_path) as input_file:
      for line in input_file:
        fields = line.strip().split("\t")
        if len(fields) < 2: continue
        source = fields[0]
        targets = set()
        for target in fields[1:]:
          target = unicodedata.normalize('NFKC', target)
          target = regex.sub(r"[\p{Ps}\p{Pe}\p{C}]", "", target)
          target = regex.sub(r"\s+", " ", target).strip()
          if target:
            aux_trans[source].append(target)
  rev_prob_dbm = None
  if rev_prob_path:
    logger.info("Reading: " + rev_prob_path)
    rev_prob_dbm = tkrzw.DBM()
    rev_prob_dbm.Open(rev_prob_path, False, dbm="HashDBM").OrDie()
  logger.info("Processing records")
  records = {}
  for line in sys.stdin:
    fields = line.strip().split("\t")
    if len(fields) < 3: continue
    source = fields[0]
    count = int(fields[1])
    targets = []
    for field in fields[2:]:
      columns = field.split("|")
      if len(columns) != 3: continue
      targets.append((columns[0], float(columns[1]), float(columns[2])))
    records[source] = (count, targets)
  for source, (count, targets) in records.items():
    if count < min_count: continue
    if len(source) <= 1: continue
    large = bool(regex.search(r"^\p{Lu}", source))
    if large:
      cap_source = source.lower()
    else:
      cap_source = source[0].upper() + source[1:]
    cap_count, cap_targets = 0, []
    if cap_source != source:
      cap_record = records.get(cap_source)
      if cap_record:
        cap_count, cap_targets = cap_record
    if large:
      cap_count *= 5.0
    if count < cap_count: continue
    scored_targets = []
    for target, ef_prob, fe_prob in targets:
      for cap_target, cap_ef_prob, cap_fe_prob in cap_targets:
        if cap_target == target:
          fe_prob += cap_fe_prob
      ef_prob = min(1.0, ef_prob)
      fe_prob = min(1.0, fe_prob)
      score = ((ef_prob ** EF_WEIGHT) * (fe_prob ** FE_WEIGHT)) ** (1 / (EF_WEIGHT + FE_WEIGHT))
      #score = 2 * ef_prob * fe_prob / (ef_prob + fe_prob)
      scored_targets.append((target, score, ef_prob, fe_prob))
    scored_targets = sorted(scored_targets, key=lambda x: x[1], reverse=True)
    source_aux_trans = aux_trans.get(source) or []
    good_targets = []
    for target, score, ef_prob, fe_prob in scored_targets:
      if target in source_aux_trans:
        score *= 1.1
      else:
        is_prefix = False
        is_single_noun = False
        for cmp_target, cmp_score, _, _ in scored_targets:
          if target != cmp_target and cmp_target.startswith(target) and cmp_score >= min_score:
            if (cmp_target == target + "の" or cmp_target == target + "する") and regex.fullmatch(r"\p{Han}+", target):
              is_single_noun = True
            else:
              is_prefix = True
        is_stop = bool(regex.fullmatch(r"[\p{Hiragana}]+", target))
        if omit_latin and regex.search(r"[\p{Latin}]{2,}", target):
          continue
        if len(target) <= 1 and is_prefix and not is_single_noun:
          continue
        if large:
          if score < min_score_large:
            continue
        elif is_stop:
          if score < min_score_stop:
            continue
        else:
          if score < min_score:
            if (regex.search(r"[\p{Latin}]{4,}", source) and not regex.search(r"\d", source) and
                (regex.search(r"[\p{Han}]{2,}", target) or
                 regex.search(r"[\p{Han}][\p{Hiragana}]", target)) and
                (ef_prob >= enough_ef or fe_prob >= enough_fe)):
              pass
            else:
              continue
        norm_source = source.lower()
        norm_target = target.lower()
        if norm_source.find(norm_target) >= 0 or norm_target.find(norm_source) >= 0:
          continue
        if norm_target in ("する", "ます", "より", "から"):
          continue
        if norm_target.startswith("っ") or norm_target.startswith("を"):
          continue
        if norm_target.endswith("っ") or norm_target.endswith("を"):
          continue
        if regex.fullmatch(r"[\p{Hiragana}ー{Latin}]", target):
          continue
        if regex.search(r"^[\p{Hiragana}]+[\p{Han}\p{Katakana}\p{Latin}]", target):
          continue
        elif regex.search(r"[\p{Han}\{Katakana}ー\p{Latin}][は|が|を|と]", target):
          continue
      if len(target) <= 1:
        score *= 0.5
      elif len(target) <= 2:
        score *= 0.9
      if regex.fullmatch(r"[\p{Hiragana}ー]+", target):
        score *= 0.8
      elif regex.search(r"\d", target):
        score *= 0.8
      target = regex.sub(r"([\p{Han}\p{Katakana}ー\p{Latin}])だ", r"\1な", target)
      good_targets.append((target, score, ef_prob, fe_prob))
    if not good_targets: continue
    good_targets = sorted(good_targets, key=lambda x: x[1], reverse=True)
    outputs = []
    for target, score, ef_prob, fe_prob in good_targets[:max_targets]:
      if rev_prob_dbm:
        prob = GetPhraseProb(rev_prob_dbm, "ja", target)
        if prob < MIN_PROB:
          continue
      #outputs.append("{}:{:.3f}:{:.3f}:{:.3f}".format(target, score, ef_prob, fe_prob))
      outputs.append(target)
    if outputs:
      print("{}\t{}".format(source, "\t".join(outputs)))
  if rev_prob_dbm:
    rev_prob_dbm.Close().OrDie()
  logger.info("Process done: elapsed_time={:.2f}s".format(
    time.time() - start_time))


tokenizer = tkrzw_tokenizer.Tokenizer()
def GetPhraseProb(prob_dbm, language, word):
  base_prob = 0.000000001
  tokens = tokenizer.Tokenize(language, word, False, True)
  if not tokens: return base_prob
  max_ngram = min(3, len(tokens))
  fallback_penalty = 1.0
  for ngram in range(max_ngram, 0, -1):
    if len(tokens) <= ngram:
      cur_phrase = " ".join(tokens)
      prob = float(prob_dbm.GetStr(cur_phrase) or 0.0)
      if prob:
        return max(prob, base_prob)
      fallback_penalty *= 0.1
    else:
      probs = []
      index = 0
      miss = False
      while index <= len(tokens) - ngram:
        cur_phrase = " ".join(tokens[index:index + ngram])
        cur_prob = float(prob_dbm.GetStr(cur_phrase) or 0.0)
        if not cur_prob:
          miss = True
          break
        probs.append(cur_prob)
        index += 1
      if not miss:
        inv_sum = 0
        for cur_prob in probs:
          inv_sum += 1 / cur_prob
        prob = len(probs) / inv_sum
        prob *= 0.3 ** (len(tokens) - ngram)
        prob *= fallback_penalty
        return max(prob, base_prob)
      fallback_penalty *= 0.1
  return base_prob


def main():
  args = sys.argv[1:]
  rev_prob_path = tkrzw_dict.GetCommandFlag(args, "--rev_prob", 1) or ""
  min_count = int(tkrzw_dict.GetCommandFlag(args, "--min_count", 1) or 10)
  enough_ef = float(tkrzw_dict.GetCommandFlag(args, "--enough_ef", 1) or 2.0)
  enough_fe = float(tkrzw_dict.GetCommandFlag(args, "--enough_fe", 1) or 2.0)
  omit_latin = tkrzw_dict.GetCommandFlag(args, "--omit_latin", 0)
  min_score = float(tkrzw_dict.GetCommandFlag(args, "--min_score", 1) or 0.25)
  min_score_large = float(tkrzw_dict.GetCommandFlag(args, "--min_score_large", 1) or 0.35)
  min_score_stop = float(tkrzw_dict.GetCommandFlag(args, "--min_score_stop", 1) or 0.3)
  max_targets = int(tkrzw_dict.GetCommandFlag(args, "--max_targets", 1) or 8)
  tran_aux_paths = (tkrzw_dict.GetCommandFlag(args, "--tran_aux", 1) or "").split(",")
  Run(rev_prob_path, min_count, enough_ef, enough_fe, omit_latin,
      min_score, min_score_large, min_score_stop, max_targets, tran_aux_paths)


if __name__=="__main__":
  main()
