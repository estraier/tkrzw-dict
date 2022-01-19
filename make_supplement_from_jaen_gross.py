#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to parse the Japense-English grossary file to make a supplement TSV file
#
# Usage:
#   make_supplement_from_jaen_gross.py [--phrase_prob str] [--tran_prob str]
#   (It reads the standard input and prints the result on the standard output.)
#
# Example:
#   $ cat wiktionary-gross-jaen.tsv edict2-gross.txt |
#     ./make_supplement_from_jaen_gross.py > supplement.tsv
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
import regex
import sys
import tkrzw
import tkrzw_dict
import tkrzw_tokenizer


logger = tkrzw_dict.GetLogger()


def Run(phrase_prob_path, rev_prob_path,
        tran_prob_path, tran_aux_paths, yomi_paths, min_phrase_prob, min_tran_prob):
  logger.info("Start the process")
  phrase_prob_dbm = None
  if phrase_prob_path:
    logger.info("Opening the phrase prob DB: " + phrase_prob_path)
    phrase_prob_dbm = tkrzw.DBM()
    phrase_prob_dbm.Open(phrase_prob_path, False, dbm="HashDBM").OrDie()
  rev_prob_dbm = None
  if rev_prob_path:
    logger.info("Opening the reverse prob DB: " + rev_prob_path)
    rev_prob_dbm = tkrzw.DBM()
    rev_prob_dbm.Open(rev_prob_path, False, dbm="HashDBM").OrDie()
  tran_prob_dbm = None
  if tran_prob_path:
    logger.info("Opening the tran prob DB: " + tran_prob_path)
    tran_prob_dbm = tkrzw.DBM()
    tran_prob_dbm.Open(tran_prob_path, False, dbm="HashDBM").OrDie()
  aux_trans = collections.defaultdict(list)
  for tran_aux_path in tran_aux_paths.split(","):
    tran_aux_path = tran_aux_path.strip()
    if tran_aux_path:
      logger.info("Reading the tran aux file: " + tran_aux_path)
      with open(tran_aux_path) as input_file:
        uniq_keys = set()
        for line in input_file:
          fields = line.strip().split("\t")
          if len(fields) < 2: continue
          word = fields[0]
          for tran in fields[1:]:
            uniq_key = word + ":" + tran
            if uniq_key in uniq_keys: continue
            aux_trans[word].append(tran)
            uniq_keys.add(uniq_key)
  yomis = set()
  for yomi_path in yomi_paths.split(","):
    yomi_path = yomi_path.strip()
    if yomi_path:
      logger.info("Reading the yomi file: " + yomi_path)
      with open(yomi_path) as input_file:
        for line in input_file:
          fields = line.strip().split("\t")
          if len(fields) < 1: continue
          yomis.add(fields[0])
  logger.info("Processing the gross.")
  tokenizer = tkrzw_tokenizer.Tokenizer()
  word_dict = collections.defaultdict(list)
  alt_source = None
  alt_targets = None
  num_lines = 0
  for line in sys.stdin:
    num_lines += 1
    if num_lines % 10000 == 0:
      logger.info("Processing the gross: {} lines: {} items".format(
        num_lines, len(word_dict)))
    fields = line.strip().split("\t")
    if len(fields) != 3: continue
    word, pos, text = fields
    if pos == "alternative":
      alt_source = word
      alt_targets = set()
      for alt in regex.split(r"[,;]", text):
        if regex.fullmatch(r"[\p{Han}\p{Hiragana}\p{Katakana}ー]+", alt):
          alt_targets.add(alt)
      continue
    text = regex.sub(r"\.$", "", text).strip()
    for tran in regex.split(r"[,;]", text):
      tran = tran.strip()
      if pos == "verb":
        tran = regex.sub(r"^to ", "", tran)
      if pos == "noun":
        tran = regex.sub(r"^(a|an|the) ", "", tran)
      if not regex.fullmatch(r"[-_\p{Latin}0-9'. ]+", tran): continue
      tokens = tran.split(" ")
      if len(tokens) < 1 or len(tokens) > 4: continue
      word_dict[tran].append((pos, word))
      if alt_source == word:
        for alt in alt_targets:
          word_dict[tran].append((pos, alt))
  norm_word_dict = collections.defaultdict(list)
  for word, trans in word_dict.items():
    scored_trans, phrase_prob = ProcessWord(
      word, trans, tokenizer, phrase_prob_dbm, rev_prob_dbm, tran_prob_dbm, aux_trans, yomis,
      min_phrase_prob, min_tran_prob)
    if scored_trans:
      key = tkrzw_dict.NormalizeWord(word)
      norm_word_dict[key].append((word, scored_trans, phrase_prob))
  for key, entries in norm_word_dict.items():
    sum_phrase_prob = 0.0
    for word, scored_trans, phrase_prob in entries:
      sum_phrase_prob += phrase_prob
    for word, scored_trans, phrase_prob in entries:
      if sum_phrase_prob > 0:
        if key == word:
          if phrase_prob / sum_phrase_prob < 0.6: continue
        else:
          if phrase_prob / sum_phrase_prob < 0.8: continue
      PrintEntry(word, scored_trans)
  if tran_prob_dbm:
    tran_prob_dbm.Close().OrDie()
  if phrase_prob_dbm:
    phrase_prob_dbm.Close().OrDie()
  logger.info("Process done")


def ProcessWord(word, trans, tokenizer, phrase_prob_dbm, rev_prob_dbm, tran_prob_dbm,
                aux_trans, yomis, min_phrase_prob, min_tran_prob):
  phrase_prob = 0.0
  if phrase_prob_dbm:
    tokens = tokenizer.Tokenize("en", word, False, True)[:3]
    norm_phrase = " ".join(tokens)
    phrase_prob = float(phrase_prob_dbm.GetStr(norm_phrase) or 0.0)
  uniq_trans = set()
  check_trans = []
  norm_word = word.lower()
  for pos, tran in trans:
    norm_tran = tran.lower()
    if norm_tran == norm_word: continue
    if norm_tran in uniq_trans: continue
    uniq_trans.add(norm_tran)
    check_trans.append((pos, tran))
    tran = tokenizer.NormalizeJaWordForPos(pos, tran)
    norm_tran = tran.lower()
    if norm_tran in uniq_trans: continue
    uniq_trans.add(norm_tran)
    check_trans.append((pos, tran))
  scored_trans = []
  aux_targets = aux_trans.get(word)
  for pos, tran in check_trans:
    tran_prob = 0.0
    if tran_prob_dbm:
      key = tkrzw_dict.NormalizeWord(word)
      tsv = tran_prob_dbm.GetStr(key)
      if tsv:
        fields = tsv.split("\t")
        extra_records = []
        for i in range(0, len(fields), 3):
          src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
          if src != word: continue
          if trg != tran: continue
          tran_prob = float(prob)
    aux_hit = False
    if aux_targets:
      count = aux_targets.count(tran)
      if count > 0:
        aux_hit = True
        tran_prob += count * 0.05
    has_yomi = tran in yomis
    if has_yomi:
      tran_prob += 0.01
    rev_prob = 0.0
    if rev_prob_dbm:
      tokens = tokenizer.Tokenize("ja", tran, False, True)[:3]
      norm_phrase = " ".join(tokens)
      rev_prob = float(rev_prob_dbm.GetStr(norm_phrase) or 0.0)
      if rev_prob > 0:
        tran_prob += min(rev_prob ** 0.5, 0.2)
      else:
        tran_prob *= 0.8
    if tran_prob > 0.04 and (rev_prob > 0.0 or has_yomi):
      pass
    else:
      if not aux_hit and phrase_prob < min_phrase_prob: continue
      if not aux_hit and tran_prob < min_tran_prob: continue
    if regex.fullmatch(r"[\p{Katakana}ー]+", tran):
      tran_prob *= 0.7
    elif regex.fullmatch(r"[\p{Hiragana}\p{Katakana}ー]+", tran):
      tran_prob *= 0.8
    scored_trans.append((pos, tran, tran_prob))
  if not scored_trans: return None, None
  scored_trans = sorted(scored_trans, key=lambda x: x[2], reverse=True)
  return (scored_trans, phrase_prob)


def PrintEntry(word, scored_trans):
  score_poses = collections.defaultdict(list)
  for pos, tran, score in scored_trans:
    score_poses[pos].append((tran, score))
  final_scores = []
  for pos, items in score_poses.items():
    sum_score = 0
    max_score = 0
    for item in items:
      sum_score += item[1]
      max_score = max(max_score, item[1])
    final_scores.append((pos, sum_score + max_score))
  final_scores = sorted(final_scores, key=lambda x: x[1], reverse=True)
  fields = []
  fields.append("word=" + word)
  for pos, score in final_scores:
    items = score_poses[pos][:5]
    text = ", ".join([x[0] for x in items])
    fields.append(pos + "=" + text)
  print("\t".join(fields))


def main():
  args = sys.argv[1:]
  phrase_prob_path = tkrzw_dict.GetCommandFlag(args, "--phrase_prob", 1) or ""
  rev_prob_path = tkrzw_dict.GetCommandFlag(args, "--rev_prob", 1) or ""
  tran_prob_path = tkrzw_dict.GetCommandFlag(args, "--tran_prob", 1) or ""
  tran_aux_paths = tkrzw_dict.GetCommandFlag(args, "--tran_aux", 1) or ""
  yomi_paths = tkrzw_dict.GetCommandFlag(args, "--yomi", 1) or ""
  min_phrase_prob = float(tkrzw_dict.GetCommandFlag(args, "--min_phrase_prob", 1) or .000001)
  min_tran_prob = float(tkrzw_dict.GetCommandFlag(args, "--min_tran_prob", 1) or 0.1)
  Run(phrase_prob_path, rev_prob_path, tran_prob_path,
      tran_aux_paths, yomi_paths, min_phrase_prob, min_tran_prob)


if __name__=="__main__":
  main()
