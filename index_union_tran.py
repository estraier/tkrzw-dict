#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to make an index of translations of a union dictionary
#
# Usage:
#   index_union_tran.py [--input str] [--output str] [--tran_prob str] [--quiet]
#
# Example:
#   ./index_union_tran.py --input union-body.tkh --output union-tran-index.tkh
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
import logging
import operator
import os
import regex
import sys
import time
import tkrzw
import tkrzw_dict
import tkrzw_tokenizer


logger = tkrzw_dict.GetLogger()


def JoinWords(words):
  text = ""
  for word in words:
    if (text and (
        not regex.search("[\p{Han}\p{Katakana}\p{Hiragana}ー]", text[-1]) and
        not regex.search("[\p{Han}\p{Katakana}\p{Hiragana}ー]", word[0]))):
      text += " "
    text += word[0]
  return text


class IndexTranslationsBatch:
  def __init__(self, input_path, output_path, supplement_labels, tran_prob_path,
               conj_verb_path, conj_adj_path):
    self.input_path = input_path
    self.output_path = output_path
    self.supplement_labels = supplement_labels
    self.tran_prob_path = tran_prob_path
    self.conj_verb_path = conj_verb_path
    self.conj_adj_path = conj_adj_path    
    self.tokenizer = tkrzw_tokenizer.Tokenizer()

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}".format(
      self.input_path, self.output_path))
    mem_index = tkrzw.DBM()
    mem_index.Open("", True, dbm="BabyDBM").OrDie()
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False, dbm="HashDBM").OrDie()
    conj_verbs = self.ReadConjWords(self.conj_verb_path)
    conj_adjs = self.ReadConjWords(self.conj_adj_path)
    it = input_dbm.MakeIterator()
    it.First()
    num_entries = 0
    num_translations = 0
    tran_dict = set()
    while True:
      record = it.GetStr()
      if not record: break
      key, serialized = record
      entry = json.loads(serialized)
      for word_entry in entry:
        word = word_entry["word"]
        prob = max(float(word_entry.get("probability") or "0"), 0.0000001)
        aoa = min(float(word_entry.get("aoa") or "20"), 20.0)
        score = prob * ((30 - aoa) / 10)
        poses = set()
        for item in word_entry["item"]:
          if item["label"] in self.supplement_labels: continue
          poses.add(item["pos"])
        word_trans = word_entry.get("translation") or []
        phrase_trans = []
        phrases = word_entry.get("phrase")
        if phrases:
          for phrase in phrases:
            if phrase.get("p") or phrase.get("i"): continue
            for phrase_tran in phrase.get("x"):
              phrase_tran = regex.sub(r"\(.*?\)", "", phrase_tran).strip()
              if phrase_tran:
                phrase_trans.append(phrase_tran)
        weight_word_trans = []
        for trans, weight in [(word_trans, 1.0), (phrase_trans, 0.5)]:
          for word_tran in trans:
            weight_word_trans.append((word_tran, weight))
            match = regex.search(
              r"([\p{Han}\p{Katakana}ー]{2,})(する|すること|される|されること|をする)$", word_tran)
            if match:
              short_word_tran = word_tran[:-len(match.group(2))]
              if short_word_tran:
                weight_word_trans.append((short_word_tran, weight * 0.8))
            short_word_tran = self.tokenizer.CutJaWordNounParticle(word_tran)
            if short_word_tran != word_tran:
              weight_word_trans.append((short_word_tran, weight * 0.8))
            match = regex.search(
              r"([\p{Han}\p{Katakana}ー]{2,})(的|的な|的に)$", word_tran)
            if match:
              short_word_tran = word_tran[:-len(match.group(2))]
              if short_word_tran:
                weight_word_trans.append((short_word_tran, weight * 0.8))
            match = regex.search(
              r"([\p{Han}]{2,})(が|の|を|に|へ|と|より|から|で|や|な|なる|たる)$", word_tran)
            if match:
              short_word_tran = word_tran[:-len(match.group(2))]
              if short_word_tran:
                weight_word_trans.append((short_word_tran, weight * 0.8))
        uniq_trans = set()
        for tran, weight in weight_word_trans:
          norm_tran = tkrzw_dict.NormalizeWord(tran)
          if norm_tran in uniq_trans: continue
          uniq_trans.add(norm_tran)
          pair = "{}\t{:.8f}".format(word, score * weight)
          score *= 0.98
          mem_index.Append(norm_tran, pair, "\t").OrDie()
        num_uniq_trans = len(uniq_trans)
        conj_trans = {}
        if "verb" in poses or "adjective" in poses:
          weight = 1.0
          for tran in word_trans:
            tokens = self.tokenizer.GetJaPosList(tran)
            if not tokens: continue
            if ("verb" in poses and tokens[-1][1] == "動詞" and
                tokens[-1][0] == tokens[-1][3]):
              for conj in conj_verbs.get(tokens[-1][0]) or []:
                tokens[-1][0] = conj
                conj_word = JoinWords(tokens)
                if conj_word not in conj_trans:
                  conj_trans[conj_word] = weight
            if ("adjective" in poses and tokens[-1][1] == "形容詞" and
                tokens[-1][0] == tokens[-1][3]):
              for conj in conj_adjs.get(tokens[-1][0]) or []:
                tokens[-1][0] = conj
                conj_word = JoinWords(tokens)
                if conj_word not in conj_trans:
                  conj_trans[conj_word] = weight
            weight *= 0.95
        for tran, weight in conj_trans.items():
          norm_tran = tkrzw_dict.NormalizeWord(tran)
          if norm_tran in uniq_trans: continue
          pair = "{}\t{:.8f}".format(word, score * weight)
          mem_index.Append(" " + norm_tran, pair, "\t").OrDie()
        for item in word_entry["item"]:
          if item["label"] in self.supplement_labels:
            for tran in item["text"].split(","):
              tran = tran.strip()
              if tran:
                tran_dict_key = word + "\t" + tran
                tran_dict.add(tran_dict_key)
        num_translations += len(uniq_trans)
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Reading: entries={}, translations={}".format(
          num_entries, num_translations))
      it.Next()
    input_dbm.Close().OrDie()
    logger.info("Reading done: entries={}, translations={}".format(
      num_entries, num_translations))
    output_dbm = tkrzw.DBM()
    num_buckets = mem_index.Count() * 2
    output_dbm.Open(
      self.output_path, True, dbm="HashDBM", truncate=True,
      align_pow=0, num_buckets=num_buckets).OrDie()
    tran_prob_dbm =None
    if self.tran_prob_path:
      tran_prob_dbm = tkrzw.DBM()
      tran_prob_dbm.Open(self.tran_prob_path, False, dbm="HashDBM").OrDie()
    it = mem_index.MakeIterator()
    it.First()
    num_records = 0
    while True:
      record = it.GetStr()
      if not record: break
      key, value = record
      scored_trans = []
      uniq_words = set()
      fields = value.split("\t")
      for i in range(0, len(fields), 2):
        word = fields[i]
        score = float(fields[i + 1])
        if word in uniq_words: continue
        uniq_words.add(word)
        if tran_prob_dbm:
          prob = self.GetTranProb(tran_prob_dbm, word, key)
          tran_dict_key = word + "\t" + key
          prob = max(prob, 0.000001)
          if tran_dict_key in tran_dict:
            prob += 0.1
          score = (score * prob) ** 0.5
        scored_trans.append((word, score))
      scored_trans = sorted(scored_trans, key=lambda x: x[1], reverse=True)
      value = "\t".join([x[0] for x in scored_trans])
      output_dbm.Set(key, value).OrDie()
      num_records += 1
      if num_records % 10000 == 0:
        logger.info("Writing: records={}".format(num_records))
      it.Next()
    if tran_prob_dbm:
      tran_prob_dbm.Close().OrDie()
    output_dbm.Close().OrDie()
    logger.info("Writing done: records={}".format(num_records))
    mem_index.Close().OrDie()
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def ReadConjWords(self, path):
    conjs = {}
    if path:
      with open(path) as input_file:
        for line in input_file:
          fields = line.strip().split("\t")
          if len(fields) <= 2: continue
          word, trans = fields[0], fields[1:]
          conjs[word] = trans
    return conjs

  def GetTranProb(self, tran_prob_dbm, src_text, trg_text):
    src_text = tkrzw_dict.NormalizeWord(src_text)
    tsv = tran_prob_dbm.GetStr(src_text)
    max_prob = 0.0
    if tsv:
      trg_text = tkrzw_dict.NormalizeWord(trg_text)
      fields = tsv.split("\t")
      for i in range(0, len(fields), 3):
        src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
        norm_trg = tkrzw_dict.NormalizeWord(trg)
        if norm_trg == trg_text:
          max_prob = max(max_prob, prob)
        elif len(norm_trg) >= 2 and trg_text.startswith(norm_trg):
          max_prob = max(max_prob, prob * 0.01)
        elif len(trg_text) >= 2 and norm_trg.startswith(trg_text):
          max_prob = max(max_prob, prob * 0.01)
    return max_prob


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-tran-index.tkh"
  supplement_labels = set((tkrzw_dict.GetCommandFlag(args, "--supplement", 1) or "xs").split(","))
  tran_prob_path = tkrzw_dict.GetCommandFlag(args, "--tran_prob", 1) or ""
  conj_verb_path = tkrzw_dict.GetCommandFlag(args, "--conj_verb", 1)
  conj_adj_path = tkrzw_dict.GetCommandFlag(args, "--conj_adj", 1)
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  IndexTranslationsBatch(input_path, output_path, supplement_labels, tran_prob_path,
                         conj_verb_path, conj_adj_path).Run()


if __name__=="__main__":
  main()
