#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to build a union database by merging TSV dictionaries
#
# Usage:
#   build_union_db.py [--output str] [--quiet] [--top str] [--slim str] [--rank str]
#     [--word_prob str] [--tran_prob str] [--rev_prob str] [--min_prob str] inputs...
#   (An input specified as "label:tsv_file".
#
# Example:
#   ./build_union_db.py --output union-body.tkh \
#     --word_prob enwiki-word-prob.tkh --tran_prob tran-prob.tkh \
#     --rev_prob jawiki-word-prob.tkh --min_prob we:0.00001 \
#     wj:wiktionary-ja.tsv wn:wordnet.tsv we:wiktionary-en.tsv 
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
import logging
import operator
import os
import regex
import sys
import time
import tkrzw
import tkrzw_dict
import tkrzw_pron_util
import tkrzw_tokenizer


logger = tkrzw_dict.GetLogger()


class BuildUnionDBBatch:
  def __init__(self, input_confs, output_path, gross_labels,
               surfeit_labels, top_labels, rank_labels, slim_labels, tran_list_labels,
               word_prob_path, tran_prob_path, rev_prob_path, min_prob_map):
    self.input_confs = input_confs
    self.output_path = output_path
    self.gross_labels = gross_labels
    self.surfeit_labels = surfeit_labels
    self.top_labels = top_labels
    self.rank_labels = rank_labels
    self.slim_labels = slim_labels
    self.tran_list_labels = tran_list_labels
    self.word_prob_path = word_prob_path
    self.tran_prob_path = tran_prob_path
    self.rev_prob_path = rev_prob_path
    self.min_prob_map = min_prob_map
    self.tokenizer = tkrzw_tokenizer.Tokenizer()

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_confs={}, output_path={}".format(
      str(self.input_confs), self.output_path))
    word_dicts = []
    for label, input_path in self.input_confs:
      slim = label in self.slim_labels
      word_dict = self.ReadInput(input_path, slim)
      word_dicts.append((label, word_dict))
    self.SaveWords(word_dicts)
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def ReadInput(self, input_path, slim):
    poses = ("noun", "verb", "adjective", "adverb",
             "pronoun", "auxverb", "preposition", "determiner", "article", "interjection",
             "prefix", "suffix", "abbreviation")
    start_time = time.time()
    logger.info("Reading an input file: input_path={}".format(input_path))
    word_dict = collections.defaultdict(list)
    num_entries = 0
    with open(input_path) as input_file:
      for line in input_file:
        word = ""
        ipa = ""
        sampa = ""
        texts = []
        inflections = {}
        mode = ""
        for field in line.strip().split("\t"):
          columns = field.split("=", 1)
          if len(columns) < 2: continue
          name, value = columns
          if name == "word":
            word = value
          elif name == "pronunciation_ipa":
            ipa = value
          elif name == "pronunciation_sampa":
            sampa = value
          elif name.startswith("inflection_"):
            name = regex.sub(r"^[a-z]+_", "", name)
            inflections[name] = inflections.get(name) or value
          elif name in poses:
            if slim:
              value = regex.sub(r" \[-+\] .*", "", value).strip()
            if value:
              texts.append((name, value))
          elif name == "mode":
            mode = value
        if not ipa and sampa:
          ipa = tkrzw_pron_util.SampaToIPA(sampa)
        if word and (ipa or texts or inflections):
          key = tkrzw_dict.NormalizeWord(word)
          entry = {"word": word}
          if ipa:
            entry["pronunciation"] = ipa
          for name, value in inflections.items():
            entry[name] = value
          if texts:
            entry["texts"] = texts
          if mode:
            key += "\t" + mode
          word_dict[key].append(entry)
          num_entries += 1
        if num_entries % 1000 == 0:
          logger.info("Reading an input: num_entries={}".format(num_entries))
    logger.info("Reading an input done: num_entries={}, elapsed_time={:.2f}s".format(
      num_entries, time.time() - start_time))
    return word_dict

  def SaveWords(self, word_dicts):
    keys = set()
    logger.info("Extracting keys")
    for label, word_dict in word_dicts:
      for key in word_dict.keys():
        if key.find("\t") >= 0: continue
        keys.add(key)
    logger.info("Extracting keys done: num_keys={}".format(len(keys)))
    start_time = time.time()
    logger.info("Saving words: output_path={}".format(self.output_path))
    word_dbm = tkrzw.DBM()
    word_dbm.Open(self.output_path, True, truncate=True)
    word_prob_dbm = None
    if self.word_prob_path:
      word_prob_dbm = tkrzw.DBM()
      word_prob_dbm.Open(self.word_prob_path, False, dbm="HashDBM").OrDie()
    tran_prob_dbm = None
    if self.tran_prob_path:
      tran_prob_dbm = tkrzw.DBM()
      tran_prob_dbm.Open(self.tran_prob_path, False, dbm="HashDBM").OrDie()
    rev_prob_dbm = None
    if self.rev_prob_path:
      rev_prob_dbm = tkrzw.DBM()
      rev_prob_dbm.Open(self.rev_prob_path, False, dbm="HashDBM").OrDie()
    num_records = 0
    for key in keys:
      record = self.MakeRecord(key, word_dicts, word_prob_dbm, tran_prob_dbm, rev_prob_dbm)
      if not record: continue
      serialized = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
      word_dbm.Set(key, serialized)
      num_records += 1
      if num_records % 1000 == 0:
        logger.info("Saving words: num_records={}".format(num_records))
    if rev_prob_dbm:
      rev_prob_dbm.Close().OrDie()
    if tran_prob_dbm:
      tran_prob_dbm.Close().OrDie()
    if word_prob_dbm:
      word_prob_dbm.Close().OrDie()
    logger.info("Optiizing: num_records={}".format(word_dbm.Count()))
    word_dbm.Rebuild().OrDie()
    word_dbm.Close().OrDie()
    logger.info("Saving words done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def MakeRecord(self, key, word_dicts, word_prob_dbm, tran_prob_dbm, rev_prob_dbm):
    word_entries = {}
    word_ranks = {}
    word_trans = {}
    num_words = 0
    for label, word_dict in word_dicts:
      dict_entries = word_dict.get(key)
      if not dict_entries: continue
      for i, entry in enumerate(dict_entries):
        num_words += 1
        word = entry["word"]
        entries = word_entries.get(word) or []
        entries.append((label, entry))
        word_entries[word] = entries
        old_rank = word_ranks.get(word) or sys.maxsize
        if label in self.rank_labels:
          rank = num_words / 10000
        else:
          rank = num_words
        word_ranks[word] = min(old_rank, rank)
      dict_entries = word_dict.get(key + "\ttranslation")
      if dict_entries:
        for entry in dict_entries:
          word = entry["word"]
          tran_texts = entry.get("texts")
          if not tran_texts: continue
          for tran_pos, tran_text in tran_texts:
            tran_key = word + "\t" + label + "\t" + tran_pos
            trans = word_trans.get(tran_key) or []
            trans.append(tran_text)
            word_trans[tran_key] = trans
    sorted_word_ranks = sorted(word_ranks.items(), key=lambda x: x[1])
    merged_entry = []
    top_names = ("pronunciation", "noun_plural",
                 "verb_singular", "verb_present_participle",
                 "verb_past", "verb_past_participle",
                 "adjective_comparative", "adjective_superative",
                 "adverb_comparative", "adverb_superative")
    for word, rank in sorted_word_ranks:
      entries = word_entries[word]
      word_entry = {}
      word_entry["word"] = word
      effective_labels = set()
      for label, entry in entries:
        if label not in self.surfeit_labels:
          effective_labels.add(label)
        for top_name in top_names:
          if label not in self.top_labels and top_name in word_entry: continue
          value = entry.get(top_name)
          if value:
            word_entry[top_name] = value
      for label, entry in entries:
        texts = entry.get("texts")
        if not texts: continue
        for pos, text in texts:
          items = word_entry.get("item") or []
          tran_key = word + "\t" + label + "\t" + pos
          trans = word_trans.get(tran_key)
          if trans:
            del word_trans[tran_key]
            for tran_text in trans:
              tran_item = {"label": label, "pos": pos, "text": tran_text}
              items.append(tran_item)
          item = {"label": label, "pos": pos, "text": text}
          items.append(item)
          word_entry["item"] = items
      if "item" not in word_entry: continue
      if word_prob_dbm:
        prob = self.GetPhraseProb(word_prob_dbm, "en", word)
        word_entry["probability"] = "{:.6f}".format(prob).replace("0.", ".")
        if self.min_prob_map:
          has_good_label = False
          for item in word_entry["item"]:
              if item["label"] not in self.min_prob_map:
                has_good_label = True
                break
          if not has_good_label:
            new_items = []
            for item in word_entry["item"]:
              for label, min_prob in self.min_prob_map.items():
                if prob < min_prob and item["label"] == label:
                  continue
                new_items.append(item)
            word_entry["item"] = new_items
      if not word_entry.get("item"):
        continue
      if merged_entry and not effective_labels:
        continue
      self.SetTranslations(word_entry, tran_prob_dbm, rev_prob_dbm)
      merged_entry.append(word_entry)
    return merged_entry

  def GetPhraseProb(self, word_prob_dbm, language, word):
    tokens = self.tokenizer.Tokenize(language, word, True, True)
    probs = []
    for token in tokens:
      token = tkrzw_dict.NormalizeWord(token)
      prob = float(word_prob_dbm.GetStr(token) or 0.0)
      probs.append(prob)
    probs = sorted(probs)
    min_prob = 0.0
    if probs:
      min_prob =probs[0]
    for prob in probs[1:]:
      min_prob *= min(prob ** 0.5, 0.2)
    min_prob = max(min_prob, 0.000001)
    return min_prob

  def SetTranslations(self, entry, tran_prob_dbm, rev_prob_dbm):
    word = entry["word"]
    tran_probs = {}
    if tran_prob_dbm:
      key = tkrzw_dict.NormalizeWord(word)
      tsv = tran_prob_dbm.GetStr(key)
      if tsv:
        fields = tsv.split("\t")
        for i in range(0, len(fields), 3):
          src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
          if src != word:
            prob *= 0.1
          norm_trg = tkrzw_dict.NormalizeWord(trg)
          if tkrzw_dict.IsStopWord("ja", norm_trg) or len(norm_trg) < 2:
            prob *= 0.5
          tran_probs[norm_trg] = prob ** 0.8
    translations = {}
    tran_labels = {}
    def Vote(tran, weight, label):
      norm_tran = tkrzw_dict.NormalizeWord(tran)
      score = 0.00001
      if rev_prob_dbm:
        prob = self.GetPhraseProb(rev_prob_dbm, "ja", tran)
        if tkrzw_dict.IsStopWord("ja", tran) or tran in ("又は"):
          prob *= 0.5
        score += prob
      score *= weight
      old_score = translations.get(tran) or 0.0
      translations[tran] = max(old_score, score)
      old_labels = tran_labels.get(norm_tran) or set()
      old_labels.add(label)
      tran_labels[norm_tran] = old_labels
    body_weight = 1.0
    tran_weight = 0.7
    for item in entry["item"]:
      label = item["label"]
      sections = item["text"].split(" [-] ")
      section_index = 1
      if (label in self.gross_labels and
          regex.search(r"[\p{Han}\p{Hiragana}\p{Katakana}ー]", sections[0])):
        weight = body_weight
        body_weight *= 0.9
        text = sections[0]
        if regex.search(r"[\(（《\{\(]([^)）\}\]]+[・、])?" +
                        r"(俗|俗語|スラング|卑|卑語|隠語|古|古語|廃|廃用|廃語)+[)）》\}\]]", text):
          weight *= 0.1
        text = regex.sub(r"[\(（《\{\(].*?[)）》\}\]]", "", text)
        text = regex.sub(r"[･・]", "", text)
        text = regex.sub(r"\s+", " ", text).strip()
        if regex.search(
            r"の(直接法|直説法|仮定法)?(現在|過去)?(第?[一二三]人称)?[ ・、]?" +
            r"(単数|複数|現在|過去|比較|最上|進行|完了|動名詞|単純)+[ ・、]?" +
            r"(形|型|分詞|級|動名詞)+", text):
          continue
        if regex.search(r"の(直接法|直説法|仮定法)(現在|過去)", text):
          continue
        if regex.search(r"の(動名詞|異綴|異体|古語)", text):
          continue
        text = regex.sub(r" \[-+\] .*", "", text).strip()
        text = regex.sub(r" -+ .*", "", text).strip()
        for tran in regex.split("[。|、|；|,|;]", text):
          if len(translations) > 1:
            if tran in ("また", "または", "又は", "しばしば"):
              continue
          tran = regex.sub(r"[-～‥…] *(が|の|を|に|へ|と|より|から|で|や)", "", tran)
          tran = regex.sub(r"[～]", "", tran)
          tokens = self.tokenizer.Tokenize("ja", tran, False, False)
          tokens = tokens[:6]
          tran = " ".join(tokens)
          tran = regex.sub(r"([\p{Han}\p{Hiragana}\p{Katakana}ー]) +", r"\1", tran)
          tran = regex.sub(r" +([\p{Han}\p{Hiragana}\p{Katakana}ー])", r"\1", tran)
          tran = tran.strip()
          if tran:
            Vote(tran, weight, label)
            weight *= 0.8
      if label in self.tran_list_labels:
        for section in sections:
          if not section.startswith("[translation]: "): continue
          weight = tran_weight
          tran_weight *= 0.9
          text = regex.sub(r"^[^:]+: ", "", section)
          text = regex.sub(r"\(.*?\)", "", text).strip()
          for tran in text.split(","):
            tran = tran.strip()
            if tran:
              Vote(tran, weight, label)
              weight *= 0.85
    bonus_translations = []
    scored_translations = set()
    for tran, score in translations.items():
      norm_tran = tkrzw_dict.NormalizeWord(tran)
      prob = tran_probs.get(norm_tran)
      if prob:
        if len(norm_tran) < 2:
          prob *= 0.5
        score += prob
        del tran_probs[norm_tran]
        scored_translations.add(norm_tran)
      bonus_translations.append((tran, score))
    sorted_translations = []
    for tran, score in bonus_translations:
      norm_tran = tkrzw_dict.NormalizeWord(tran)
      if norm_tran not in scored_translations:
        bonus = 0.0
        for dict_tran, prob in tran_probs.items():
          if len(dict_tran) >= 2 and norm_tran.startswith(dict_tran):
            bonus = max(bonus, prob * 0.3)
          elif len(norm_tran) >= 2 and dict_tran.startswith(norm_tran):
            bonus = max(bonus, prob * 0.2)
          elif len(dict_tran) >= 2 and norm_tran.find(dict_tran) >= 0:
            bonus = max(bonus, prob * 0.1)
          elif len(norm_tran) >= 2 and dict_tran.find(norm_tran) >= 0:
            bonus = max(bonus, prob * 0.1)
        score += bonus
      if norm_tran in tran_labels:
        score += (len(tran_labels[norm_tran]) - 1) * 0.001
      sorted_translations.append((tran, score))
    sorted_translations = sorted(sorted_translations, key=lambda x: x[1], reverse=True)
    deduped_translations = []
    for tran, score in sorted_translations:
      norm_tran = tkrzw_dict.NormalizeWord(tran)
      bias = 1.0
      for prev_tran, prev_score in deduped_translations:
        if len(prev_tran) >= 2 and norm_tran.startswith(prev_tran):
          bias = min(bias, 0.4 if len(prev_tran) >= 2 else 0.6)
        elif len(norm_tran) >= 2 and prev_tran.startswith(norm_tran):
          bias = min(bias, 0.6 if len(norm_tran) >= 2 else 0.7)
        elif len(prev_tran) >= 2 and norm_tran.find(prev_tran) >= 0:
          bias = min(bias, 0.8 if len(prev_tran) >= 2 else 0.9)
        elif len(norm_tran) >= 2 and prev_tran.find(norm_tran) >= 0:
          bias = min(bias, 0.8 if len(norm_tran) >= 2 else 0.9)
        dist = tkrzw.Utility.EditDistanceLev(norm_tran, prev_tran)
        dist /= max(len(norm_tran), len(prev_tran))
        if dist < 0.3:
          bias = min(bias, dist + 0.2)
      score *= bias
      deduped_translations.append((tran, score))
    deduped_translations = sorted(deduped_translations, key=lambda x: x[1], reverse=True)
    final_translations = []
    for tran, score in deduped_translations:
      if len(final_translations) <= 16 or score >= 0.001:
        final_translations.append(tran)
    if final_translations:
      entry["translation"] = final_translations


def main():
  args = sys.argv[1:]
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "wiktionary.tkh"
  gross_labels = set((tkrzw_dict.GetCommandFlag(args, "--gross", 1) or "wj").split(","))
  top_labels = set((tkrzw_dict.GetCommandFlag(args, "--top", 1) or "we").split(","))
  rank_labels = set((tkrzw_dict.GetCommandFlag(args, "--rank", 1) or "wn").split(","))
  slim_labels = set((tkrzw_dict.GetCommandFlag(args, "--slim", 1) or "we").split(","))
  surfeit_labels = set((tkrzw_dict.GetCommandFlag(args, "--surfeit", 1) or "we").split(","))
  tran_list_labels = set((tkrzw_dict.GetCommandFlag(args, "--tran_list", 1) or "wn,we").split(","))
  word_prob_path = tkrzw_dict.GetCommandFlag(args, "--word_prob", 1) or ""
  tran_prob_path = tkrzw_dict.GetCommandFlag(args, "--tran_prob", 1) or ""
  rev_prob_path = tkrzw_dict.GetCommandFlag(args, "--rev_prob", 1) or ""
  min_prob_exprs = tkrzw_dict.GetCommandFlag(args, "--min_prob", 1) or ""
  min_prob_map = {}
  for min_prob_expr in min_prob_exprs.split(","):
    columns = min_prob_expr.split(":")
    if len(columns) == 2:
      min_prob_map[columns[0]] = float(columns[1])
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  unused_flag = tkrzw_dict.GetUnusedFlag(args)
  if unused_flag:
    raise RuntimeError("Unknow flag: " + unused_flag)
  inputs = tkrzw_dict.GetArguments(args)
  if not inputs:
    raise RuntimeError("inputs are required")
  input_confs = []
  for input in inputs:
    input_conf = input.split(":", 1)
    if len(input_conf) != 2:
      raise RuntimeError("invalid input: " + input)
    input_confs.append(input_conf)
  BuildUnionDBBatch(input_confs, output_path, gross_labels,
                    surfeit_labels, top_labels, rank_labels, slim_labels, tran_list_labels,
                    word_prob_path, tran_prob_path, rev_prob_path, min_prob_map).Run()
 

if __name__=="__main__":
  main()
