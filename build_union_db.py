#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to build a union database by merging TSV dictionaries
#
# Usage:
#   build_union_db.py [--output str] [--core str] [--gross str] [--top str] [--slim str]
#     [--phrase_prob str] [--tran_prob str] [--tran_aux str] [--rev_prob str] [--cooc_prob str]
#     [--aoa str] [--keyword str] [--min_prob str] [--quiet] inputs...
#   (An input specified as "label:tsv_file".
#
# Example:
#   ./build_union_db.py --output union-body.tkh \
#     --phrase_prob enwiki-phrase-prob.tkh --tran_prob tran-prob.tkh \
#     --tran_aux dict1.tsv,dict2.tsv --rev_prob jawiki-word-prob.tkh \
#     --cooc_prob enwiki-cooc-prob.tkh --min_prob we:0.00001 \
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
import math
import operator
import os
import regex
import sys
import time
import tkrzw
import tkrzw_dict
import tkrzw_pron_util
import tkrzw_tokenizer
import unicodedata


logger = tkrzw_dict.GetLogger()
poses = ("noun", "verb", "adjective", "adverb",
         "pronoun", "auxverb", "preposition", "determiner", "article",
         "interjection", "conjunction",
         "prefix", "suffix", "abbreviation")
inflection_names = ("noun_plural","verb_singular", "verb_present_participle",
                   "verb_past", "verb_past_participle",
                   "adjective_comparative", "adjective_superative",
                   "adverb_comparative", "adverb_superative")
etymology_names = ("etymology_prefix", "etymology_core", "etymology_suffix")
top_names = ("pronunciation",) + inflection_names + etymology_names
rel_weights = {"synonym": 1.0,
               "hypernym": 0.9,
               "hyponym": 0.8,
               "antonym": 0.2,
               "derivative": 0.7,
               "relation": 0.5}
noun_suffixes = [
  "ment", "age", "tion", "ics", "ness", "ity", "ism", "or", "er", "ist",
  "ian", "ee", "tion", "sion", "ty", "ance", "ence", "ency", "cy", "ry",
  "al", "age", "dom", "hood", "ship", "nomy",
]
verb_suffixes = [
  "ify", "en", "ize", "ise", "fy", "ate",
]
adjective_suffixes = [
  "some", "able", "ible", "ic", "ical", "ive", "ful", "less", "ly", "ous", "y",
  "ised", "ing", "ed", "ish",
]
adverb_suffixes = [
  "ly",
]
particles = [
  "about", "above", "across", "after", "against", "along", "amid", "among", "around", "as", "at",
  "before", "behind", "below", "beneath", "between", "beside", "beyond", "by", "despite", "during",
  "down", "except", "for", "from", "in", "inside", "into", "near",
  "of", "off", "on", "onto", "out", "outside", "over",
  "per", "re", "since", "through", "throughout", "till", "to", "toward",
  "under", "until", "up", "upon", "with", "within", "without", "via",
]


class BuildUnionDBBatch:
  def __init__(self, input_confs, output_path, core_labels, gross_labels,
               surfeit_labels, top_labels, slim_labels, tran_list_labels,
               phrase_prob_path, tran_prob_path, tran_aux_paths, rev_prob_path,
               cooc_prob_path, aoa_paths, keyword_path, min_prob_map):
    self.input_confs = input_confs
    self.output_path = output_path
    self.core_labels = core_labels
    self.gross_labels = gross_labels
    self.surfeit_labels = surfeit_labels
    self.top_labels = top_labels
    self.slim_labels = slim_labels
    self.tran_list_labels = tran_list_labels
    self.phrase_prob_path = phrase_prob_path
    self.tran_prob_path = tran_prob_path
    self.tran_aux_paths = tran_aux_paths
    self.rev_prob_path = rev_prob_path
    self.cooc_prob_path = cooc_prob_path
    self.aoa_paths = aoa_paths
    self.keyword_path = keyword_path
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
    aux_trans = {}
    for tran_aux_path in self.tran_aux_paths:
      if not tran_aux_path: continue
      self.ReadTranAuxTSV(tran_aux_path, aux_trans)
    aoa_words = {}
    for aoa_path in self.aoa_paths:
      if not aoa_path: continue
      self.ReadAOAWords(aoa_path, aoa_words)
    keywords = set()
    if self.keyword_path:
      self.ReadKeywords(self.keyword_path, keywords)
    self.SaveWords(word_dicts, aux_trans, aoa_words, keywords)
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def ReadInput(self, input_path, slim):
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
        etymologies = {}
        mode = ""
        rel_words = {}
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
          elif name.startswith("etymology_"):
            etymologies[name] = value
          elif name in poses:
            if slim:
              value = regex.sub(r" \[-+\] .*", "", value).strip()
            if value:
              texts.append((name, value))
          elif name in rel_weights:
            rel_words[name] = value
          elif name == "mode":
            mode = value
        if not ipa and sampa:
          ipa = tkrzw_pron_util.SampaToIPA(sampa)
        if word and (ipa or texts or inflections or etymologies):
          key = tkrzw_dict.NormalizeWord(word)
          entry = {"word": word}
          if ipa:
            entry["pronunciation"] = ipa
          for name, value in inflections.items():
            entry[name] = value
          for name, value in etymologies.items():
            entry[name] = value
          if texts:
            entry["text"] = texts
          for rel_name, rel_value in rel_words.items():
            entry[rel_name] = rel_value
          if mode:
            key += "\t" + mode
          word_dict[key].append(entry)
          num_entries += 1
        if num_entries % 10000 == 0:
          logger.info("Reading an input: num_entries={}".format(num_entries))
    logger.info("Reading an input done: num_entries={}, elapsed_time={:.2f}s".format(
      num_entries, time.time() - start_time))
    return word_dict

  def ReadTranAuxTSV(self, input_path, aux_trans):
    start_time = time.time()
    logger.info("Reading a translation aux file: input_path={}".format(input_path))
    num_entries = 0
    with open(input_path) as input_file:
      for line in input_file:
        fields = line.strip().split("\t")
        if len(fields) < 2: continue
        word = fields[0]
        values = aux_trans.get(word) or []
        uniq_trans = set()
        for tran in fields[1:]:
          tran = regex.sub(r"[\p{Ps}\p{Pe}\p{C}]", "", tran)
          tran = regex.sub(r"\s+", " ", tran).strip()
          norm_tran = tkrzw_dict.NormalizeWord(tran)
          if not tran or not norm_tran: continue
          if norm_tran in uniq_trans: continue
          uniq_trans.add(norm_tran)
          values.append(tran)
        aux_trans[word] = values
        num_entries += 1
        if num_entries % 10000 == 0:
          logger.info("Reading a translation aux file: num_entries={}".format(num_entries))
    logger.info("Reading a translation aux file: num_entries={}, elapsed_time={:.2f}s".format(
      num_entries, time.time() - start_time))

  def ReadAOAWords(self, input_path, aoa_words):
    start_time = time.time()
    logger.info("Reading a AOA file: input_path={}".format(input_path))
    num_entries = 0
    with open(input_path) as input_file:
      is_first = True
      for line in input_file:
        if is_first:
          is_first = False
          continue
        fields = line.strip().split(",")
        if len(fields) != 7: continue
        word = fields[0].strip()
        occur = fields[3]
        mean = fields[4]
        stddev = fields[5]
        if not word or not regex.fullmatch(r"[0-9.]+", mean): continue
        if not regex.fullmatch(r"[.0-9]+", occur): continue
        mean = float(mean)
        if regex.fullmatch(r"[0-9.]+", stddev):
          mean += float(stddev)
        else:
          mean += 3.0
        if word not in aoa_words:
          aoa_words[word] = mean
        num_entries += 1
        if num_entries % 10000 == 0:
          logger.info("Reading a AOA file: num_entries={}".format(num_entries))
    logger.info("Reading a translation aux file: num_entries={}, elapsed_time={:.2f}s".format(
      num_entries, time.time() - start_time))

  def ReadKeywords(self, input_path, keywords):
    start_time = time.time()
    logger.info("Reading a keyword file: input_path={}".format(input_path))
    num_entries = 0
    with open(input_path) as input_file:
      for line in input_file:
        keyword = line.strip()
        keywords.add(keyword)
        num_entries += 1
        if num_entries % 10000 == 0:
          logger.info("Reading a keyword file: num_entries={}".format(num_entries))
    logger.info("Reading a translation aux file: num_entries={}, elapsed_time={:.2f}s".format(
      num_entries, time.time() - start_time))

  def SaveWords(self, word_dicts, aux_trans, aoa_words, keywords):
    start_time = time.time()
    logger.info("Extracting keys")
    keys = set()
    for label, word_dict in word_dicts:
      for key in word_dict.keys():
        if key.find("\t") >= 0: continue
        keys.add(key)
    logger.info("Extracting keys done: num_keys={}, elapsed_time={:.2f}s".format(
      len(keys), time.time() - start_time))
    start_time = time.time()
    logger.info("Indexing stems")
    stem_index = collections.defaultdict(list)
    for label, word_dict in word_dicts:
      if label not in self.core_labels: continue
      for key in keys:
        for entry in word_dict[key]:
          word = entry["word"]
          if not regex.fullmatch("[a-z]+", word): continue
          stems = self.GetDerivativeStems(entry)
          if stems:
            valid_stems = set()
            for stem in stems:
              if stem in keys:
                stem_index[stem].append(word)
                valid_stems.add(stem)
            if valid_stems:
              entry["stem"] = list(valid_stems)
    for label, word_dict in word_dicts:
      if label not in self.core_labels: continue
      for key in keys:
        for entry in word_dict[key]:
          word = entry["word"]
          children = stem_index.get(word)
          if children:
            entry["stem_child"] = list(set(children))
    logger.info("Indexing stems done: num_stems={}, elapsed_time={:.2f}s".format(
      len(stem_index), time.time() - start_time))
    start_time = time.time()
    logger.info("Checking POS of words")
    noun_words = set()
    verb_words = set()
    adj_words = set()
    adv_words = set()
    core_words = set()
    for label, word_dict in word_dicts:
      if label in self.core_labels:
        for key in keys:
          for entry in word_dict[key]:
            word = entry["word"]
            for pos, text in entry["text"]:
              if pos == "noun": noun_words.add(word)
              if pos == "verb": verb_words.add(word)
              if pos == "adjective": adj_words.add(word)
              if pos == "adverb": adv_words.add(word)
              core_words.add(word)
    logger.info("Checking POS of words done: num_core_words={}, elapsed_time={:.2f}s".format(
      len(core_words), time.time() - start_time))
    start_time = time.time()
    logger.info("Indexing base forms")
    base_index = collections.defaultdict(list)
    core_index = collections.defaultdict(list)
    for label, word_dict in word_dicts:
      if label not in self.top_labels: continue
      for key, entries in word_dict.items():
        for entry in entries:
          word = entry["word"]
          if not regex.fullmatch("[a-z]+", word): continue
          if word in verb_words:
            children = set()
            for part_name in ("verb_present_participle", "verb_past_participle"):
              part = entry.get(part_name)
              if part and (part in noun_words or part in adj_words):
                base_index[part].append(word)
                children.add(part)
            if children:
              entry["base_child"] = list(children)
          if word in adj_words:
            children = set()
            for part_name in ("adjective_comparative", "adjective_superative"):
              part = entry.get(part_name)
              if part and (part in noun_words or part in adj_words):
                base_index[part].append(word)
                children.add(part)
            if children:
              entry["base_child"] = list(children)
          core = entry.get("etymology_core")
          prefix = entry.get("etymology_prefix")
          suffix = entry.get("etymology_suffix")
          if core and len(core) >= 4 and not prefix and suffix:
            entry["core"] = core
            core_index[core].append(word)
      for key, entries in word_dict.items():
        for entry in entries:
          word = entry["word"]
          if not regex.fullmatch("[a-z]+", word): continue
          bases = base_index.get(word)
          if bases:
            entry["base"] = list(bases)
          children = core_index.get(word)
          if children:
            entry["core_child"] = list(children)
    logger.info("Indexing base forms done: num_participles={}, elapsed_time={:.2f}s".format(
      len(base_index), time.time() - start_time))
    logger.info("Preparing DBMs")
    phrase_prob_dbm = None
    if self.phrase_prob_path:
      phrase_prob_dbm = tkrzw.DBM()
      phrase_prob_dbm.Open(self.phrase_prob_path, False, dbm="HashDBM").OrDie()
    tran_prob_dbm = None
    if self.tran_prob_path:
      tran_prob_dbm = tkrzw.DBM()
      tran_prob_dbm.Open(self.tran_prob_path, False, dbm="HashDBM").OrDie()
    rev_prob_dbm = None
    if self.rev_prob_path:
      rev_prob_dbm = tkrzw.DBM()
      rev_prob_dbm.Open(self.rev_prob_path, False, dbm="HashDBM").OrDie()
    cooc_prob_dbm = None
    if self.cooc_prob_path:
      cooc_prob_dbm = tkrzw.DBM()
      cooc_prob_dbm.Open(self.cooc_prob_path, False, dbm="HashDBM").OrDie()
    start_time = time.time()
    logger.info("Merging entries: num_keys={}".format(len(keys)))
    merged_entries = []
    for key in keys:
      merged_entry = self.MergeRecord(
        key, word_dicts, aux_trans, aoa_words, keywords, core_words,
        phrase_prob_dbm, tran_prob_dbm, rev_prob_dbm, cooc_prob_dbm)
      if not merged_entry: continue
      merged_entries.append((key, merged_entry))
      if len(merged_entries) % 1000 == 0:
        logger.info("Merging entries:: num_entries={}".format(len(merged_entries)))
    logger.info("Making records done: num_records={}, elapsed_time={:.2f}s".format(
      len(merged_entries), time.time() - start_time))
    start_time = time.time()
    logger.info("Modifying entries")
    merged_entries = sorted(merged_entries)
    live_words = tkrzw.DBM()
    live_words.Open("", True, dbm="BabyDBM").OrDie()
    rev_live_words = tkrzw.DBM()
    rev_live_words.Open("", True, dbm="BabyDBM").OrDie()
    for key, merged_entry in merged_entries:
      for word_entry in merged_entry:
        word = word_entry["word"]
        prob = float(word_entry.get("probability") or 0)
        value = "{:.8f}".format(prob)
        live_words.Set(word, value).OrDie()
        rev_word = " ".join(reversed(word.split(" ")))
        rev_live_words.Set(rev_word, value).OrDie()
    num_entries = 0
    for key, merged_entry in merged_entries:
      for word_entry in merged_entry:
        word = word_entry["word"]
        entries = []
        for label, word_dict in word_dicts:
          dict_entries = word_dict.get(key)
          if not dict_entries: continue
          for entry in dict_entries:
            if entry["word"] == word:
              entries.append((label, entry))
        self.SetAOA(word_entry, entries, aoa_words, live_words, phrase_prob_dbm)
        self.SetTranslations(word_entry, aux_trans, tran_prob_dbm, rev_prob_dbm)
        self.SetRelations(word_entry, entries, word_dicts, live_words, rev_live_words,
                          phrase_prob_dbm, tran_prob_dbm, cooc_prob_dbm)
        if phrase_prob_dbm and cooc_prob_dbm:
          self.SetCoocurrences(word_entry, entries, word_dicts, phrase_prob_dbm, cooc_prob_dbm)
      num_entries += 1
      if num_entries % 1000 == 0:
        logger.info("Modifying entries: num_records={}".format(num_entries))
    logger.info("Modifying entries done: elapsed_time={:.2f}s".format(time.time() - start_time))
    rev_live_words.Close().OrDie()
    live_words.Close().OrDie()
    if cooc_prob_dbm:
      cooc_prob_dbm.Close().OrDie()
    if rev_prob_dbm:
      rev_prob_dbm.Close().OrDie()
    if tran_prob_dbm:
      tran_prob_dbm.Close().OrDie()
    if phrase_prob_dbm:
      phrase_prob_dbm.Close().OrDie()
    start_time = time.time()
    logger.info("Saving records: output_path={}".format(self.output_path))
    word_dbm = tkrzw.DBM()
    num_buckets = len(merged_entries) * 2
    word_dbm.Open(self.output_path, True, dbm="HashDBM", truncate=True,
                  align_pow=0, num_buckets=num_buckets)
    num_records = 0
    for key, merged_entry in merged_entries:
      serialized = json.dumps(merged_entry, separators=(",", ":"), ensure_ascii=False)
      word_dbm.Set(key, serialized)
      num_records += 1
      if num_records % 1000 == 0:
        logger.info("Saving records: num_records={}".format(num_records))
    word_dbm.Close().OrDie()
    logger.info("Saving records done: num_records={}, elapsed_time={:.2f}s".format(
      len(merged_entries), time.time() - start_time))

  def GetDerivativeStems(self, entry):
    word = entry["word"]
    poses = set()
    deris = set()
    for pos, text in entry["text"]:
      poses.add(pos)
      while True:
        match = regex.search(r"\[(synonym|derivative)\]: ", text)
        if match:
          text = text[match.end():]
          expr = regex.sub(r"\[-.*", "", text)
          for deri in expr.split(","):
            deri = deri.strip()
            if regex.fullmatch("[a-z]+", deri):
              deris.add(deri)
        else:
          break
    stems = set()
    for pos in poses:
      for rule_pos, suffixes in (
          ("noun", noun_suffixes),
          ("verb", verb_suffixes),
          ("adjective", adjective_suffixes),
          ("adverb", adverb_suffixes)):
        if pos == rule_pos:
          for suffix in suffixes:
            if word.endswith(suffix):
              stem = word[:-len(suffix)]
              if len(stem) >= 2:
                stems.add(stem)
    valid_stems = set()
    for stem in stems:
      if len(stem) >= 8:
        valid_stems.add(stem)
      for deri in deris:
        hit = False
        if deri == stem:
          hit = True
        if stem[:3] == deri[:3] and len(stem) >= 4:
          prefix = deri[:len(stem)]
          if tkrzw.Utility.EditDistanceLev(stem, prefix) < 2:
            hit = True
        if hit:
          valid_stems.add(deri)
    return list(valid_stems)

  def MergeRecord(self, key, word_dicts, aux_trans, aoa_words, keywords, core_words,
                  phrase_prob_dbm, tran_prob_dbm, rev_prob_dbm, cooc_prob_dbm):
    word_entries = {}
    word_shares = collections.defaultdict(float)
    word_trans = collections.defaultdict(set)
    entry_tran_texts = collections.defaultdict(list)
    num_words = 0
    for label, word_dict in word_dicts:
      dict_entries = word_dict.get(key)
      if not dict_entries: continue
      for entry in dict_entries:
        num_words += 1
        word = entry["word"]
        entries = word_entries.get(word) or []
        entries.append((label, entry))
        word_entries[word] = entries
        texts = entry.get("text")
        if texts:
          text_score = len(texts) * 1.0
          for pos, text in texts:
            trans = self.ExtractTextLabelTrans(text)
            if trans:
              text_score += 0.5
              word_trans[word].update(trans)
          word_shares[word] += math.log2(1 + text_score)
      dict_entries = word_dict.get(key + "\ttranslation")
      if dict_entries:
        for entry in dict_entries:
          word = entry["word"]
          tran_texts = entry.get("text")
          if not tran_texts: continue
          for tran_pos, tran_text in tran_texts:
            tran_key = word + "\t" + label + "\t" + tran_pos
            entry_tran_texts[tran_key].append(tran_text)
            trans = self.ExtractTextLabelTrans(tran_text)
            if trans:
              word_trans[word].update(trans)
    sorted_word_shares = sorted(word_shares.items(), key=lambda x: x[1], reverse=True)
    if len(sorted_word_shares) > 1 and aux_trans and tran_prob_dbm:
      spell_ratios = {}
      if phrase_prob_dbm:
        word_probs = {}
        for word, share in sorted_word_shares:
          if word in word_probs: continue
          word_probs[word] = self.GetPhraseProb(phrase_prob_dbm, "en", word)
        sum_prob = sum([x[1] for x in word_probs.items()])
        for word, prob in word_probs.items():
          spell_ratios[word] = prob / sum_prob
      word_scores = []
      for word, share in sorted_word_shares:
        score = 0.0
        if word in keywords:
          score += 0.1
        cap_aux_trans = aux_trans.get(word) or []
        if cap_aux_trans:
          score += 0.1
        cap_word_trans = word_trans.get(word) or []
        cap_trans = set(cap_aux_trans).union(cap_word_trans)
        tran_score = 0.0
        if cap_trans:
          key = tkrzw_dict.NormalizeWord(word)
          tsv = tran_prob_dbm.GetStr(key)
          if tsv:
            fields = tsv.split("\t")
            max_prob = 0.0
            sum_prob = 0.0
            for i in range(0, len(fields), 3):
              src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
              if src != word:
                prob *= 0.1
              if not regex.search(r"[\p{Han}\{Hiragana}]", trg):
                prob *= 0.5
                if regex.match(r"[A-Z]", src):
                  prob *= 0.5
              if trg in cap_trans:
                max_prob = max(max_prob, prob)
                sum_prob += prob
            tran_score += (sum_prob * max_prob) ** 0.5
        spell_score = (spell_ratios.get(word) or 0.0) * 0.5
        score += ((tran_score + 0.05) * (share + 0.05) * (spell_score + 0.05)) ** (1 / 3)
        word_scores.append((word, score))
      sorted_word_shares = sorted(word_scores, key=lambda x: x[1], reverse=True)
    share_sum = sum([x[1] for x in sorted_word_shares])
    merged_entry = []
    for word, share in sorted_word_shares:
      entries = word_entries[word]
      word_entry = {}
      word_entry["word"] = word
      stem = " ".join(self.tokenizer.Tokenize("en", word, False, True))
      effective_labels = set()
      surfaces = set([word.lower()])
      is_keyword = word in aux_trans or word in aoa_words or word in keywords
      for label, entry in entries:
        if label not in self.surfeit_labels or is_keyword:
          effective_labels.add(label)
        for top_name in top_names:
          if label not in self.top_labels and top_name in word_entry: continue
          value = entry.get(top_name)
          if value:
            value = unicodedata.normalize('NFKC', value)
            word_entry[top_name] = value
        for infl_name in inflection_names:
          value = entry.get(infl_name)
          if value:
            surfaces.add(value.lower())
      if merged_entry and not effective_labels:
        continue
      for label, entry in entries:
        texts = entry.get("text")
        if not texts: continue
        has_good_text = False
        for pos, text in texts:
          pure_text = regex.sub(r"[^\p{Latin}\p{Han}\p{Hiragana}\p{Katakana}\d]", "", text)
          if not pure_text or pure_text == stem:
            continue
          has_good_text = True
        if not has_good_text:
          continue
        for pos, text in texts:
          items = word_entry.get("item") or []
          tran_key = word + "\t" + label + "\t" + pos
          sections = []
          for section in text.split(" [-] "):
            if not sections:
              sections.append(section)
              continue
            eg_match = regex.search(r"^e\.g\.: (.*)", section)
            if eg_match:
              eg_text = eg_match.group(1).lower()
              eg_words = regex.findall("[-\p{Latin}]+", eg_text)
              hit = False
              for surface in surfaces:
                if surface in eg_words:
                  hit = True
                  break
              if not hit: continue
            sections.append(section)
          text = " [-] ".join(sections)
          tran_texts = entry_tran_texts.get(tran_key)
          if tran_texts:
            del entry_tran_texts[tran_key]
            for tran_text in tran_texts:
              tran_item = {"label": label, "pos": pos, "text": tran_text}
              items.append(tran_item)
          item = {"label": label, "pos": pos, "text": text}
          items.append(item)
          word_entry["item"] = items
      if "item" not in word_entry:
        continue
      prob = None
      if phrase_prob_dbm:
        prob = self.GetPhraseProb(phrase_prob_dbm, "en", word)
        if stem.lower() != word.lower():
          if word.count(" "):
            prob *= 0.5
          else:
            prob *= 0.1
        word_entry["probability"] = "{:.7f}".format(prob).replace("0.", ".")
        if self.min_prob_map:
          has_good_label = False
          for item in word_entry["item"]:
            if item["label"] not in self.min_prob_map:
              has_good_label = True
              break
          if not has_good_label:
            new_items = []
            for item in word_entry["item"]:
              is_good_item = True
              for label, min_prob in self.min_prob_map.items():
                if item["label"] == label:
                  if is_keyword:
                    min_prob *= 0.1
                  if prob < min_prob:
                    is_good_item = False
              if is_good_item:
                new_items.append(item)
            word_entry["item"] = new_items
      if not word_entry.get("item"):
        continue
      if stem != word and stem in core_words and word not in core_words:
        continue
      share_ratio = share / share_sum
      if share_ratio < 1:
        word_entry["share"] = "{:.3f}".format(share_ratio).replace("0.", ".")
      merged_entry.append(word_entry)
    return merged_entry

  def GetPhraseProb(self, prob_dbm, language, word):
    base_prob = 0.000000001
    tokens = self.tokenizer.Tokenize(language, word, False, True)
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

  def SetAOA(self, word_entry, entries, aoa_words, live_words, phrase_prob_dbm):
    word = word_entry["word"]
    phrase_prob = min(float(word_entry.get("probability") or 0), 0.0000001)
    share = float(word_entry.get("share") or 1)
    share_bias = 0.0
    if share < 0.5:
      share_bias = (0.5 - share) * 4
    aoa = aoa_words.get(word)
    if aoa:
      aoa += share_bias
      word_entry["aoa"] = "{:.3f}".format(aoa)
    concepts = set()
    for label, entry in entries:
      stems = entry.get("stem")
      if stems:
        for stem in stems:
          concepts.add(stem)
      core = entry.get("core")
      if core:
        concepts.add(core)
    min_aoa = sys.maxsize
    for concept in concepts:
      if not live_words.Get(concept):
        continue
      aoa = aoa_words.get(concept)
      if aoa:
        if phrase_prob and phrase_prob_dbm:
          concept_prob = self.GetPhraseProb(phrase_prob_dbm, "en", concept)
          diff = max(math.log(concept_prob) - math.log(phrase_prob), 0.0)
          aoa += min(diff * 1.0, 1.0)
        else:
          aoa += 1.0
        min_aoa = min(min_aoa, aoa)
    if min_aoa < sys.maxsize:
      min_aoa += share_bias
      word_entry["aoa_concept"] = "{:.3f}".format(min_aoa)
    bases = set()
    for label, entry in entries:
      tmp_bases = entry.get("base")
      if tmp_bases:
        for base in tmp_bases:
          bases.add(base)
    stem = " ".join(self.tokenizer.Tokenize("en", word, False, True))
    if stem != word:
      bases.add(stem)
    min_aoa = sys.maxsize
    for base in bases:
      if not live_words.Get(base):
        continue
      aoa = aoa_words.get(base)
      if aoa:
        aoa += 1.0
        min_aoa = min(min_aoa, aoa)
    if min_aoa < sys.maxsize:
      min_aoa += share_bias
      word_entry["aoa_base"] = "{:.3f}".format(min_aoa)

  def ExtractTextLabelTrans(self, text):
    trans = []
    match = regex.search(r"\[translation\]: ", text)
    if match:
      text = text[match.end():]
      text = regex.sub(r"\[-.*", "", text)
      text = regex.sub(r"\(.*?\)", "", text)
      for tran in text.split(","):
        tran = unicodedata.normalize('NFKC', tran)
        tran = tran.strip()
        tran = regex.sub(r"[\p{S}\p{P}]+ *(が|の|を|に|へ|と|より|から|で|や)", "", tran)
        tran = regex.sub(r"[～\p{S}\p{P}]", " ", tran)
        tran = regex.sub(r"[\s]+", " ", tran).strip()
        if tran:
          trans.append(tran)
    return trans

  def SetTranslations(self, entry, aux_trans, tran_prob_dbm, rev_prob_dbm):
    word = entry["word"]
    tran_probs = {}
    if tran_prob_dbm:
      key = tkrzw_dict.NormalizeWord(word)
      tsv = tran_prob_dbm.GetStr(key)
      if tsv:
        fields = tsv.split("\t")
        extra_records = []
        for i in range(0, len(fields), 3):
          src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
          if src != word:
            prob *= 0.1
          norm_trg = tkrzw_dict.NormalizeWord(trg)
          if tkrzw_dict.IsStopWord("ja", norm_trg) or len(norm_trg) < 2:
            prob *= 0.5
          prob **= 0.8
          tran_probs[norm_trg] = prob
          stem_trg = regex.sub(
            r"([\p{Han}\p{Katakana}ー]{2,})(する|すること|される|されること|をする)$",
            r"\1", norm_trg)
          if stem_trg != norm_trg:
            extra_records.append((stem_trg, prob * 0.5))
          stem_trg = self.tokenizer.CutJaWordNounParticle(norm_trg)
          if stem_trg != norm_trg:
            extra_records.append((stem_trg, prob * 0.5))
          stem_trg = regex.sub(r"([\p{Han}\p{Katakana}ー]{2,})(的|的な|的に)$", r"\1", norm_trg)
          if stem_trg != norm_trg:
            extra_records.append((stem_trg, prob * 0.5))
          if self.tokenizer.IsJaWordSahenNoun(norm_trg):
            long_trg = norm_trg + "する"
            extra_records.append((long_trg, prob * 0.5))
        for extra_trg, extra_prob in extra_records:
          tran_probs[extra_trg] = max(tran_probs.get(extra_trg) or 0.0, extra_prob)
    word_aux_trans = aux_trans.get(word)
    count_aux_trans = {}
    if word_aux_trans:
      for aux_tran in word_aux_trans:
        count = (count_aux_trans.get(aux_tran) or 0) + 1
        count_aux_trans[aux_tran] = count
      aux_weight = 1.0
      extra_records = []
      for aux_tran, count in count_aux_trans.items():
        aux_score = (0.01 ** (1 / count)) * aux_weight
        prob = (tran_probs.get(aux_tran) or 0) + aux_score
        tran_probs[aux_tran] = prob
        stem_tran = regex.sub(
          r"([\p{Han}\p{Katakana}ー]{2,})(する|すること|される|されること|をする)$",
          r"\1", aux_tran)
        if stem_tran != aux_tran:
          extra_records.append((stem_tran, aux_score * 0.5))
        stem_tran = self.tokenizer.CutJaWordNounParticle(aux_tran)
        if stem_tran != aux_tran:
          extra_records.append((stem_tran, aux_score * 0.5))
        stem_tran = regex.sub(r"([\p{Han}\p{Katakana}ー]{2,})(的|的な|的に)$", r"\1", aux_tran)
        if stem_tran != aux_tran:
          extra_records.append((stem_tran, aux_score * 0.5))
        if self.tokenizer.IsJaWordSahenNoun(aux_tran):
          long_tran = aux_tran + "する"
          extra_records.append((long_tran, aux_score * 0.5))
        aux_weight *= 0.9
      for extra_tran, extra_prob in extra_records:
        tran_probs[extra_tran] = max(tran_probs.get(extra_tran) or 0.0, extra_prob)
    translations = {}
    tran_labels = {}
    def Vote(tran, weight, label):
      norm_tran = tkrzw_dict.NormalizeWord(tran)
      score = 0.00001
      if rev_prob_dbm:
        prob = self.GetPhraseProb(rev_prob_dbm, "ja", tran)
        prob = max(prob, 0.0000001)
        prob = math.exp(-abs(math.log(0.001) - math.log(prob))) * 0.1
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
      text = sections[0]
      text = regex.sub(r"。 *(また|または|又は)、.*?。", r"。", text)
      if (label in self.gross_labels and
          regex.search(r"[\p{Han}\p{Hiragana}\p{Katakana}ー]", text)):
        weight = body_weight
        body_weight *= 0.9
        if regex.search(r"[\(（《〔\{\(]([^)）》〕\}\]]+[・、])?" +
                        r"(俗|俗語|スラング|卑|卑語|隠語|古|古語|廃|廃用|廃語)+[)）》〕\}\]]",
                        text):
          weight *= 0.1
        text = regex.sub(r"[\(（《〔\{\(].*?[)）》〕\}\]]", "〜", text)
        text = regex.sub(r"[･・]", "", text)
        text = regex.sub(r"\s+", " ", text).strip()
        if regex.search(
            r"の(直接法|直説法|仮定法)?(現在|過去)?(第?[一二三]人称)?[ ・･、]?" +
            r"(単数|複数|現在|過去|比較|最上|進行|完了|動名詞|単純)+[ ・･、]?" +
            r"(形|型|分詞|級|動名詞|名詞|動詞|形容詞|副詞)+", text):
          continue
        if regex.search(r"の(直接法|直説法|仮定法)(現在|過去)", text):
          continue
        if regex.search(r"の(動名詞|異綴|異体|古語|略|省略|短縮|頭字語)", text):
          continue
        if regex.search(r"その他、[^。、]{12,}", text):
          continue
        text = regex.sub(r" \[-+\] .*", "", text).strip()
        text = regex.sub(r" -+ .*", "", text).strip()
        for tran in regex.split("[。|、|；|,|;]", text):
          if len(translations) > 1:
            if tran in ("また", "または", "又は", "しばしば"):
              continue
          if regex.search(r"^[ \p{Latin}]+〜", tran):
            continue
          tran = regex.sub(r"^[\p{S}\p{P}]+ *(が|の|を|に|へ|と|より|から|で|や)", "", tran)
          tran = regex.sub(r"[～〜]", "", tran)
          tokens = self.tokenizer.Tokenize("ja", tran, False, False)
          if len(tokens) > 6:
            break
          if regex.search(r"^[ \p{Latin}]+ *など", tran):
            continue
          if regex.search(r"[\p{Latin}].*の.*(詞形|綴り)$", tran):
            continue
          tran = " ".join(tokens)
          tran = regex.sub(r"([\p{Han}\p{Hiragana}\p{Katakana}ー]) +", r"\1", tran)
          tran = regex.sub(r" +([\p{Han}\p{Hiragana}\p{Katakana}ー])", r"\1", tran)
          tran = regex.sub(r"[\s]+", " ", tran).strip()
          if tran:
            Vote(tran, weight, label)
            weight *= 0.8
      if label in self.tran_list_labels:
        for section in sections:
          trans = self.ExtractTextLabelTrans(section)
          if not trans: continue
          weight = tran_weight
          tran_weight *= 0.9
          for tran in trans:
            Vote(tran, weight, label)
            weight *= 0.85
    poses = set()
    for item in entry["item"]:
      poses.add(item["pos"])
    pure_verb = len(poses) == 1 and "verb" in poses
    pure_adjective = len(poses) == 1 and "adjective" in poses
    pure_adverb = len(poses) == 1 and "adverb" in poses
    bonus_translations = []
    scored_translations = set()
    for tran, score in translations.items():
      tran = unicodedata.normalize('NFKC', tran)
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
    uniq_trans = set()
    final_translations = []
    max_elems = int(min(max(math.log2(len(entry["item"])), 2), 6) * 6)
    for tran, score in deduped_translations:
      norm_tran = tkrzw_dict.NormalizeWord(tran)
      if norm_tran in uniq_trans:
        continue
      uniq_trans.add(norm_tran)
      if len(final_translations) < max_elems or score >= 0.001:
        final_translations.append(tran)
    sorted_aux_trans = sorted(count_aux_trans.items(), key=lambda x: -x[1])
    for aux_tran, count in sorted_aux_trans:
      if pure_verb and self.tokenizer.IsJaWordSahenNoun(aux_tran):
        aux_tran += "する"
      if pure_adjective and self.tokenizer.IsJaWordAdjvNoun(aux_tran):
        aux_tran += "な"
      if pure_adverb and self.tokenizer.IsJaWordAdjvNoun(aux_tran):
        aux_tran += "に"
      if len(final_translations) >= max_elems: break
      norm_tran = tkrzw_dict.NormalizeWord(aux_tran)
      if norm_tran in uniq_trans:
        continue
      uniq_trans.add(norm_tran)
      final_translations.append(aux_tran)
    if final_translations:
      entry["translation"] = final_translations

  def SetRelations(self, word_entry, entries, word_dicts, live_words, rev_live_words,
                   phrase_prob_dbm, tran_prob_dbm, cooc_prob_dbm):
    word = word_entry["word"]
    norm_word = tkrzw_dict.NormalizeWord(word)
    scores = {}
    def Vote(rel_word, label, weight):
      values = scores.get(rel_word) or []
      values.append((weight, label))
      scores[rel_word] = values
    parents = set()
    children = set()
    for label, entry in entries:
      stems = entry.get("stem")
      if stems:
        for stem in stems:
          parents.add(stem)
      stem_children = entry.get("stem_child")
      if stem_children:
        for child in stem_children:
          children.add(child)
      core = entry.get("core")
      if core:
        parents.add(core)
      core_children = entry.get("core_child")
      if core_children:
        for child in core_children:
          children.add(child)
      bases = entry.get("base")
      if bases:
        for base in bases:
          parents.add(base)
      base_children = entry.get("base_child")
      if base_children:
        for child in base_children:
          children.add(child)
      for rel_name, rel_weight in rel_weights.items():
        ent_rel_words = []
        expr = entry.get(rel_name)
        if expr:
          for rel_word in expr.split(","):
            rel_word = rel_word.strip()
            ent_rel_words.append(rel_word)
          if ent_rel_words:
            scored_rel_words = []
            for i, rel_word in enumerate(ent_rel_words):
              weight = 30 / (min(i, 30) + 30)
              weight *= rel_weight
              Vote(rel_word, label, weight)
        texts = entry.get("text")
        if texts:
          base_weight = 1.1
          for text in texts:
            for field in text[1].split(" [-] "):
              if not field.startswith("[" + rel_name + "]: "): continue
              field = regex.sub(r"^[^:]+: ", "", field)
              field = regex.sub(r"\(.*?\) *", "", field)
              for i, rel_word in enumerate(field.split(",")):
                rel_word = rel_word.strip()
                if rel_word:
                  weight = 30 / (min(i, 30) + 30)
                  weight *= rel_weight * base_weight
                  Vote(rel_word, label, weight)
            base_weight *= 0.95
    translations = list(word_entry.get("translation") or [])
    if tran_prob_dbm:
      tsv = tran_prob_dbm.GetStr(norm_word)
      if tsv:
        fields = tsv.split("\t")
        for i in range(0, len(fields), 3):
          src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
          translations.append(trg)
    translations = set([tkrzw_dict.NormalizeWord(x) for x in translations])
    rel_words = []
    for rel_word, votes in scores.items():
      norm_rel_word = tkrzw_dict.NormalizeWord(rel_word)
      label_weights = {}
      for weight, label in votes:
        old_weight = label_weights.get(label) or 0.0
        label_weights[label] = max(old_weight, weight)
      total_weight = 0
      for label, weight in label_weights.items():
        total_weight += weight
      if tran_prob_dbm:
        tsv = tran_prob_dbm.GetStr(norm_rel_word)
        if tsv:
          bonus = 0.0
          fields = tsv.split("\t")
          for i in range(0, len(fields), 3):
            src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
            norm_tran = tkrzw_dict.NormalizeWord(trg)
            for dict_tran in translations:
              if dict_tran == norm_tran:
                bonus = max(bonus, 1.0)
              elif len(dict_tran) >= 2 and norm_tran.startswith(dict_tran):
                bonus = max(bonus, 0.3)
              elif len(norm_tran) >= 2 and dict_tran.startswith(norm_tran):
                bonus = max(bonus, 0.2)
              elif len(dict_tran) >= 2 and norm_tran.find(dict_tran) >= 0:
                bonus = max(bonus, 0.1)
              elif len(norm_tran) >= 2 and dict_tran.find(norm_tran) >= 0:
                bonus = max(bonus, 0.1)
              dist = tkrzw.Utility.EditDistanceLev(dict_tran, norm_tran)
              dist /= max(len(dict_tran), len(norm_tran))
              if dist < 0.3:
                bonus = max(bonus, 0.3)
          total_weight += bonus
      score = 1.0
      if phrase_prob_dbm:
        prob = self.GetPhraseProb(phrase_prob_dbm, "en", rel_word)
        prob = max(prob, 0.0000001)
        score += math.exp(-abs(math.log(0.001) - math.log(prob))) * 0.1
      score *= total_weight
      if tkrzw_dict.IsStopWord("en", norm_rel_word):
        if tkrzw_dict.IsStopWord("en", norm_word):
          score *= 0.3
        else:
          score *= 0.1
      rel_words.append((rel_word, score))
    rel_words = sorted(rel_words, key=lambda x: x[1], reverse=True)
    uniq_words = set()
    final_rel_words = []
    for rel_word, score in rel_words:
      if rel_word in parents or rel_word in children:
        continue
      if not live_words.Get(rel_word) or rel_word == word:
        continue
      norm_rel_word = tkrzw_dict.NormalizeWord(rel_word)
      if not norm_rel_word: continue
      if norm_rel_word in uniq_words: continue
      uniq_words.add(norm_rel_word)
      hit = False
      for label, word_dict in word_dicts:
        if label in self.surfeit_labels: continue
        if norm_rel_word in word_dict:
          hit = True
          break
      if not hit: continue
      final_rel_words.append(rel_word)
    if final_rel_words:
      max_elems = int(min(max(math.log2(len(word_entry["item"])), 2), 6) * 6)
      word_entry["related"] = final_rel_words[:max_elems]
    scored_parents = []
    for parent in parents:
      if not live_words.Get(parent) or parent == word:
        continue
      prob = self.GetPhraseProb(phrase_prob_dbm, "en", parent)
      scored_parents.append((parent, prob))
    scored_parents = sorted(scored_parents, key=lambda x: x[1], reverse=True)
    if scored_parents:
      word_entry["parent"] = [x[0] for x in scored_parents]
    scored_children = []
    for child in children:
      if not live_words.Get(child) or child == word:
        continue
      prob = self.GetPhraseProb(phrase_prob_dbm, "en", child)
      scored_children.append((child, prob))
    scored_children = sorted(scored_children, key=lambda x: x[1], reverse=True)
    if scored_children:
      word_entry["child"] = [x[0] for x in scored_children]
    prob = float(live_words.GetStr(word) or 0.0)
    if prob >= 0.000001 and regex.fullmatch(r"[-\p{Latin}]+", word):
      prefix = word + " "
      idioms = []
      it = live_words.MakeIterator()
      it.Jump(prefix)
      while True:
        rec = it.GetStr()
        if not rec: break
        cmp_word, cmp_prob = rec
        if not cmp_word.startswith(prefix): break
        cmp_prob = float(cmp_prob)
        cmp_score = cmp_prob / prob
        if cmp_score >= 0.001:
          has_particle = False
          for cmp_token in cmp_word.split(" ")[1:]:
            if cmp_token in particles:
              has_particle = True
              break
          if has_particle:
            cmp_score *= 3.0
          idioms.append((cmp_word, cmp_score))
        it.Next()
      it = rev_live_words.MakeIterator()
      it.Jump(prefix)
      while True:
        rec = it.GetStr()
        if not rec: break
        cmp_word, cmp_prob = rec
        if not cmp_word.startswith(prefix): break
        cmp_word = " ".join(reversed(cmp_word.split(" ")))
        cmp_prob = float(cmp_prob)
        cmp_score = cmp_prob / prob
        if cmp_score >= 0.001:
          has_particle = False
          for cmp_token in cmp_word.split(" ")[:-1]:
            if cmp_token in particles:
              has_particle = True
              break
          if has_particle:
            cmp_score *= 3.0
          cmp_score * 0.9
          idioms.append((cmp_word, cmp_score))
        it.Next()
      idioms = sorted(idioms, key=lambda x: x[1], reverse=True)
      uniq_idioms = set()
      final_idioms = []
      for idiom, prob in idioms:
        if idiom in uniq_idioms: continue
        uniq_idioms.add(idiom)
        final_idioms.append(idiom)
      if final_idioms:
        max_elems = int(min(max(math.log2(len(word_entry["item"])), 2), 6) * 4)
        word_entry["idiom"] = final_idioms[:max_elems]

  def SetCoocurrences(self, word_entry, entries, word_dicts, phrase_prob_dbm, cooc_prob_dbm):
    word = word_entry["word"]
    norm_word = tkrzw_dict.NormalizeWord(word)
    tokens = self.tokenizer.Tokenize("en", word, True, True)
    cooc_words = {}
    for token in tokens:
      phrase_prob = self.GetPhraseProb(phrase_prob_dbm, "en", token)
      word_idf = math.log(phrase_prob) * -1
      word_weight = word_idf ** 2
      tsv = cooc_prob_dbm.GetStr(token)
      if tsv:
        for field in tsv.split("\t")[:24]:
          cooc_word, cooc_prob = field.split(" ", 1)
          if cooc_word not in tokens:
            old_score = cooc_words.get(cooc_word) or 0.0
            cooc_words[cooc_word] = old_score + float(cooc_prob) * word_weight
    merged_cooc_words = sorted(cooc_words.items(), key=lambda x: x[1], reverse=True)
    weighed_cooc_words = []
    for cooc_word, cooc_score in merged_cooc_words:
      cooc_prob = self.GetPhraseProb(phrase_prob_dbm, "en", cooc_word)
      cooc_idf = math.log(cooc_prob) * -1
      cooc_score *= cooc_idf ** 2
      if tkrzw_dict.IsStopWord("en", cooc_word):
        if tkrzw_dict.IsStopWord("en", norm_word):
          cooc_score *= 0.3
        else:
          cooc_score *= 0.1
      weighed_cooc_words.append((cooc_word, cooc_score))
    sorted_cooc_words = sorted(weighed_cooc_words, key=lambda x: x[1], reverse=True)
    final_cooc_words = []
    for cooc_word, cooc_score in sorted_cooc_words:
      if len(final_cooc_words) >= 16: break
      hit = False
      for label, word_dict in word_dicts:
        if label in self.surfeit_labels: continue
        if cooc_word in word_dict:
          hit = True
          break
      if not hit: continue
      final_cooc_words.append(cooc_word)
    if final_cooc_words:
      word_entry["cooccurrence"] = final_cooc_words


def main():
  args = sys.argv[1:]
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-body.tkh"
  core_labels = set((tkrzw_dict.GetCommandFlag(args, "--core", 1) or "xa,wn").split(","))
  gross_labels = set((tkrzw_dict.GetCommandFlag(args, "--gross", 1) or "wj").split(","))
  top_labels = set((tkrzw_dict.GetCommandFlag(args, "--top", 1) or "xa,we").split(","))
  slim_labels = set((tkrzw_dict.GetCommandFlag(args, "--slim", 1) or "we").split(","))
  surfeit_labels = set((tkrzw_dict.GetCommandFlag(args, "--surfeit", 1) or "we").split(","))
  tran_list_labels = set((tkrzw_dict.GetCommandFlag(args, "--tran_list", 1) or "xa,wn,we").split(","))
  phrase_prob_path = tkrzw_dict.GetCommandFlag(args, "--phrase_prob", 1) or ""
  tran_prob_path = tkrzw_dict.GetCommandFlag(args, "--tran_prob", 1) or ""
  tran_aux_paths = (tkrzw_dict.GetCommandFlag(args, "--tran_aux", 1) or "").split(",")
  rev_prob_path = tkrzw_dict.GetCommandFlag(args, "--rev_prob", 1) or ""
  cooc_prob_path = tkrzw_dict.GetCommandFlag(args, "--cooc_prob", 1) or ""
  aoa_paths = (tkrzw_dict.GetCommandFlag(args, "--aoa", 1) or "").split(",")
  keyword_path = tkrzw_dict.GetCommandFlag(args, "--keyword", 1) or ""
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
  BuildUnionDBBatch(input_confs, output_path, core_labels, gross_labels,
                    surfeit_labels, top_labels, slim_labels, tran_list_labels,
                    phrase_prob_path, tran_prob_path, tran_aux_paths,
                    rev_prob_path, cooc_prob_path, aoa_paths, keyword_path,
                    min_prob_map).Run()


if __name__=="__main__":
  main()
