#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to append WordNet Japanese translation to the WordNet database
#
# Usage:
#   append_wordnet_jpn.py [--input str] [--output str] [--wnjpn str]
#     [--phrase_prob str] [--rev_prob str] [--tran_prob str]
#     [--tran_aux str] [--tran_subaux str] [--tran_thes str] [--quiet]
#
# Example:
#   ./append_wordnet_jpn.py --input wordnet.tkh --output wordnet-tran.tkh \
#     --wnjpn wnjpn-ok.tab --tran_aux wiktionary-tran.tsv
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
import tkrzw_tokenizer
import unicodedata


MAX_TRANSLATIONS_PER_WORD = 10


logger = tkrzw_dict.GetLogger()


class AppendWordnetJPNBatch:
  def __init__(self, input_path, output_path, wnjpn_path, vote_path, wnmt_paths, feedback_path,
               phrase_prob_path, rev_prob_path, tran_prob_path, nmt_prob_path,
               tran_aux_paths, tran_subaux_paths, tran_thes_path, hint_path, synonym_path):
    self.input_path = input_path
    self.output_path = output_path
    self.wnjpn_path = wnjpn_path
    self.vote_path = vote_path
    self.wnmt_paths = wnmt_paths
    self.feedback_path = feedback_path
    self.phrase_prob_path = phrase_prob_path
    self.rev_prob_path = rev_prob_path
    self.tran_prob_path = tran_prob_path
    self.nmt_prob_path = nmt_prob_path
    self.tran_aux_paths = tran_aux_paths
    self.tran_subaux_paths = tran_subaux_paths
    self.tran_thes_path = tran_thes_path
    self.hint_path = hint_path
    self.synonym_path = synonym_path

  def Run(self):
    tokenizer = tkrzw_tokenizer.Tokenizer()
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}, wnjpn_path={}".format(
                  self.input_path, self.output_path, self.wnjpn_path))
    wnjpn_trans = self.ReadTranslations()
    wnmt_trans = self.ReadMachineTranslations()
    if self.feedback_path:
      feedback_trans = self.ReadFeedbackTranslations()
    else:
      feedback_trans = None
    aux_trans, subaux_trans, tran_thes = self.ReadAuxTranslations()
    votes = self.ReadVotes()
    hints = self.ReadHints()
    extra_synonyms = self.ReadSynonyms()
    synset_index = self.ReadSynsetIndex()
    tran_index = {}
    tran_index = self.ReadTranIndex(synset_index)
    self.AppendTranslations(
      wnjpn_trans, votes, wnmt_trans, feedback_trans, aux_trans, subaux_trans,
      tran_thes, hints, extra_synonyms,
      synset_index, tran_index)
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def ReadTranslations(self):
    start_time = time.time()
    logger.info("Reading translations: path={}".format(self.wnjpn_path))
    trans = collections.defaultdict(list)
    num_trans = 0
    with open(self.wnjpn_path) as input_file:
      for line in input_file:
        line = line.strip()
        fields = line.split("\t")
        if len(fields) != 3: continue
        synset_id, text, src = fields
        text = unicodedata.normalize('NFKC', text)
        trans[synset_id].append((text, src))
        num_trans += 1
        if num_trans % 10000 == 0:
          logger.info("Reading translations: synsets={}, word_entries={}".format(
            len(trans), num_trans))
    logger.info(
      "Reading translations done: synsets={}, translations={}, elapsed_time={:.2f}s".format(
        len(trans), num_trans, time.time() - start_time))
    return trans

  def ReadVotes(self):
    synset_votes = {}
    if self.vote_path:
      start_time = time.time()
      logger.info("Reading votes: path={}".format(self.vote_path))
      word_votes = collections.defaultdict(list)
      num_votes = 0
      with open(self.vote_path) as input_file:
        for line in input_file:
          line = line.strip()
          fields = line.split("\t")
          if len(fields) != 4: continue
          word, synset_id, text, score = fields
          score = int(score)
          if score >= 0:
            word_votes[word].append((synset_id, score))
          num_votes += 1
          if num_votes % 10000 == 0:
            logger.info("Reading votes: votes={}".format(num_votes))
      logger.info("Reading votes done: votes={}, elapsed_time={:.2f}s".format(
        len(word_votes), time.time() - start_time))
      for word, items in word_votes.items():
        score_max = 0
        for synset, score in items:
          score_max = max(score, score_max)
        if score_max > 0:
          for synset, score in items:
            if score <= 0: continue
            score = score / score_max
            score *= max(1, math.log(len(items) + 1))
            key = word + ":" + synset
            synset_votes[key] = score
    return synset_votes

  def ReadMachineTranslations(self):
    trans = collections.defaultdict(list)
    for wnmt_path in self.wnmt_paths.split(","):
      if not wnmt_path: continue
      start_time = time.time()
      logger.info("Reading machine translations: path={}".format(wnmt_path))
      num_trans = 0
      with open(wnmt_path) as input_file:
        for line in input_file:
          line = line.strip()
          fields = line.split("\t")
          if len(fields) <= 2: continue
          synset_id = fields[0]
          word = fields[1]
          key = synset_id + ":" + word
          for text in fields[2:]:
            text = unicodedata.normalize('NFKC', text)
            text = regex.sub(r"[・]", "", text)
            text = regex.sub(r"\s+", " ", text).strip()
            if text:
              trans[key].append(text)
          num_trans += 1
          if num_trans % 10000 == 0:
            logger.info("Reading machine translations: synsets={}, word_entries={}".format(
              len(trans), num_trans))
      logger.info(
        "Reading machine translations done: synsets={}, translations={},"
        " elapsed_time={:.2f}s".format(
          len(trans), num_trans, time.time() - start_time))
    return trans

  def ReadFeedbackTranslations(self):
    start_time = time.time()
    logger.info("Reading feadback translations: path={}".format(self.feedback_path))
    trans = {}
    num_trans = 0
    with open(self.feedback_path) as input_file:
      for line in input_file:
        line = line.strip()
        fields = line.split("\t")
        if len(fields) < 2: continue
        key = unicodedata.normalize('NFKC', fields[0])
        translations = []
        for text in fields[1:]:
          text = unicodedata.normalize('NFKC', text)
          if text:
            translations.append(text)
        if key and translations:
          trans[key] = translations
        num_trans += 1
        if num_trans % 10000 == 0:
          logger.info("Reading translations: synsets={}, word_entries={}".format(
            len(trans), num_trans))
    logger.info(
      "Reading feedback translations done: synsets={}, translations={}, elapsed_time={:.2f}s"
      .format(len(trans), num_trans, time.time() - start_time))
    return trans

  def ReadAuxTranslations(self):
    aux_trans = collections.defaultdict(list)
    subaux_trans = collections.defaultdict(list)
    for records, paths in (
        (aux_trans, self.tran_aux_paths),
        (subaux_trans, self.tran_subaux_paths)):
      for aux_path in paths:
        if not aux_path: continue
        start_time = time.time()
        logger.info("Reading aux translations: path={}".format(aux_path))
        num_trans = 0
        tmp_records = set()
        with open(aux_path) as input_file:
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
                tmp_records.add((source, target))
            num_trans += 1
            if num_trans % 10000 == 0:
              logger.info("Reading aux translations: records={}, word_entries={}".format(
                len(tmp_records), num_trans))
        logger.info(
          "Reading aux translations done: records={}, word_entries={}, elapsed_time={:.2f}s".format(
            len(tmp_records), num_trans, time.time() - start_time))
        for source, targets in tmp_records:
          records[source].append(targets)
    tran_thes = {}
    if self.tran_thes_path:
      with open(self.tran_thes_path) as input_file:
        for line in input_file:
          fields = line.strip().split("\t")
          if len(fields) >= 2:
            tran_thes[fields[0]] = fields[1:]
    return aux_trans, subaux_trans, tran_thes

  def ReadHints(self):
    if not self.hint_path: return
    hints = {}
    start_time = time.time()
    logger.info("Reading hints: path={}".format(self.hint_path))
    num_hints = 0
    with open(self.hint_path) as input_file:
      for line in input_file:
        fields = line.strip().split("\t")
        if len(fields) < 3: continue
        word, poses, prob = fields[:3]
        hints[word] = (poses, prob)
        num_hints += 1
        if num_hints % 10000 == 0:
          logger.info("Reading hints: hints={}".format(num_hints))
    logger.info(
      "Reading hints done: hints={}, elapsed_time={:.2f}s".format(
        num_hints, time.time() - start_time))
    return hints

  def ReadSynonyms(self):
    if not self.synonym_path: return
    synonyms = {}
    start_time = time.time()
    logger.info("Reading synonyms: path={}".format(self.synonym_path))
    num_records = 0
    with open(self.synonym_path) as input_file:
      for line in input_file:
        fields = line.strip().split("\t")
        if len(fields) < 2: continue
        word = fields[0]
        synonyms[word] = fields[1:]
        num_records += 1
        if num_records % 10000 == 0:
          logger.info("Reading synonyms: records={}".format(num_records))
    logger.info(
      "Reading synonyms done: records={}, elapsed_time={:.2f}s".format(
        num_records, time.time() - start_time))
    return synonyms

  def ReadSynsetIndex(self):
    logger.info("Reading synset index: input_path={}".format(self.input_path))
    synset_index = collections.defaultdict(set)
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False, dbm="HashDBM").OrDie()
    num_words = 0
    it = input_dbm.MakeIterator()
    it.First()
    while True:
      record = it.GetStr()
      if not record: break
      key, serialized = record
      entry = json.loads(serialized)
      for item in entry["item"]:
        word = item["word"]
        synset = item["synset"]
        synset_index[word].add(synset)
      num_words += 1
      if num_words % 10000 == 0:
        logger.info("Reading synsets: words={}".format(num_words))
      it.Next()
    logger.info("Reading synset index done: records={}".format(len(synset_index)))
    return synset_index

  def ReadTranIndex(self, synset_index):
    tran_index = {}
    if not self.tran_prob_path:
      return tran_index
    logger.info("Reading tran index: input_path={}".format(self.tran_prob_path))
    tran_prob_dbm = tkrzw.DBM()
    tran_prob_dbm.Open(self.tran_prob_path, False, dbm="HashDBM").OrDie()
    num_words = 0
    for word in synset_index:
      key = tkrzw_dict.NormalizeWord(word)
      tsv = tran_prob_dbm.GetStr(key)
      if tsv:
        tran_probs = {}
        fields = tsv.split("\t")
        for i in range(0, len(fields), 3):
          src, trg, prob = fields[i], fields[i + 1], fields[i + 2]
          if src != word: continue
          prob = float(prob)
          if prob > 0.04:
            tran_probs[trg] = prob
        if tran_probs:
          tran_index[word] = tran_probs
      num_words += 1
      if num_words % 10000 == 0:
        logger.info("Reading trans: words={}".format(num_words))
    tran_prob_dbm.Close().OrDie()
    logger.info("Reading tran index done: records={}".format(len(tran_index)))
    if self.nmt_prob_path:
      logger.info("Reading NMT probs: path={}".format(self.nmt_prob_path))
      num_probs = 0
      with open(self.nmt_prob_path) as input_file:
        for line in input_file:
          fields = line.strip().split("\t")
          if len(fields) < 3: continue
          word = fields[0]
          if word not in synset_index: continue
          tran_probs = tran_index.get(word) or {}
          for i in range(1, len(fields), 2):
            tran = fields[i]
            prob = float(fields[i + 1]) * 0.3
            if prob > 0.02:
              tran_probs[tran] = (tran_probs.get(tran) or 0) + prob
          if tran_probs:
            tran_index[word] = tran_probs
          num_probs += 1
          if num_probs % 10000 == 0:
            logger.info("Reading NMT probs: records={}".format(num_probs))
      logger.info("Reading NMT probs done: records={}".format(len(tran_index)))
    return tran_index

  def AppendTranslations(self, wnjpn_trans, votes, wnmt_trans, feedback_trans,
                         aux_trans, subaux_trans, tran_thes, hints, extra_synonyms,
                         synset_index, tran_index):
    start_time = time.time()
    logger.info("Appending translations: input_path={}, output_path={}".format(
      self.input_path, self.output_path))
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False, dbm="HashDBM").OrDie()
    phrase_prob_dbm = None
    if self.phrase_prob_path:
      phrase_prob_dbm = tkrzw.DBM()
      phrase_prob_dbm.Open(self.phrase_prob_path, False, dbm="HashDBM").OrDie()
    rev_prob_dbm = None
    if self.rev_prob_path:
      rev_prob_dbm = tkrzw.DBM()
      rev_prob_dbm.Open(self.rev_prob_path, False, dbm="HashDBM").OrDie()
    tokenizer = tkrzw_tokenizer.Tokenizer()
    tran_prob_dbm =None
    if self.tran_prob_path:
      tran_prob_dbm = tkrzw.DBM()
      tran_prob_dbm.Open(self.tran_prob_path, False, dbm="HashDBM").OrDie()
    output_dbm = tkrzw.DBM()
    num_buckets = input_dbm.Count() * 2
    output_dbm.Open(
      self.output_path, True, dbm="HashDBM", truncate=True,
      align_pow=0, num_buckets=num_buckets).OrDie()
    num_words = 0
    num_orig_trans = 0
    num_match_trans = 0
    num_voted_trans = 0
    num_borrowed_trans = 0
    num_items = 0
    num_items_bare = 0
    num_items_rescued = 0
    it = input_dbm.MakeIterator()
    it.First()
    while True:
      record = it.GetStr()
      if not record: break
      key, serialized = record
      entry = json.loads(serialized)
      items = entry["item"]
      spell_ratios = {}
      for item in items:
        word = item["word"]
        phrase_prob = float(item.get("prob") or 0.0)
        spell_ratios[word] = phrase_prob + 0.00000001
      sum_prob = 0.0
      for word, prob in spell_ratios.items():
        sum_prob += prob
      for word, prob in list(spell_ratios.items()):
        spell_ratios[word] = prob / sum_prob
      all_tran_probs = tran_index.get(word) or {}
      for item in items:
        word = item["word"]
        word_extra_synonyms = extra_synonyms.get(word) or [] if extra_synonyms else []
        attrs = ["translation", "synonym", "antonym", "hypernym", "hyponym",
                 "similar", "derivative"]
        for attr in attrs:
          rel_words = item.get(attr)
          if rel_words:
            rel_words = self.SortRelatedWords(
              rel_words, all_tran_probs, tokenizer, phrase_prob_dbm, tran_prob_dbm,
              synset_index, tran_index, word_extra_synonyms)
            item[attr] = rel_words
      for item in items:
        word = item["word"]
        word_extra_synonyms = extra_synonyms.get(word) or [] if extra_synonyms else []
        pos = item["pos"]
        synset = item["synset"]
        links = item.get("link") or {}
        phrase_prob = float(item.get("prob") or 0.0)
        spell_ratio = spell_ratios[word]
        synonyms = self.DeduplicateSynonyms(word, item.get("synonym") or [])
        hypernyms = self.DeduplicateSynonyms(word, item.get("hypernym") or [])
        hyponyms = self.DeduplicateSynonyms(word, item.get("hyponym") or [])
        similars = self.DeduplicateSynonyms(word, item.get("similar") or [])
        derivatives = self.DeduplicateSynonyms(word, item.get("derivative") or [])
        synonym_ids = [synset]
        hypernym_ids = links.get("hypernym") or []
        hyponym_ids = links.get("hyponym") or []
        similar_ids = links.get("similar") or []
        derivative_ids = links.get("derivative") or []
        item_tran_pairs = wnjpn_trans.get(synset) or []
        mt_word_trans = wnmt_trans.get(synset + ":" + word) or []
        mt_bare_trans = wnmt_trans.get(synset + ":-") or []
        mt_tran_set = set(mt_word_trans + mt_bare_trans)
        item_aux_trans = list(aux_trans.get(word) or [])
        item_aux_tran_set = set(item_aux_trans)
        for extra_synonym in word_extra_synonyms[:4]:
          extra_trans = aux_trans.get(extra_synonym)
          if extra_trans:
            item_aux_trans.extend(extra_trans[:4])
        ext_item_aux_trans = list(item_aux_trans)
        ext_item_aux_trans.extend(subaux_trans.get(word) or [])
        ext_aux_trans_set = set(ext_item_aux_trans)
        uniq_synonym_trans = set()
        for synonym in set(synonyms + hypernyms + hyponyms):
          if word[:4] == synonym[:4]: continue
          dist = tkrzw.Utility.EditDistanceLev(word, synonym)
          dist_ratio = dist / max(len(word), len(synonym))
          if dist_ratio < 0.3: continue
          trans = aux_trans.get(synonym)
          if not trans: continue
          for tran in trans:
            tran = regex.sub(r"[・]", "", tran)
            tran = regex.sub(r"\s+", " ", tran).strip()
            if tran:
              uniq_synonym_trans.add(tran)
        self.NormalizeTranslationList(tokenizer, pos, item_aux_trans)
        self.NormalizeTranslationList(tokenizer, pos, ext_item_aux_trans)
        scored_item_trans = collections.defaultdict(float)
        for tran in mt_word_trans:
          if len(tran) > 10: continue
          if tran in mt_bare_trans:
            synonym_match = tran in uniq_synonym_trans
            scored_item_trans[tran] = 1.5 if synonym_match else 1.4
        for tran in mt_bare_trans:
          if len(tran) > 10: continue
          if tran in scored_item_trans: continue
          if tran in ext_aux_trans_set:
            synonym_match = tran in uniq_synonym_trans
            scored_item_trans[tran] = 1.5 if synonym_match else 1.4
        hand_trans = set()
        for tran, src in item_tran_pairs:
          mt_hit = tran in mt_tran_set
          if not mt_hit and src == "mono":
            hit = False
            for item_aux_tran in ext_item_aux_trans:
              dist = tkrzw.Utility.EditDistanceLev(tran, item_aux_tran)
              dist_ratio = dist / max(len(tran), len(item_aux_tran))
              if dist < 0.3:
                hit = True
            if not hit:
              continue
          tran = tokenizer.NormalizeJaWordForPos(pos, tran)
          if tran in mt_tran_set:
            mt_hit = True
          if tran not in scored_item_trans:
            score = 1.3 if mt_hit else 1.0
            scored_item_trans[tran] = score
          if src == "hand":
            hand_trans.add(tran)
        if feedback_trans:
          item_fb_trans = feedback_trans.get(word + ":" + synset) or []
          if item_fb_trans:
            for tran in item_fb_trans:
              tran = tokenizer.NormalizeJaWordForPos(pos, tran)
              if tran not in scored_item_trans:
                scored_item_trans[tran] = 0.8
        for tran, score in list(scored_item_trans.items()):
          if score != 1.0: continue
          cmp_words = tran_thes.get(tran)
          if cmp_words:
            for cmp_word in cmp_words:
              if cmp_word not in scored_item_trans:
                scored_item_trans[cmp_word] = 0.5
        for tran in mt_word_trans:
          if len(tran) > 10: continue
          if tran in scored_item_trans: continue
          if tran not in ext_aux_trans_set: continue
          if tran in uniq_synonym_trans:
            scored_item_trans[tran] = 0.4
          elif len(items) == 1:
            scored_item_trans[tran] = 0.2
        num_items += 1
        bare = not scored_item_trans
        if bare:
          num_items_bare += 1
        num_orig_trans += len(scored_item_trans)
        syno_tran_counts = collections.defaultdict(int)
        hyper_tran_counts = collections.defaultdict(int)
        hypo_tran_counts = collections.defaultdict(int)
        similar_tran_counts = collections.defaultdict(int)
        derivative_tran_counts = collections.defaultdict(int)
        checked_words = set()
        checked_ids = set([synset])
        adopted_rel_trans = set()
        voted_rel_words = set()
        voted_rel_records = set()
        for rel_words, rel_ids, tran_counts in (
            (synonyms, synonym_ids, syno_tran_counts),
            (hypernyms, hypernym_ids, hyper_tran_counts),
            (hyponyms, hyponym_ids, hypo_tran_counts),
            (similars, similar_ids, similar_tran_counts),
            (derivatives, derivative_ids, derivative_tran_counts)):
          for rel_word in rel_words:
            is_similar = self.AreSimilarWords(rel_word, word)
            rel_phrase_prob = 0.0
            if phrase_prob_dbm:
              rel_phrase_prob = self.GetPhraseProb(phrase_prob_dbm, tokenizer, "en", rel_word)
            mean_prob = (phrase_prob * rel_phrase_prob) ** 0.5
            rel_aux_trans = []
            if rel_word not in checked_words:
              checked_words.add(rel_word)
              tmp_aux_trans = aux_trans.get(rel_word)
              if tmp_aux_trans:
                rel_aux_trans.extend(tmp_aux_trans)
            for rel_id in synset_index[rel_word]:
              if rel_id not in rel_ids: continue
              if rel_id not in checked_ids:
                checked_ids.add(rel_id)
                tmp_aux_trans = wnjpn_trans.get(rel_id)
                if tmp_aux_trans:
                  tmp_aux_trans = [x[0] for x in tmp_aux_trans]
                  rel_aux_trans.extend(tmp_aux_trans)
            if rel_aux_trans:
              self.NormalizeTranslationList(tokenizer, pos, rel_aux_trans)
              if not is_similar and mean_prob < 0.0005:
                for item_aux_tran in ext_item_aux_trans:
                  if regex.fullmatch(r"[\p{Hiragana}]{,3}", item_aux_tran): continue
                  if item_aux_tran in rel_aux_trans:
                    if self.IsValidPosTran(tokenizer, pos, item_aux_tran):
                      adopted_rel_trans.add(item_aux_tran)
              if mean_prob < 0.005:
                voted_top = rel_word
                for voted_rel_word in voted_rel_words:
                  if self.AreSimilarWords(rel_word, voted_rel_word):
                    voted_top = voted_rel_word
                    break
                voted_rel_words.add(rel_word)
                for rel_aux_tran in set(rel_aux_trans):
                  voted_record = (voted_top, rel_aux_tran)
                  if voted_record in voted_rel_records:
                    continue
                  voted_rel_records.add(voted_record)
                  tran_counts[rel_aux_tran] += 1
        for rel_tran in adopted_rel_trans:
          scored_item_trans[rel_tran] = max(0.8, scored_item_trans[rel_tran] + 0.25)
          num_match_trans += 1
        if bare:
          for deri_tran, count in derivative_tran_counts.items():
            syno_tran_counts[deri_tran] = syno_tran_counts[deri_tran] + count
          derivative_tran_counts.clear()
        adopted_syno_trans = set()
        for syno_tran, count in syno_tran_counts.items():
          if regex.fullmatch(r"[\p{Hiragana}]{,3}", syno_tran): continue
          if syno_tran in hyper_tran_counts: count += 1
          if syno_tran in hypo_tran_counts: count += 1
          if syno_tran in similar_tran_counts: count += 1
          if syno_tran in derivative_tran_counts: count += 1
          if syno_tran in ext_aux_trans_set: count += 1
          if count >= 3 and self.IsValidPosTran(tokenizer, pos, syno_tran):
            adopted_syno_trans.add(syno_tran)
        for syno_tran in adopted_syno_trans:
          scored_item_trans[syno_tran] = max(0.8, scored_item_trans[syno_tran] + 0.25)
          num_voted_trans += 1
        if item_aux_trans:
          aux_scores = {}
          for syno_tran, count in syno_tran_counts.items():
            if count < math.ceil(len(synonyms) * 2 / 3): continue
            if len(syno_tran) < 2: continue
            if not regex.search(r"\p{Han}[\p{Han}\p{Hiragana}]", syno_tran): continue
            for aux_tran in item_aux_trans:
              if aux_tran.find(syno_tran) >= 0 and self.IsValidPosTran(tokenizer, pos, aux_tran):
                weight = 0.25 if aux_tran == syno_tran else 0.2
                aux_scores[aux_tran] = max(aux_scores.get(aux_tran) or 0.0, weight)
          for hyper_tran, count in hyper_tran_counts.items():
            if count < math.ceil(len(hypernyms) * 2 / 3): continue
            if len(hyper_tran) < 2: continue
            if not regex.search(r"\p{Han}[\p{Han}\p{Hiragana}]", hyper_tran): continue
            for aux_tran in item_aux_trans:
              if aux_tran.find(hyper_tran) >= 0 and self.IsValidPosTran(tokenizer, pos, aux_tran):
                weight = 0.25 if aux_tran == hyper_tran else 0.2
                aux_scores[aux_tran] = max(aux_scores.get(aux_tran) or 0.0, weight)
          for aux_tran, score in aux_scores.items():
            scored_item_trans[aux_tran] = scored_item_trans[aux_tran] + score
            num_borrowed_trans += 1
        item_score = 0.0
        if scored_item_trans:
          scored_item_trans = scored_item_trans.items()
          if bare:
            num_items_rescued += 1
          if rev_prob_dbm or tran_prob_dbm:
            sorted_item_trans, item_score, tran_scores = (self.SortWordsByScore(
              word, pos, scored_item_trans, mt_tran_set, hand_trans,
              rev_prob_dbm, tokenizer, tran_prob_dbm))
          else:
            scored_item_trans = sorted(scored_item_trans, key=lambda x: x[1], reverse=True)
            sorted_item_trans = [x[0] for x in scored_item_trans]
          final_item_trans = []
          uniq_item_trans = set()
          for tran in sorted_item_trans:
            tran = regex.sub(r"^を.*", "", tran)
            tran = regex.sub(r"・", "", tran)
            if len(tran) > 16: continue
            if not tran or tran in uniq_item_trans: continue
            uniq_item_trans.add(tran)
            final_item_trans.append(tran)
          item["translation"] = final_item_trans[:MAX_TRANSLATIONS_PER_WORD]
          if tran_scores:
            tran_score_map = {}
            for tran, tran_score in tran_scores[:MAX_TRANSLATIONS_PER_WORD]:
              tran = regex.sub(r"^を.*", "", tran)
              tran = regex.sub(r"・", "", tran)
              if tran and tran not in tran_score_map:
                tran_score_map[tran] = "{:.6f}".format(tran_score).replace("0.", ".")
            item["translation_score"] = tran_score_map
        item_score += spell_ratio * 0.5
        hint = hints.get(word) if hints else None
        if hint:
          for hi, hint_pos in enumerate(hint[0].split(",")):
            if pos == hint_pos:
              hint_weight = 2 ** (1.0 / ((hi + 1) * 2))
              item_score *= hint_weight
              break
        if word_extra_synonyms:
          base_syn_score = 1.0
          extra_syn_score = 0
          for extra_synonym in word_extra_synonyms:
            if extra_synonym in synonyms:
              extra_syn_score = max(extra_syn_score, base_syn_score)
            if extra_synonym in hypernyms:
              extra_syn_score = max(extra_syn_score, base_syn_score * 0.6)
            if extra_synonym in hyponyms:
              extra_syn_score = max(extra_syn_score, base_syn_score * 0.4)
            base_syn_score *= 0.95
          item_score += extra_syn_score
        if votes:
          vote_key = word + ":" + synset
          vote_score = votes.get(vote_key) or 0.0
          item_score += vote_score
        item["score"] = "{:.8f}".format(item_score).replace("0.", ".")
        if "link" in item:
          del item["link"]
      if rev_prob_dbm:
        entry["item"] = sorted(
          items, key=lambda item: float(item.get("score") or 0.0), reverse=True)
      serialized = json.dumps(entry, separators=(",", ":"), ensure_ascii=False)
      output_dbm.Set(key, serialized).OrDie()
      num_words += 1
      if num_words % 1000 == 0:
        logger.info("Saving words: words={}".format(num_words))
      it.Next()
    output_dbm.Close().OrDie()
    if tran_prob_dbm:
      tran_prob_dbm.Close().OrDie()
    if rev_prob_dbm:
      rev_prob_dbm.Close().OrDie()
    if phrase_prob_dbm:
      phrase_prob_dbm.Close().OrDie()
    input_dbm.Close().OrDie()
    logger.info(
      "Appending translations done: words={}, elapsed_time={:.2f}s".format(
        num_words, time.time() - start_time))
    logger.info(("Stats: orig={}, match={}, voted={}, borrowed={}" +
                 ", items={}, bare={}, rescued={}").format(
      num_orig_trans, num_match_trans, num_voted_trans, num_borrowed_trans,
      num_items, num_items_bare, num_items_rescued))

  def DeduplicateSynonyms(self, word, synonyms):
    result = []
    predecessors = [word.split(" ")]
    for synonym in synonyms:
      if regex.fullmatch(r"[A-Z]{2,}", synonym): continue
      tokens = synonym.split(" ")
      is_dup = False
      for predecessor in predecessors:
        if len(predecessor) == 1:
          if predecessor[0] in tokens:
            is_dup = True
        if len(tokens) == 1:
          if tokens[0] in predecessor:
            is_dup = True
        if is_dup: break
      if is_dup: continue
      result.append(synonym)
      if len(predecessors) < 5:
        predecessors.append(tokens)
    return result

  def AreSimilarWords(self, word_a, word_b):
    word_a = word_a.lower()
    word_b = word_b.lower()
    if word_a.startswith(word_b) or word_b.startswith(word_a):
      return True
    mono_a = regex.sub(r"[-_ ]", "", word_a)
    mono_b = regex.sub(r"[-_ ]", "", word_b)
    dist = tkrzw.Utility.EditDistanceLev(mono_a, mono_b)
    dist_ratio = dist / max(len(mono_a), len(mono_b))
    if dist_ratio <= 0.3:
      return True
    prefix_a = mono_a[:8]
    prefix_b = mono_b[:8]
    dist = tkrzw.Utility.EditDistanceLev(prefix_a, prefix_b)
    if dist <= 1:
      return True
    return False

  def NormalizeTranslationList(self, tokenizer, pos, trans):
    for i, tran in enumerate(trans):
      restored = tokenizer.NormalizeJaWordForPos(pos, tran)
      if restored != tran:
        trans[i] = restored

  def GetPhraseProb(self, prob_dbm, tokenizer, language, word):
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

  def GetTranProb(self, tran_prob_dbm, word, tran):
    max_prob = 0.0
    key = tkrzw_dict.NormalizeWord(word)
    tsv = tran_prob_dbm.GetStr(key)
    norm_tran = tran.lower()
    if tsv:
      fields = tsv.split("\t")
      for i in range(0, len(fields), 3):
        src, trg, prob = fields[i], fields[i + 1], fields[i + 2]
        if src == word and trg.lower() == norm_tran:
          prob = float(prob)
          max_prob = max(max_prob, prob)
    return max_prob

  def NormalizeTran(self, tokenizer, text):
    parts = tokenizer.StripJaParticles(text)
    if parts[0]:
      text = parts[0]
    pos = tokenizer.GetJaLastPos(text)
    if text.endswith(pos[0]) and pos[3]:
      text = text[:-len(pos[0])] + pos[3]
    return text

  def SortRelatedWords(self, rel_words, seed_tran_probs,
                       tokenizer, phrase_prob_dbm, tran_prob_dbm, synset_index, tran_index,
                       word_extra_synonyms):
    word_scores = []
    for rel_word in rel_words:
      prob_score = 0
      if phrase_prob_dbm:
        prob = self.GetPhraseProb(phrase_prob_dbm, tokenizer, "en", rel_word)
        prob_score = 8 / (abs(math.log(prob) - math.log(0.001)) + 8)
      tran_score = 0
      if seed_tran_probs:
        rel_tran_probs = tran_index.get(rel_word)
        if rel_tran_probs:
          for seed_tran, seed_prob in seed_tran_probs.items():
            norm_seed_tran = self.NormalizeTran(tokenizer, seed_tran)
            seed_prob **= 0.5
            for rel_tran, rel_prob in rel_tran_probs.items():
              norm_rel_tran = self.NormalizeTran(tokenizer, rel_tran)
              rel_prob **= 0.5
              if rel_tran == seed_tran:
                tran_score = max(tran_score, seed_prob * rel_prob)
              elif norm_seed_tran == norm_rel_tran:
                tran_score = max(tran_score, seed_prob * rel_prob * 0.5)
      extra_score = 0
      if word_extra_synonyms:
        base_extra_score = 1.0
        for extra_synonym in word_extra_synonyms:
          if rel_word == extra_synonym:
            extra_score = base_extra_score
            break
          base_extra_score *= 0.95
      rel_syn_num = max(len(synset_index.get(rel_word) or []), 1)
      uniq_score = 4 / (math.log(rel_syn_num) + 4)
      score = prob_score + tran_score + extra_score + uniq_score
      word_scores.append((rel_word, score))
    word_scores = sorted(word_scores, key=lambda x: x[1], reverse=True)
    return [x[0] for x in word_scores]

  _regex_stop_word_katakana = regex.compile(r"[\p{Katakana}ー]+")
  _regex_stop_word_hiragana = regex.compile(r"[\p{Hiragana}ー]+")
  def SortWordsByScore(
      self, word, pos, input_trans, mt_tran_set, hand_trans,
      rev_prob_dbm, tokenizer, tran_prob_dbm):
    norm_word = word.lower()
    scored_trans = []
    pure_translation_scores = []
    max_score = 0.0
    sum_score = 0.0
    for tran, score in input_trans:
      norm_tran = tran.lower()
      if norm_tran == norm_word:
        tran = word
      tran_bias = score
      if self._regex_stop_word_katakana.search(tran):
        tran_bias *= 0.8
        if self._regex_stop_word_katakana.fullmatch(tran):
          tran_bias *= 0.8
        if pos != "noun":
          tran_bias *= 0.8
      elif self._regex_stop_word_hiragana.fullmatch(tran):
        tran_bias *= 0.9
      elif self.IsValidPosTran(tokenizer, pos, tran):
        tran_bias *= 1.2
      prob_score = 0.0
      if rev_prob_dbm:
        prob_score = self.GetPhraseProb(rev_prob_dbm, tokenizer, "ja", tran)
        if tokenizer.IsJaWordSahenVerb(tran):
          stem = regex.sub(r"する$", "", tran)
          stem_prob_score = self.GetPhraseProb(rev_prob_dbm, tokenizer, "ja", stem)
          prob_score = max(prob_score, stem_prob_score)
        stem = tokenizer.CutJaWordNounThing(tran)
        stem = tokenizer.CutJaWordNounParticle(tran)
        if stem != tran:
          stem_prob_score = self.GetPhraseProb(rev_prob_dbm, tokenizer, "ja", stem)
          prob_score = max(prob_score, stem_prob_score * 0.9)
        prob_score = max(prob_score, 0.0000001)
        prob_score = math.exp(-abs(math.log(0.001) - math.log(prob_score))) * 0.1
        if self._regex_stop_word_hiragana.fullmatch(tran):
          prob_score *= 0.5
        elif len(tran) <= 1:
          prob_score *= 0.5
      tran_score = 0.0
      if tran_prob_dbm:
        tran_score = self.GetTranProb(tran_prob_dbm, word, tran) * tran_bias
        if tran_score:
          pure_translation_scores.append((tran, tran_score))
      if tran in mt_tran_set or tran in hand_trans:
        tran_score += 0.1 * tran_bias
      score = prob_score + tran_score
      scored_trans.append((tran, score))
      max_score = max(max_score, score)
      sum_score += score
    scored_trans = sorted(scored_trans, key=operator.itemgetter(1), reverse=True)
    score_bias = 1000 / (1000 + min(10, len(input_trans) - 1))
    pure_translation_scores = sorted(
      pure_translation_scores, key=operator.itemgetter(1), reverse=True)
    mean_score = (max_score * sum_score) ** 0.5 + 0.00001
    uniq_scores = set()
    dedup_scores = []
    for tran, score in scored_trans:
      norm_tran = tran.lower()
      if norm_tran in uniq_scores: continue
      dedup_scores.append(tran)
      uniq_scores.add(norm_tran)
    return (dedup_scores, mean_score ** score_bias, pure_translation_scores)

  def IsValidPosTran(self, tokenizer, pos, tran):
    tran_surface, tran_pos, tran_subpos, tran_lemma = tokenizer.GetJaLastPos(tran)
    if len(tran) <= 1 and tran != tran_lemma:
      return False
    if pos == "noun":
      if tran_pos == "名詞":
        return True
    if pos == "verb":
      if tran_pos == "動詞":
        return True
    if pos == "adjective":
      if tran_pos == "形容詞":
        return True
      if tran_pos in ("助詞", "助動詞") and tran_surface in ("な", "の", "た"):
        return True
    if pos == "adverb":
      if tran_pos == "副詞":
        return True
      if tran_pos in ("助詞", "助動詞") and tran_surface == "に":
        return True
      if tran_pos == "形容詞" and tran_surface != tran_lemma and tran_surface.endswith("く"):
        return True
      if tran_pos in "助詞" and (tran_subpos == "副詞化" or tran_surface == "として"):
        return True
      if tran_pos in "名詞" and tran_subpos == "副詞可能":
        return True
    return False


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "wordnet.thk"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "wordnet-tran.tkh"
  wnjpn_path = tkrzw_dict.GetCommandFlag(args, "--wnjpn", 1) or "wnjpn-ok.tab"
  vote_path = tkrzw_dict.GetCommandFlag(args, "--vote", 1) or ""
  wnmt_paths = tkrzw_dict.GetCommandFlag(args, "--wnmt", 1) or ""
  feedback_path = tkrzw_dict.GetCommandFlag(args, "--feedback", 1) or ""
  phrase_prob_path = tkrzw_dict.GetCommandFlag(args, "--phrase_prob", 1) or ""
  rev_prob_path = tkrzw_dict.GetCommandFlag(args, "--rev_prob", 1) or ""
  tran_prob_path = tkrzw_dict.GetCommandFlag(args, "--tran_prob", 1) or ""
  nmt_prob_path = tkrzw_dict.GetCommandFlag(args, "--nmt_prob", 1) or ""
  tran_aux_paths = (tkrzw_dict.GetCommandFlag(args, "--tran_aux", 1) or "").split(",")
  tran_subaux_paths = (tkrzw_dict.GetCommandFlag(args, "--tran_subaux", 1) or "").split(",")
  tran_thes_path = tkrzw_dict.GetCommandFlag(args, "--tran_thes", 1) or ""
  hint_path = tkrzw_dict.GetCommandFlag(args, "--hint", 1) or ""
  synonym_path = tkrzw_dict.GetCommandFlag(args, "--synonym", 1) or ""
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  AppendWordnetJPNBatch(
    input_path, output_path, wnjpn_path, vote_path, wnmt_paths, feedback_path,
    phrase_prob_path, rev_prob_path, tran_prob_path, nmt_prob_path,
    tran_aux_paths, tran_subaux_paths, tran_thes_path, hint_path, synonym_path).Run()


if __name__=="__main__":
  main()
