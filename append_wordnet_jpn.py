#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to append WordNet Japanese translation to the WordNet database
#
# Usage:
#   append_wordnet_jpn.py [--input str] [--output str] [--wnjpn str]
#     [--phrase_prob str] [--rev_prob str] [--tran_prob str]
#     [--tran_aux str] [--tran_subaux str] [--quiet]
#
# Example:
#   ./append_wordnet_jpn.py --input wordnet.tkh --output wordnet-body.tkh \
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
  def __init__(self, input_path, output_path, wnjpn_path, feedback_path,
               phrase_prob_path, rev_prob_path, tran_prob_path,
               tran_aux_paths, tran_subaux_paths):
    self.input_path = input_path
    self.output_path = output_path
    self.wnjpn_path = wnjpn_path
    self.feedback_path = feedback_path
    self.phrase_prob_path = phrase_prob_path
    self.rev_prob_path = rev_prob_path
    self.tran_prob_path = tran_prob_path
    self.tran_aux_paths = tran_aux_paths
    self.tran_subaux_paths = tran_subaux_paths

  def Run(self):
    tokenizer = tkrzw_tokenizer.Tokenizer()
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}, wnjpn_path={}".format(
                  self.input_path, self.output_path, self.wnjpn_path))
    wnjpn_trans = self.ReadTranslations()
    if self.feedback_path:
      feedback_trans = self.ReadFeedbackTranslations()
    else:
      feedback_trans = None
    aux_trans, subaux_trans = self.ReadAuxTranslations()
    synset_index = self.ReadSynsetIndex()
    self.AppendTranslations(wnjpn_trans, feedback_trans, aux_trans, subaux_trans, synset_index)
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
    return aux_trans, subaux_trans

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
        logger.info("Reading words: words={}".format(num_words))
      it.Next()
    logger.info("Reading synset index done: records={}".format(len(synset_index)))
    return synset_index

  def AppendTranslations(self, wnjpn_trans, feedback_trans,
                         aux_trans, subaux_trans, synset_index):
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
      for item in items:
        word = item["word"]
        pos = item["pos"]
        synset = item["synset"]
        links = item.get("link") or {}
        phrase_prob = float(item.get("prob") or 0.0)
        spell_ratio = spell_ratios[word]
        synonyms = item.get("synonym") or []
        hypernyms = item.get("hypernym") or []
        hyponyms = item.get("hyponym") or []
        similars = item.get("similar") or []
        derivatives = item.get("derivative") or []
        synonym_ids = links.get("synonym") or []
        hypernym_ids = links.get("hypernym") or []
        hyponym_ids = links.get("hyponym") or []
        similar_ids = links.get("similar") or []
        derivative_ids = links.get("derivative") or []
        item_tran_pairs = wnjpn_trans.get(synset) or []
        item_aux_trans = list(aux_trans.get(word) or [])
        ext_item_aux_trans = list(item_aux_trans)
        ext_item_aux_trans.extend(subaux_trans.get(word) or [])
        self.NormalizeTranslationList(tokenizer, pos, item_aux_trans)
        self.NormalizeTranslationList(tokenizer, pos, ext_item_aux_trans)
        scored_item_trans = []
        hand_trans = set()
        for tran, src in item_tran_pairs:
          if src == "mono":
            hit = False
            for item_aux_tran in ext_item_aux_trans:
              dist = tkrzw.Utility.EditDistanceLev(tran, item_aux_tran)
              dist_ratio = dist / max(len(tran), len(item_aux_tran))
              if dist < 0.3:
                hit = True
            if not hit:
              continue
          tran = tokenizer.NormalizeJaWordForPos(pos, tran)
          scored_item_trans.append((tran, 1.0))
          if src == "hand":
            hand_trans.add(tran)
        if feedback_trans:
          item_fb_trans = feedback_trans.get(word + ":" + synset) or []
          if item_fb_trans:
            for tran in item_fb_trans:
              tran = tokenizer.NormalizeJaWordForPos(pos, tran)
              scored_item_trans.append((tran, 0.9))
        uniq_item_trans = set()
        for tran, score in scored_item_trans:
          uniq_item_trans.add(tran)
        num_items += 1
        bare = not scored_item_trans
        if bare:
          num_items_bare += 1
        num_orig_trans += len(uniq_item_trans)
        syno_tran_counts = collections.defaultdict(int)
        hyper_tran_counts = collections.defaultdict(int)
        hypo_tran_counts = collections.defaultdict(int)
        similar_tran_counts = collections.defaultdict(int)
        derivative_tran_counts = collections.defaultdict(int)
        aux_trans_set = set(ext_item_aux_trans)
        checked_words = set()
        checked_ids = set([synset])
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
                    valid_pos = self.IsValidPosTran(tokenizer, pos, item_aux_tran)
                    if valid_pos and item_aux_tran not in uniq_item_trans:
                      scored_item_trans.append((item_aux_tran, 0.9))
                      uniq_item_trans.add(item_aux_tran)
                      num_match_trans += 1
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
        if bare:
          for deri_tran, count in derivative_tran_counts.items():
            syno_tran_counts[deri_tran] = syno_tran_counts[deri_tran] + count
          derivative_tran_counts.clear()
        for syno_tran, count in syno_tran_counts.items():
          if regex.fullmatch(r"[\p{Hiragana}]{,3}", syno_tran): continue
          if syno_tran in hyper_tran_counts: count += 1
          if syno_tran in hypo_tran_counts: count += 1
          if syno_tran in similar_tran_counts: count += 1
          if syno_tran in derivative_tran_counts: count += 1
          if syno_tran in aux_trans_set: count += 1
          if count >= 3 and syno_tran not in uniq_item_trans:
            valid_pos = self.IsValidPosTran(tokenizer, pos, syno_tran)
            if valid_pos:
              scored_item_trans.append((syno_tran, 0.9))
              num_voted_trans += 1
        if item_aux_trans:
          for syno_tran, count in syno_tran_counts.items():
            if count < 2 and count < len(synonyms): continue
            if len(syno_tran) < 2: continue
            if not regex.search(r"\p{Han}[\p{Han}\p{Hiragana}]", syno_tran): continue
            for aux_tran in item_aux_trans:
              if aux_tran.find(syno_tran) >= 0 and aux_tran not in uniq_item_trans:
                scored_item_trans.append((aux_tran, 0.2))
                uniq_item_trans.add(aux_tran)
          for hyper_tran, count in hyper_tran_counts.items():
            if count < 2 and count < len(hypernyms): continue
            if len(hyper_tran) < 2: continue
            if not regex.search(r"\p{Han}[\p{Han}\p{Hiragana}]", hyper_tran): continue
            for aux_tran in item_aux_trans:
              if aux_tran.find(hyper_tran) >= 0 and aux_tran not in uniq_item_trans:
                scored_item_trans.append((aux_tran, 0.2))
                uniq_item_trans.add(aux_tran)
        item_score = 0.0
        if scored_item_trans:
          if bare:
            num_items_rescued += 1
          if rev_prob_dbm or tran_prob_dbm:
            final_item_trans, item_score, tran_scores = (self.SortWordsByScore(
              word, pos, scored_item_trans, hand_trans, rev_prob_dbm, tokenizer, tran_prob_dbm))
          else:
            scored_item_trans = sorted(scored_item_trans, key=lambda x: x[1], reverse=True)
            final_item_trans = [x[0] for x in scored_item_trans]
          item["translation"] = final_item_trans[:MAX_TRANSLATIONS_PER_WORD]
          if tran_scores:
            tran_score_map = {}
            for tran, tran_score in tran_scores[:MAX_TRANSLATIONS_PER_WORD]:
              tran_score_map[tran] = "{:.6f}".format(tran_score).replace("0.", ".")
            item["translation_score"] = tran_score_map
        item_score += spell_ratio * 0.5
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
      "Aappending translations done: words={}, elapsed_time={:.2f}s".format(
        num_words, time.time() - start_time))
    logger.info("Stats: orig={}, match={}, voted={}, items={}, bare={}, rescued={}".format(
      num_orig_trans, num_match_trans, num_voted_trans,
      num_items, num_items_bare, num_items_rescued))

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

  _regex_stop_word_katakana = regex.compile(r"[\p{Katakana}ー]+")
  _regex_stop_word_hiragana = regex.compile(r"[\p{Hiragana}ー]+")
  def SortWordsByScore(
      self, word, pos, input_trans, hand_trans, rev_prob_dbm, tokenizer, tran_prob_dbm):
    scored_trans = []
    pure_translation_scores = []
    max_score = 0.0
    sum_score = 0.0
    for tran, score in input_trans:
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
      if tran in hand_trans:
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
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "wordnet-body.tkh"
  wnjpn_path = tkrzw_dict.GetCommandFlag(args, "--wnjpn", 1) or "wnjpn-ok.tab"
  feedback_path = tkrzw_dict.GetCommandFlag(args, "--feedback", 1) or ""
  phrase_prob_path = tkrzw_dict.GetCommandFlag(args, "--phrase_prob", 1) or ""
  rev_prob_path = tkrzw_dict.GetCommandFlag(args, "--rev_prob", 1) or ""
  tran_prob_path = tkrzw_dict.GetCommandFlag(args, "--tran_prob", 1) or ""
  tran_aux_paths = (tkrzw_dict.GetCommandFlag(args, "--tran_aux", 1) or "").split(",")
  tran_subaux_paths = (tkrzw_dict.GetCommandFlag(args, "--tran_subaux", 1) or "").split(",")
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  AppendWordnetJPNBatch(
    input_path, output_path, wnjpn_path, feedback_path,
    phrase_prob_path, rev_prob_path, tran_prob_path, tran_aux_paths, tran_subaux_paths).Run()


if __name__=="__main__":
  main()
