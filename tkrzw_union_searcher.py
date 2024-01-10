#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Dictionary searcher of union database
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
import html
import json
import math
import operator
import regex
import tkrzw
import tkrzw_dict


class UnionSearcher:
  def __init__(self, data_prefix):
    body_path = data_prefix + "-body.tkh"
    self.body_dbm = tkrzw.DBM()
    self.body_dbm.Open(body_path, False, dbm="HashDBM").OrDie()
    tran_index_path = data_prefix + "-tran-index.tkh"
    self.tran_index_dbm = tkrzw.DBM()
    self.tran_index_dbm.Open(tran_index_path, False, dbm="HashDBM").OrDie()
    infl_index_path = data_prefix + "-infl-index.tkh"
    self.infl_index_dbm = tkrzw.DBM()
    self.infl_index_dbm.Open(infl_index_path, False, dbm="HashDBM").OrDie()
    self.keys_path = data_prefix + "-keys.txt"
    self.keys_file = None
    self.tran_keys_path = data_prefix + "-tran-keys.txt"
    self.tran_keys_file = None
    self.examples_path = data_prefix + "-examples.tsv"
    self.examples_file = None

  def __del__(self):
    if self.examples_file:
      self.examples_file.Close().OrDie()
    if self.tran_keys_file:
      self.tran_keys_file.Close().OrDie()
    if self.keys_file:
      self.keys_file.Close().OrDie()
    self.infl_index_dbm.Close().OrDie()
    self.tran_index_dbm.Close().OrDie()
    self.body_dbm.Close().OrDie()

  def OpenKeysFile(self):
    self.keys_file = tkrzw.File()
    self.keys_file.Open(self.keys_path, False).OrDie()

  def OpenTranKeysFile(self):
    self.tran_keys_file = tkrzw.File()
    self.tran_keys_file.Open(self.tran_keys_path, False).OrDie()

  def OpenExamplesFile(self):
    self.examples_file = tkrzw.File()
    self.examples_file.Open(self.examples_path, False).OrDie()

  def SearchBody(self, text):
    serialized = self.body_dbm.GetStr(text)
    if not serialized:
      return None
    return json.loads(serialized)

  def SearchTranIndex(self, text):
    text = tkrzw_dict.NormalizeWord(text)
    tsv = self.tran_index_dbm.GetStr(text)
    result = []
    if tsv:
      result.extend(tsv.split("\t"))
    return result

  def GetResultKeys(self, entries):
    keys = set()
    for entry in entries:
      keys.add(tkrzw_dict.NormalizeWord(entry["word"]))
    return keys

  def SearchInflections(self, text):
    result = []
    result.append(tkrzw_dict.NormalizeWord(text))
    tsv = self.infl_index_dbm.GetStr(text)
    if tsv:
      for value in tsv.split("\t"):
        if value not in result:
          result.append(value)
    return result

  def SearchInflectionsReverse(self, text):
    result = self.SearchTranIndex(text)
    key = " " + tkrzw_dict.NormalizeWord(text)
    tsv = self.tran_index_dbm.GetStr(key)
    if tsv:
      result.extend(tsv.split("\t"))
    return result

  def CheckExact(self, text):
    for word in text.split(","):
      word = tkrzw_dict.NormalizeWord(word)
      if not word: continue
      if word in self.body_dbm:
        return True
    return False

  def SearchExact(self, text, capacity):
    result = []
    uniq_words = set()
    for word in text.split(","):
      if len(result) >= capacity: break
      word = tkrzw_dict.NormalizeWord(word)
      if not word: continue
      entries = self.SearchBody(word)
      if not entries: continue
      for entry in entries:
        if len(result) >= capacity: break
        word = entry["word"]
        if word in uniq_words: continue
        uniq_words.add(word)
        result.append(entry)
    return result

  def SearchExactReverse(self, text, capacity):
    ja_words = []
    ja_uniq_words = set()
    for ja_word in text.split(","):
      ja_word = tkrzw_dict.NormalizeWord(ja_word)
      if not ja_word: continue
      if ja_word in ja_uniq_words: continue
      ja_uniq_words.add(ja_word)
      ja_words.append(ja_word)
    en_words = []
    en_uniq_words = set()
    for ja_word in ja_words:
      for en_word in self.SearchTranIndex(ja_word):
        if en_word in en_uniq_words: continue
        en_uniq_words.add(en_word)
        en_words.append(en_word)
    result = []
    uniq_words = set()
    for en_word in en_words:
      if capacity < 1: break
      norm_en_word = tkrzw_dict.NormalizeWord(en_word)
      entries = self.SearchBody(norm_en_word)
      if entries:
        for entry in entries:
          if capacity < 1: break
          word = entry["word"]
          if word != en_word: continue
          if word in uniq_words: continue
          uniq_words.add(word)
          match = False
          translations = entry.get("translation")
          if translations:
            for tran in translations:
              tran = tkrzw_dict.NormalizeWord(tran)
              for ja_word in ja_words:
                if tran.find(ja_word) >= 0:
                  match = True
                  break
              if match: break
          if not match:
            phrases = entry.get("phrase")
            if phrases:
              for phrase in phrases:
                for tran in phrase["x"]:
                  tran = tkrzw_dict.NormalizeWord(tran)
                  for ja_word in ja_words:
                    if tran.find(ja_word) >= 0:
                      match = True
                      break
          if match:
            result.append(entry)
            capacity -= 1
    return result

  def ExpandEntries(self, seed_entries, seed_features, capacity):
    result = []
    seeds = []
    num_steps = 0
    def AddSeed(entry):
      nonlocal num_steps
      features = self.GetFeatures(entry)
      score = self.GetSimilarity(seed_features, features)
      heapq.heappush(seeds, (-score, num_steps, entry))
      num_steps += 1
    checked_words = set()
    checked_trans = set()
    for entry in seed_entries:
      word = entry["word"]
      if word in checked_words: continue
      checked_words.add(word)
      AddSeed(entry)
    while seeds:
      score, cur_steps, entry = heapq.heappop(seeds)
      score *= -1
      result.append(entry)
      num_appends = 0
      max_rel_words = 16 / math.log2(len(result) + 1) * score
      max_trans = 8 / math.log2(len(result) + 1) * score
      max_rel_words = max(int(max_rel_words), 4)
      max_trans = max(int(max_trans), 2)
      rel_words = []
      for i, rel_name in enumerate(("related", "rephrase", "parent", "child")):
        tmp_rel_words = entry.get(rel_name)
        if tmp_rel_words:
          for j, rel_word in enumerate(tmp_rel_words):
            rel_words.append((rel_word, i + j))
      if rel_words:
        rel_words = sorted(rel_words, key=lambda x: x[1])
        rel_words = [x[0] for x in rel_words]
        num_checks = 0
        for rel_word in rel_words:
          if num_checks >= max_rel_words: break
          if len(checked_words) >= capacity: break
          if rel_word in checked_words: continue
          children = self.SearchExact(rel_word, capacity - len(checked_words))
          if not children: continue
          num_checks += 1
          for child in children:
            if len(checked_words) >= capacity: break
            word = child["word"]
            if word in checked_words: continue
            checked_words.add(word)
            AddSeed(child)
            num_appends += 1
      trans = entry.get("translation")
      if trans:
        for tran in trans[:max_trans]:
          if len(checked_words) >= capacity: break
          tran = regex.sub(
            r"([\p{Han}\p{Katakana}ー]{2,})(する|すること|される|されること|をする)$",
            r"\1", tran)
          tran = regex.sub(
            r"([\p{Han}\p{Katakana}ー]{2,})(的|的な|的に)$",
            r"\1", tran)
          if tran in checked_trans: continue
          checked_trans.add(tran)
          max_children = min(capacity - len(checked_words), 10)
          num_tran_adopts = 0
          for child in self.SearchExactReverse(tran, max_children):
            if len(checked_words) >= capacity: break
            if num_tran_adopts >= 5: break
            word = child["word"]
            if word in checked_words: continue
            checked_words.add(word)
            AddSeed(child)
            num_tran_adopts += 1
            num_appends += 1
      coocs = entry.get("cooccurrence")
      if coocs:
        for cooc in coocs:
          if num_appends >= 8: break
          if len(checked_words) >= capacity: break
          if cooc in checked_words: continue
          for child in self.SearchExact(cooc, capacity - len(checked_words)):
            if len(checked_words) >= capacity: break
            word = child["word"]
            if word in checked_words: continue
            checked_words.add(word)
            AddSeed(child)
            num_appends += 1
    return result

  def GetFeatures(self, entry):
    SCORE_DECAY = 0.95
    word = tkrzw_dict.NormalizeWord(entry["word"])
    features = {word: 1.0}
    pos_score = 1.0
    pos_score_max = 0.0
    pos_features = collections.defaultdict(float)
    for item in entry["item"]:
      pos = "__" + item["pos"]
      new_score = (pos_features.get(pos) or 0.0) + pos_score
      pos_features[pos] = new_score
      pos_score_max = max(pos_score_max, new_score)
      pos_score *= SCORE_DECAY
    for pos, pos_feature_score in pos_features.items():
      features[pos] = pos_feature_score / pos_score_max
    score = 1.0
    rel_words = entry.get("related")
    if rel_words:
      for rel_word in rel_words[:16]:
        rel_word = tkrzw_dict.NormalizeWord(rel_word)
        if rel_word not in features:
          score *= SCORE_DECAY
          features[rel_word] = score
    rephrases = entry.get("rephrase")
    if rephrases:
      for rephrase in rephrases[:4]:
        rephrase = tkrzw_dict.NormalizeWord(rephrase)
        if rephrase not in features:
          score *= SCORE_DECAY
          features[rephrase] = score
    score = max(score, 0.4)
    trans = entry.get("translation")
    if trans:
      for tran in trans[:20]:
        tran = tkrzw_dict.NormalizeWord(tran)
        tran = regex.sub(
          r"([\p{Han}\p{Katakana}ー]{2,})(する|すること|される|されること|をする|な|に|さ)$",
          r"\1", tran)
        if tran not in features:
          score *= SCORE_DECAY
          features[tran] = score
    score = max(score, 0.2)
    coocs = entry.get("cooccurrence")
    if coocs:
      for cooc in coocs[:20]:
        cooc = tkrzw_dict.NormalizeWord(cooc)
        if cooc not in features:
          score *= SCORE_DECAY
          features[cooc] = score
    return features

  def GetSimilarity(self, seed_features, cand_features):
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

  def SearchRelatedWithSeeds(self, seeds, capacity):
    seed_features = collections.defaultdict(float)
    base_weight = 1.0
    uniq_words = set()
    for seed in seeds:
      norm_word = tkrzw_dict.NormalizeWord(seed["word"])
      weight = base_weight
      if norm_word in uniq_words:
        weight *= 0.1
      uniq_words.add(norm_word)
      for word, score in self.GetFeatures(seed).items():
        seed_features[word] += score * weight
      base_weight *= 0.8
    result = self.ExpandEntries(seeds, seed_features, max(int(capacity * 1.2), 100))
    return result[:capacity]

  def SearchRelated(self, text, capacity):
    seeds = []
    words = text.split(",")
    for word in words:
      if word:
        seeds.extend(self.SearchExact(word, capacity))
    return self.SearchRelatedWithSeeds(seeds, capacity)

  def SearchRelatedReverse(self, text, capacity):
    seeds = []
    words = text.split(",")
    for word in words:
      if word:
        seeds.extend(self.SearchExactReverse(word, capacity))
    return self.SearchRelatedWithSeeds(seeds, capacity)

  _particles = {
    "aback", "about", "above", "abroad", "across", "after", "against", "ahead", "along",
    "amid", "among", "apart", "around", "as", "at", "away", "back", "before", "behind",
    "below", "beneath", "between", "beside", "beyond", "by", "despite", "during", "down",
    "except", "for", "forth", "from", "in", "inside", "into", "near", "of", "off", "on",
    "onto", "out", "outside", "over", "per", "re", "since", "than", "through", "throughout",
    "till", "to", "together", "toward", "under", "until", "up", "upon", "with", "within",
    "without", "via",
  }
  _possessives = {
    "my", "our", "your", "his", "her", "its", "their",
  }
  _reflexives = {
    "myself", "ourselves", "yourself", "yourselves", "himself", "herself", "itself", "theirselves",
  }
  _object_pronouns = {
    "me", "us", "you", "him", "her", "it", "them",
  }
  _determiners = {
    "a", "an", "the", "my", "our", "your", "his", "her", "its", "their", "this", "these", "that", "those",
  }
  def SearchSetPhrases(self, text, capacity):
    text = tkrzw_dict.NormalizeWord(text)
    tokens = text.split(" ")
    result = []
    if len(tokens) >= 3 and len(tokens) <= 5 and tokens[-1] in self._particles:
      num_tokens = len(tokens) - 2
      while num_tokens >= 1:
        phrase = " ".join(tokens[0:num_tokens]) + " " + tokens[-1]
        for entry in self.SearchExact(phrase, capacity - len(result)):
          result.append(entry)
        num_tokens -= 1
    if len(tokens) >= 2 and len(tokens) <= 5:
      for i, token in enumerate(tokens):
        if token in self._possessives or regex.search(r"^[A-Za-z]+'s$", token):
          phrase = " ".join(tokens[:i] + ["one's"] + tokens[i + 1:])
          for entry in self.SearchExact(phrase, capacity - len(result)):
            result.append(entry)
      for i, token in enumerate(tokens):
        if token in self._reflexives:
          phrase = " ".join(tokens[:i] + ["oneself"] + tokens[i + 1:])
          for entry in self.SearchExact(phrase, capacity - len(result)):
            result.append(entry)
      for i, token in enumerate(tokens):
        if token in self._object_pronouns or token in self._reflexives:
          for wild in ["someone", "something"]:
            phrase = " ".join(tokens[:i] + [wild] + tokens[i + 1:])
            for entry in self.SearchExact(phrase, capacity - len(result)):
              result.append(entry)
    if len(tokens) >= 4 and tokens[1] in self._determiners:
      for wild in ["someone", "something"]:
        phrase = " ".join(tokens[:1] + [wild] + tokens[3:])
        for entry in self.SearchExact(phrase, capacity - len(result)):
          result.append(entry)
    uniq_result = []
    uniq_words = set()
    for entry in result:
      word = entry["word"]
      if word in uniq_words: continue
      uniq_words.add(word)
      uniq_result.append(entry)
    return uniq_result

  def SearchPartial(self, text, capacity):
    text = tkrzw_dict.NormalizeWord(text)
    tokens = text.split(" ")
    result = []
    if len(tokens) >= 2 and len(tokens) <= 6:
      num_tokens = len(tokens) - 1
      while num_tokens > 0 and num_tokens >= len(tokens) - 2:
        i = 0
        while i + num_tokens < len(tokens):
          phrase = " ".join(tokens[i:i + num_tokens])
          for entry in self.SearchExact(phrase, capacity - len(result)):
            result.append(entry)
          i += 1
        num_tokens -= 1;
    return result

  def SearchWithContext(self, core_phrase, prefix, suffix, capacity):
    result = []
    prefix_tokens = regex.sub(r"\s+", " ", prefix).split(" ")[-3:]
    suffix_tokens = regex.sub(r"\s+", " ", suffix).split(" ")[:3]
    lemmas = [core_phrase]
    uniq_lemmas = {core_phrase}
    for lemma in self.SearchInflections(core_phrase):
      if lemma in uniq_lemmas: continue
      uniq_lemmas.add(lemma)
      lemmas.append(lemma)
    for core_lemma in lemmas:
      prefix_length = 0
      while prefix_length <= len(prefix_tokens):
        prefix_context = core_lemma
        if prefix_length > 0:
          prefix_context = " ".join(prefix_tokens[-prefix_length:]) + " " + prefix_context
        suffix_length = 0
        while suffix_length <= len(suffix_tokens):
          context_query = prefix_context
          if suffix_length > 0:
            context_query += " " + " ".join(suffix_tokens[:suffix_length])
          if len(result) < capacity:
            for entry in self.SearchExact(context_query, capacity - len(result)):
              result.append(entry)
          if len(result) < capacity:
            for entry in self.SearchSetPhrases(context_query, capacity - len(result)):
              result.append(entry)
          suffix_length += 1
        prefix_length += 1
    uniq_result = []
    uniq_words = set()
    for entry in result:
      word = entry["word"]
      if word in uniq_words: continue
      uniq_words.add(word)
      uniq_result.append(entry)
    return uniq_result

  def SearchPatternMatch(self, mode, text, capacity):
    self.OpenKeysFile()
    text = tkrzw_dict.NormalizeWord(text)
    keys = self.keys_file.Search(mode, text, capacity)
    result = []
    for key in keys:
      if len(result) >= capacity: break
      for entry in self.SearchExact(key, capacity - len(result)):
        result.append(entry)
    return result

  def SearchPatternMatchReverse(self, mode, text, capacity):
    self.OpenTranKeysFile()
    text = tkrzw_dict.NormalizeWord(text)
    keys = self.tran_keys_file.Search(mode, text, capacity)
    result = []
    uniq_words = set()
    for key in keys:
      if len(result) >= capacity: break
      for entry in self.SearchExactReverse(key, capacity - len(result) + 10):
        if len(result) >= capacity: break
        word = entry["word"]
        if word in uniq_words: continue
        uniq_words.add(word)
        result.append(entry)
    return result

  def SearchByGrade(self, capacity, page, first_only):
    self.OpenKeysFile()
    keys = self.keys_file.Search("begin", "", capacity * page)
    if page > 1:
      skip = capacity * (page - 1)
      keys = keys[skip:]
    result = []
    for key in keys:
      if len(result) >= capacity: break
      for entry in self.SearchExact(key, capacity - len(result)):
        result.append(entry)
        if first_only:
          break
    return result

  _infl_names = ["noun_plural", "verb_singular", "verb_present_participle",
                 "verb_past", "verb_past_participle",
                 "adjective_comparative", "adjective_superlative",
                 "adverb_comparative", "adverb_superlative"]
  def SearchExample(self, text, search_mode, capacity):
    self.OpenExamplesFile()
    result = []
    surfaces = {}
    if tkrzw_dict.PredictLanguage(text) == "en":
      if search_mode == "exact":
        mode = "containcaseword"
        query = text
      elif search_mode == "prefix":
        mode = "regex"
        query = r"(?i)(^|\W){}".format(regex.escape(text))
      elif search_mode == "suffix":
        mode = "regex"
        query = r"(?i){}(\W|$)".format(regex.escape(text))
      elif search_mode == "contain":
        mode = "containcase"
        query = text
      else:
        infls = {text.lower()}
        entries = self.SearchExact(text, 4)
        if entries:
          for entry in entries:
            word = entry["word"]
            infls.add(word.lower())
            surfaces[word] = True
            for infl_name in self._infl_names:
              value = entry.get(infl_name)
              if value:
                for infl in value:
                  infls.add(infl.lower())
                  surfaces[infl] = True
        if len(infls) > 1:
          mode = "containcaseword*"
          query = "\n".join(infls)
        else:
          mode = "containcaseword"
          query = text
    elif regex.search("[a-zA-Z]", text):
      mode = "containcase"
      query = text
    else:
      mode = "contain"
      query = text
    lines = self.examples_file.Search(mode, query, capacity)
    if lines:
      entry = {"word": text, "probability": ".0", "item": []}
      if surfaces:
        entry["surface"] = surfaces.keys()
      examples = []
      for line in lines:
        fields = line.split("\t")
        if len(fields) != 2: continue
        source, target = fields
        example = {"e": source, "j": target}
        examples.append(example)
      entry["example"] = examples
      result.append(entry)
    return result

  infl_names = (
    "noun_plural", "verb_singular", "verb_present_participle",
    "verb_past", "verb_past_participle",
    "adjective_comparative", "adjective_superlative",
    "adverb_comparative", "adverb_superlative")
  re_latin_word = regex.compile(r"[\p{Latin}\d][-_'’\p{Latin}\d]*")
  re_aux_contraction = regex.compile(r"(.+)['’](s|ve|d|ll|m|re|em)$", regex.IGNORECASE)
  re_not_contraction = regex.compile(r"([a-z]{2,})n['’]t$", regex.IGNORECASE)
  re_multi_possessive = regex.compile(r"([a-z]{2,})(s|S)['’ ]$")
  def AnnotateText(self, text):
    spans = []
    cursor = 0
    for match in self.re_latin_word.finditer(text):
      start, end = match.span()
      if start > cursor:
        region = text[cursor:start]
        spans.append((region, False))
      region = text[start:end]
      spans.append((region, True))
      cursor = end
    if cursor < len(text):
      region = text[cursor:]
      spans.append((region, False))
    out_spans = []
    sent_head = True
    for start_index in range(0, len(spans)):
      span, span_is_word = spans[start_index]
      if not span_is_word:
        out_spans.append((span, False, None))
        continue
      def CheckSurfaceMatch(surface, title):
        if surface == title:
          return True
        if sent_head and surface != "I":
          norm_surface = surface[0].lower() + surface[1:]
          if norm_surface == title:
            return True
        return False
      annots = []
      tokens = []
      for index in range(start_index, len(spans)):
        token, token_is_word = spans[index]
        if token_is_word:
          tokens.append(token)
          phrase = " ".join(tokens)
          variants = []
          variants.append((phrase, 1.0))
          for infl_base in self.SearchInflections(phrase.lower()):
            if infl_base.count(" ") + 1 != len(tokens): continue
            variants.append((infl_base, 0.7))
          if index == start_index:
            match = self.re_aux_contraction.search(token)
            if match:
              bare = match.group(1)
              variants.append((bare, 0.7))
              for infl_base in self.SearchInflections(bare.lower()):
                variants.append((infl_base, 0.6))
              suffix = match.group(2).lower()
              if suffix == "s" and bare.lower() in ("it", "he", "she"):
                variants.append(("be", 0.0001))
              elif suffix == "ve":
                variants.append(("would", 0.0001))
              elif suffix == "d":
                variants.append(("would", 0.0001))
                variants.append(("have", 0.0001))
              elif suffix == "ll":
                variants.append(("will", 0.0001))
              elif suffix == "m" or suffix == "re":
                variants.append(("be", 0.0001))
              elif suffix == "em":
                variants.append(("them", 0.0001))
            match = self.re_not_contraction.search(token)
            if match:
              bare = match.group(1)
              lower_bare = bare.lower()
              if lower_bare == "wo": bare = "will"
              if lower_bare == "ca": bare = "can"
              if lower_bare == "sha": bare = "shall"
              variants.append((bare, 0.7))
              variants.append(("not", 0.0001))
            match = self.re_multi_possessive.search(token)
            if match:
              bare = match.group(1) + match.group(2)
              for infl_base in self.SearchInflections(bare):
                variants.append((infl_base, 0.7))
            if token.find("-") > 0:
              for part in token.split("-"):
                if not regex.search(r"\p{Latin}{3,}", part): continue
                variants.append((part, 0.0002))
                for infl_base in self.SearchInflections(part.lower()):
                  variants.append((infl_base, 0.0001))
          uniq_variants = set()
          uniq_words = set()
          for variant, var_score in variants:
            if variant in uniq_variants: continue
            uniq_variants.add(variant)
            for entry in self.SearchExact(variant, 10):
              word = entry["word"]
              if word in uniq_words: continue
              uniq_words.add(word)
              match = False
              if CheckSurfaceMatch(phrase, word):
                match = True
              else:
                for infl_name in self.infl_names:
                  infl_values = entry.get(infl_name)
                  if infl_values:
                    for infl_value in infl_values:
                      if CheckSurfaceMatch(phrase, infl_value):
                        match = True
                        break
                  if match:
                    break
              prob = float(entry.get("probability") or 0)
              prob_score = min(0.05, max(prob ** 0.5, 0.00001)) * 20
              aoa = entry.get("aoa") or entry.get("aoa_concept") or entry.get("aoa_base")
              if aoa:
                aoa = float(aoa)
              else:
                aoa = math.log(prob + 0.00000001) * -1 + 3.5
              aoa = min(max(aoa, 3), 20)
              aoa_score = (25 - min(aoa, 20.0)) / 10.0
              entry["aoa_syn"] = int(aoa)
              tran_score = 1.0 if "translation" in entry else 0.5
              item_score = math.log2(len(entry["item"]) + 1)
              labels = set()
              for item in entry["item"]:
                labels.add(item["label"])
              label_score = len(labels) + 1
              children = entry.get("child")
              child_score = math.log2((len(children) if children else 0) + 4)
              width_score = (200 if "translation" in entry else 10) ** word.count(" ")
              match_score = 1.0 if match else 0.2
              score = (var_score * prob_score * aoa_score * tran_score * item_score *
                       label_score * child_score * match_score * width_score)
              annots.append((entry, score))
        elif index == start_index:
          break
        elif not regex.match(r"\s", token):
          break
        if len(tokens) > 3:
          break
      annots = sorted(annots, key=lambda x: x[1], reverse=True)
      annots = [x[0] for x in annots]
      out_spans.append((span, True, annots or None))
      sent_head = span.find("\n") >= 0 or bool(regex.search(r"[.!?;:]", span))
    return out_spans


def ConvertHTMLToText(text):
  text = regex.sub(r"\s+", " ", text)
  text = regex.sub(r"<!--.*?-->", "", text)
  title = ""
  match = regex.search(r"<title[^>]*?>(.*?)</title>", text)
  if match:
    title = match.group(1)
  text = regex.sub(r".*<body[^>]*?>", "", text)
  text = regex.sub(r"</body>.*", "", text)
  text = regex.sub(r"<script[^>]*?>.*?</script>", "", text, flags=regex.IGNORECASE)
  text = regex.sub(r"<style[^>]*?>.*?</style>", "", text, flags=regex.IGNORECASE)
  text = regex.sub(r"<h1(>|\s[^>]*?>)", "[_LF_][_HEAD1_]", text, flags=regex.IGNORECASE)
  text = regex.sub(r"<h2(>|\s[^>]*?>)", "[_LF_][_HEAD2_]", text, flags=regex.IGNORECASE)
  text = regex.sub(r"<h3(>|\s[^>]*?>)", "[_LF_][_HEAD3_]", text, flags=regex.IGNORECASE)
  text = regex.sub(r"<(h\d|p|div|br|li|dt|dd|tr)(>|\s[^>]*?>)",
                   "[_LF_]", text, flags=regex.IGNORECASE)
  text = regex.sub(r"</(h\d|p|div|br|li|dt|dd|tr)>",
                   "[_LF_]", text, flags=regex.IGNORECASE)
  text = regex.sub(r"<(th|td)(>|\s[^>]*?>)",
                   " ", text, flags=regex.IGNORECASE)
  text = regex.sub(r"<[^>]*?>", "", text)
  text = html.unescape(text)
  lines = []
  if title:
    lines.append("====[META]====")
    lines.append("[title]: " + title)
    lines.append("====[PAGE]====")
  for line in text.split("[_LF_]"):
    line = line.strip()
    match = regex.search(r"^\[_HEAD([1-3])_\]", line)
    if match:
      line = "[head" + match.group(1) + "]: " + line[match.end():].strip()
    if line:
      lines.append(line)
  text = "\n".join(lines)
  return text


def CramText(text):
  lines = []
  last_line = ""
  for line in text.split("\n"):
    line = line.strip()
    if line:
      if last_line:
        last_line += " "
      last_line += line
    elif last_line:
      lines.append(last_line)
      last_line = ""
  if last_line:
    lines.append(last_line)
  text = "\n".join(lines)
  return text


def DivideTextToPages(text):
  meta = []
  pages = []
  lines = []
  is_meta = False
  for line in text.split("\n"):
    line = line.strip()
    if not line: continue
    if line == "====[META]====":
      is_meta = True
      if lines:
        pages.append(lines)
      lines = []
    elif line == "====[PAGE]====":
      is_meta = False
      if lines:
        pages.append(lines)
      lines = []
    elif is_meta:
      meta.append(line)
    else:
      lines.append(line)
  if lines:
    pages.append(lines)
  return meta, pages
