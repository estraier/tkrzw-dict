#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to attach example sentences to a union database
#
# Usage:
#   attach_union_examples.py [--input str] [--output str] [--index str] [--max_examples]
#
# Example:
#   ./attach_union_examples.py --input union-body.tkh --output union-body-final.tkh \
#     --index tanaka-index.tkh
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
import os
import regex
import sys
import time
import tkrzw
import tkrzw_dict
import tkrzw_pron_util
import tkrzw_tokenizer


logger = tkrzw_dict.GetLogger()
inflection_names = ("noun_plural", "verb_singular", "verb_present_participle",
                    "verb_past", "verb_past_participle",
                    "adjective_comparative", "adjective_superlative",
                    "adverb_comparative", "adverb_superlative")


def MakeSentenceKey(text):
  text = regex.sub(r"[^ \p{Latin}]", "", text)
  tokens = [x for x in text.lower().split(" ") if x]
  chars = []
  for token in tokens:
    chars.append(chr(hash(token) % 0xD800))
  return "".join(chars)

def NormalizeSentence(text):
  text = text.strip()
  text = regex.sub(r"^(・|-|\*)+ *", "", text)
  text = regex.sub(r"\s+", " ", text).strip()
  return text

class AttachExamplesBatch:
  def __init__(self, input_path, output_path, index_paths, rev_prob_path, hint_paths,
               max_examples, min_examples, max_tran_matches):
    self.input_path = input_path
    self.output_path = output_path
    self.index_paths = index_paths
    self.rev_prob_path = rev_prob_path
    self.hint_paths = hint_paths
    self.max_examples = max_examples
    self.min_examples = min_examples
    self.max_tran_matches = max_tran_matches
    self.count_all_entries = 0
    self.count_hit_entries = 0
    self.count_examples = 0
    self.count_labels = collections.defaultdict(int)
    self.tokenizer = tkrzw_tokenizer.Tokenizer()

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}, index_path={}".format(
      self.input_path, self.output_path, ":".join(self.index_paths)))
    word_dict = self.AttachExamples()
    self.SaveEntries(word_dict)
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def AttachExamples(self):
    start_time = time.time()
    logger.info("Attaching started: input_path={}, index_path={}".format(
      self.input_path, ":".join(self.index_paths)))
    word_dict = {}
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False).OrDie()
    indices = []
    for index_path in self.index_paths:
      logger.info("Opening an index: path={}".format(index_path))
      rank = 0
      match = regex.search(r"^(\++)(.*)", index_path)
      if match:
        rank = len(match.group(1))
        index_path = match.group(2)
      match = regex.search(r"^(-+)(.*)", index_path)
      if match:
        rank = -1
        index_path = match.group(2)
      index_dbm = tkrzw.DBM()
      index_dbm.Open(index_path, False).OrDie()
      label = os.path.splitext(os.path.basename(index_path))[0]
      indices.append((index_dbm, rank, label))
    rev_prob_dbm = None
    if self.rev_prob_path:
      rev_prob_dbm = tkrzw.DBM()
      rev_prob_dbm.Open(self.rev_prob_path, False, dbm="HashDBM").OrDie()
    hint_dict = self.ReadHint()
    it = input_dbm.MakeIterator()
    keys = []
    infl_dict = collections.defaultdict(list)
    it.First().OrDie()
    while True:
      record = it.GetStr()
      if not record: break;
      key, data = record
      entries = json.loads(data)
      sum_prob = 0
      for entry in entries:
        word = entry["word"]
        prob = float(entry.get("probability") or 0)
        if entry.get("parent"):
          prob *= 0.1
        sum_prob += prob
        for infl_name in inflection_names:
          infl_values = entry.get(infl_name)
          if not infl_values or infl_name == word: continue
          for infl in infl_values:
            infl_dict[infl].append(word)
        for item in entry["item"]:
          for text in item["text"].split(" [-] ")[1:]:
            match = regex.search("^\[synonym\]: ", text)
            if match:
              for synonym in text[match.span()[1]:].split(","):
                synonym = synonym.strip()
                synonym_tokens = synonym.split(" ")
                if len(synonym_tokens) >= 2 and word in synonym_tokens:
                  infl_dict[synonym].append(word)
      keys.append((key, sum_prob))
      it.Next()
    keys = sorted(keys, key=lambda x: (-x[1], x[0]))
    word_dict = {}
    adopt_source_counts = collections.defaultdict(int)
    adopt_parent_hashes = set()
    for key, sum_prob in keys:
      data = input_dbm.GetStr(key)
      if not data: continue
      entries = json.loads(data)
      first_entry = True
      for entry in entries:
        self.count_all_entries += 1
        if self.count_all_entries % 10000 == 0:
          logger.info("Attaching: entries={}, hit_entries={}, examples={}".format(
            self.count_all_entries, self.count_hit_entries, self.count_examples))
        strict = not first_entry
        self.AttachExamplesEntry(
          entry, indices, input_dbm, rev_prob_dbm,
          infl_dict, hint_dict, adopt_source_counts, adopt_parent_hashes, strict)
        first_entry = False
      serialized = json.dumps(entries, separators=(",", ":"), ensure_ascii=False)
      word_dict[key] = serialized
    for index_dbm, rank, label in indices:
      index_dbm.Close().OrDie()
    input_dbm.Close().OrDie()
    logger.info("Attaching done: elapsed_time={:.2f}s,"
                " entries={}, hit_entries={}, examples={}".format(
                  time.time() - start_time,
                  self.count_all_entries, self.count_hit_entries, self.count_examples))
    for label in sorted(self.count_labels.keys()):
      logger.info("  Label:{} = {}".format(label, self.count_labels[label]))
    return word_dict

  def ReadHint(self):
    hint_dict = collections.defaultdict(list)
    for hint_path in self.hint_paths:
      if not hint_path: continue
      logger.info("Reading hint: path={}".format(hint_path))
      with open(hint_path) as input_file:
        for line in input_file:
          fields = line.strip().split("\t")
          if len(fields) < 2: continue
          source = fields[0]
          for target in fields[1:]:
            hint_dict[source].append(target)
      logger.info("Reading hint done: hints={}".format(len(hint_dict)))
    return hint_dict

  def AttachExamplesEntry(self, entry, indices, input_dbm, rev_prob_dbm,
                          infl_dict, hint_dict, adopt_source_counts, adopt_parent_hashes, strict):
    entry.pop("example", None)
    word = entry["word"]
    core_words = collections.defaultdict(float)
    core_words[word] = 1.0
    inflections = [
      ("noun_plural", 0.8),
      ("verb_singular", 0.8),
      ("verb_present_participle", 0.7),
      ("verb_past", 0.7),
      ("verb_past_participle", 0.6),
      ("adjective_comparative", 0.3),
      ("adjective_superlative", 0.2),
      ("adverb_comparative", 0.3),
      ("adverb_superlative", 0.2),
    ]
    labels = set()
    word_poses = set()
    for item in entry["item"]:
      labels.add(item["label"])
      word_poses.add(item["pos"])
    if len(labels) < 2:
      strict = True
    best_source_length = 57
    best_target_length = 29
    for infl_name, weight in inflections:
      infls = entry.get(infl_name)
      if infls:
        infl = infls[0]
        core_words[infl] = max(core_words[infl], weight)
    docs = []
    for core_word, weight in core_words.items():
      for i, index in enumerate(indices):
        index_dbm, rank, label = index
        index_weight = 0.98 ** i
        if rank > 0:
          index_weight *= 0.5 ** rank
        expr = index_dbm.GetStr(core_word.lower())
        if not expr: continue
        doc_ids = expr.split(",")
        for doc_id in doc_ids:
          docs.append((i, doc_id, weight * index_weight))
    docs = sorted(docs, key=lambda x: (-x[2], x[0], x[1]))
    uniq_docs = []
    uniq_keys = set()
    for index_id, doc_id, weight in docs:
      key = "{}:{}".format(index_id, doc_id)
      if key in uniq_keys: continue
      uniq_keys.add(key)
      uniq_docs.append((index_id, doc_id, weight))
    if not uniq_docs:
      return
    hints = set(hint_dict.get(word) or [])
    seed_records = []
    source_hashes = set()
    target_hashes = set()
    for index_id, doc_id, doc_weight in uniq_docs:
      index_dbm, rank, label = indices[index_id]
      doc_key = "[" + doc_id + "]"
      value = index_dbm.GetStr(doc_key)
      if not value: continue
      fields = value.split("\t")
      if len(fields) < 2: continue
      source, target = fields[:2]
      source_hash = hash(source.lower())
      if source_hash in source_hashes:
        self.count_labels["reject-source-duplication"] += 1
        continue
      source_hashes.add(source_hash)
      target_hash = hash(target.lower())
      if target_hash in target_hashes:
        self.count_labels["reject-target-duplication"] += 1
        continue
      target_hashes.add(target_hash)
      source = NormalizeSentence(source)
      target = NormalizeSentence(target)
      short_min_ratio = 3
      if regex.search("[A-Z].*[.!?]$", source):
        short_min_ratio = 4
      if len(source) < best_source_length / short_min_ratio:
        self.count_labels["reject-source-short"] += 1
        continue
      if len(target) < best_target_length / short_min_ratio:
        self.count_labels["reject-target-short"] += 1
        continue
      if len(source) > best_source_length * 3:
        self.count_labels["reject-source-long"] += 1
        continue
      if len(target) > best_target_length * 3:
        self.count_labels["reject-target-long"] += 1
        continue
      if regex.search(r"あなた.*あなた", target) or regex.search(r"彼女.*彼女", target):
        self.count_labels["reject-target-bad-pronoun"] += 1
        continue
      if regex.search(r"^[」』】］）)\].?!;,？！を。、；]", target):
        self.count_labels["reject-target-bad-prefix"] += 1
        continue
      if target.count("、") >= 4 or target.count("。") >= 3 or target.count(" ") >= 4:
        self.count_labels["reject-target-bad-punctuation"] += 1
        continue
      adopt_count = adopt_source_counts.get(source_hash) or 0
      if adopt_count > 0:
        doc_weight *= 0.9 ** adopt_count
        self.count_labels["demote-dup-source"] += 1
      source_loc = -1
      source_hit_weight = 0
      for core_word, core_weight in core_words.items():
        loc = source.find(core_word)
        if regex.search(r"[A-Z]", core_word):
          if loc > 0:
            source_loc = loc
            source_hit_weight = 1.0
          elif not strict and loc == 0:
            source_loc = loc
            source_hit_weight = 0.5
        else:
          if loc >= 0:
            source_loc = loc
            source_hit_weight = 1.0
          elif not strict:
            cap_word = core_word[0].upper() + core_word[1:]
            if source.startswith(cap_word):
              source_loc = 0
              source_hit_weight = 0.5
      if source_loc < 0:
        self.count_labels["reject-source-mismatch"] += 1
        continue
      source_hit_score = ((len(source) - source_loc) / len(source) / 2 + 0.5) ** 0.5
      source_hit_score *= source_hit_weight
      source_len_score = 1 / math.exp(abs(math.log(len(source) / best_source_length)))
      target_len_score = 1 / math.exp(abs(math.log(len(target) / best_target_length)))
      length_score = (source_len_score * target_len_score) ** 0.2
      tmp_score = doc_weight * source_hit_score * length_score
      seed_records.append((index_id, doc_weight, rank, source, target,
                           source_hit_score, length_score, tmp_score))
    seed_records = sorted(seed_records, key=lambda x: (-x[7], source, target))
    seed_records = seed_records[:3000]
    prep_records = []
    for (index_id, doc_weight, rank, source, target,
         source_hit_score, length_score, tmp_score) in seed_records:
      target_tokens = self.tokenizer.GetJaPosList(target)
      uniq_key = MakeSentenceKey(source)
      prep_records.append((index_id, doc_weight, rank, source, target,
                           source_hit_score, length_score, target_tokens, uniq_key))
    dedup_records = []
    for i, record in enumerate(prep_records):
      uniq_key = record[8]
      ti = max(0, i - 10)
      end = min(i + 10, len(prep_records))
      is_dup = False
      while ti < end:
        if ti != i:
          old_uniq_key = prep_records[ti][8]
          dist = tkrzw.Utility.EditDistanceLev(old_uniq_key, uniq_key)
          dist /= max(len(old_uniq_key), len(uniq_key))
          if dist < 0.4:
            is_dup = True
            break
        ti += 1
      if is_dup:
        self.count_labels["reject-source-similar"] += 1
      else:
        dedup_records.append(record)
    adhoc_trans = None
    if rev_prob_dbm and not regex.search(r"[A-Z]", word) and len(dedup_records) > 5:
      adhoc_trans = self.GetAdHocTranslations(
        word, word_poses, input_dbm, rev_prob_dbm, hints, dedup_records[:100])
    tran_cands = (entry.get("translation") or []).copy()
    uniq_trans = set()
    for tran in tran_cands:
      norm_tran = tran.lower()
      uniq_trans.add(norm_tran)
    orig_trans = uniq_trans.copy()
    for tran in hints:
      norm_tran = tran.lower()
      if norm_tran not in uniq_trans:
        tran_cands.append(tran)
        uniq_trans.add(norm_tran)
        self.count_labels["use-hint-translation"] += 1
    if adhoc_trans:
      for tran in adhoc_trans:
        norm_tran = tran.lower()
        if norm_tran not in uniq_trans:
          tran_cands.append(tran)
          uniq_trans.add(norm_tran)
          self.count_labels["use-adhoc-translation"] += 1
    for item in entry.get("item") or []:
      text = item["text"]
      match = regex.search(r"\[translation\]: ([^\[]+)", text)
      if match:
        tran = match.group(1)
        tran = regex.sub(r"\([^)]+\)", "", tran)
        for tran in tran.split(","):
          tran = tran.strip()
          if regex.search("\p{Han}", tran) and len(tran) >= 2:
            tran_cands.append(tran)
    trans = {}
    uniq_trans = {}
    tran_weight_base = 1.0
    for tran in tran_cands:
      norm_tran = tran.lower()
      if norm_tran in uniq_trans: continue
      tran_score = 1.0
      if regex.search(r"\p{Han}", tran):
        tran_score *= 1.5
      if len(tran) == 1:
        tran_score *= 0.5
      elif len(tran) == 2:
        tran_score *= 0.8
      elif len(tran) == 3:
        tran_score *= 0.95
      new_score = tran_weight_base * tran_score
      old_score = trans.get(tran) or 0
      trans[tran] = max(old_score, new_score)
      alternatives = []
      match = regex.search(
        r"(.*[\p{Han}]{2,})(する|される|される|させる|をする|している|な)", tran)
      if match:
        alternatives.append(match.group(1))
      core, prefix, suffix = self.tokenizer.StripJaParticles(tran)
      if core and len(core) >= 2 and regex.search(r"[\p{Han}\p{Katakana}]", core):
        alternatives.append(core)
      for alternative in alternatives:
        old_score = trans.get(alternative) or 0
        trans[alternative] = max(old_score, new_score * 0.8)
      tran_weight_base *= 0.97
    infls = infl_dict.get(word)
    infl_trans = set()
    if infls:
      for infl in infls:
        infl_key = tkrzw_dict.NormalizeWord(infl)
        infl_data = input_dbm.GetStr(infl_key)
        if not infl_data: continue
        infl_entries = json.loads(infl_data)
        for infl_entry in infl_entries:
          if infl_entry["word"] != infl: continue
          for infl_tran in infl_entry.get("translation") or []:
            infl_trans.add(infl_tran)
    cache_stem_trans = {}
    scored_records = []
    tran_hit_counts = {}
    for (index_id, doc_weight, rank, source, target,
         source_hit_score, length_score, target_tokens, uniq_key) in dedup_records:
      if rank < 0: continue
      if not target_tokens: continue
      if target_tokens[0][1] in ["助詞", "助動詞"] and strict:
        self.count_labels["reject-target-aux-start"] += 1
        continue
      tran_score = 0
      best_tran = None
      is_dup_tran = False
      for tran, tran_weight in trans.items():
        tran_hit_count = tran_hit_counts.get(tran) or 0
        tran_weight *= 0.7 ** tran_hit_count
        if self.CheckTranMatch(target, target_tokens, tran):
          norm_tran = tran.lower()
          if norm_tran not in orig_trans:
            tran_weight *= 0.4
          if infl_trans:
            if tran in infl_trans:
              tran_weight *= 0.1
              is_dup_tran = True
            else:
              stem_trans = cache_stem_trans.get(tran)
              if not stem_trans:
                stem_trans = self.GetTranslationStems(tran)
                cache_stem_trans[tran] = stem_trans
              stem_hit = False
              for stem_tran in stem_trans:
                if stem_tran in infl_trans:
                  stem_hit = True
                  break
              if stem_hit:
                tran_weight *= 0.1
                is_dup_tran = True
          if tran_weight > tran_score:
            tran_score = tran_weight
            best_tran = tran
      if best_tran:
        tran_hit_count = tran_hit_counts.get(best_tran) or 0
        tran_hit_counts[best_tran] = tran_hit_count + 1
      if (strict or rank != 0) and not best_tran:
        self.count_labels["reject-target-mismatch"] += 1
        continue
      if not is_dup_tran:
        tran_score += 0.2
      final_score = (tran_score * tran_score *
                     source_hit_score * length_score * doc_weight) ** (1 / 5)
      match = regex.search(r"[-\p{Latin}']{3,}", target)
      if match:
        raw_word = match.group(0)
        if regex.fullmatch(r"[-A-Z]+", raw_word):
          final_score *= 0.95
        elif regex.search(r"[A-Z]", raw_word) and source.lower().find(raw_word.lower()) >= 0:
          final_score *= 0.8
        else:
          final_score *= 0.5
      if infls:
        for infl in infls:
          parent_hash = hash(infl + ":" + source)
          if parent_hash in adopt_parent_hashes:
            final_score *= 0.1
            break
      scored_records.append((final_score, index_id, source, target, best_tran, uniq_key))
    scored_records = sorted(scored_records, key=lambda x: (-x[0], x[1], x[2]))
    final_records = []
    reserve_records = []
    uniq_keys = []
    adopt_tran_counts = {}
    for score, index_id, source, target, best_tran, uniq_key in scored_records:
      if len(final_records) >= self.max_examples: break
      is_dup = False
      for old_uniq_key in uniq_keys:
        dist = tkrzw.Utility.EditDistanceLev(old_uniq_key, uniq_key)
        dist /= max(len(old_uniq_key), len(uniq_key))
        if dist < 0.5:
          is_dup = True
          break
      if is_dup:
        self.count_labels["reject-source-similar"] += 1
        continue
      uniq_keys.append(uniq_key)
      example = {"e": source, "j": target}
      is_ok = True
      if best_tran:
        adopt_tran_count = adopt_tran_counts.get(best_tran) or 0
        if adopt_tran_count > self.max_tran_matches:
          self.count_labels["reserve-tran-dup"] += 1
          is_ok = False
        adopt_tran_counts[best_tran] = adopt_tran_count + 1
      if len(source) > best_source_length * 2:
        self.count_labels["reserve-source-long"] += 1
        is_ok = False
      elif len(target) > best_target_length * 2:
        self.count_labels["reserve-target-long"] += 1
        is_ok = False
      if is_ok:
        final_records.append((example, index_id))
      else:
        reserve_records.append((example, index_id))
    for example, index_id in reserve_records:
      if len(final_records) >= self.min_examples: break
      final_records.append((example, index_id))
    if final_records:
      examples = []
      for example, index_id in final_records:
        examples.append(example)
        label = indices[index_id][2]
        self.count_labels["adopt-" + label] += 1
        source_hash = hash(example["e"])
        adopt_source_counts[source_hash] += 1
        parent_hash = hash(word + ":" + example["e"])
        adopt_parent_hashes.add(parent_hash)
      self.count_examples += len(examples)
      entry["example"] = examples
      self.count_hit_entries += 1

  def GetAdHocTranslations(self, word, word_poses, input_dbm, rev_prob_dbm, hints, dedup_records):
    sources = []
    pos_list = []
    for record in dedup_records:
      if record[2] > 2: continue
      sources.append(record[3])
      pos_list.append(record[7])
    if not pos_list:
      return []
    norm_phrases = collections.defaultdict(list)
    for tokens in pos_list:
      uniq_phrases = set()
      for start_index in range(len(tokens)):
        i = start_index
        end_index = min(start_index + 5, len(tokens))
        mid_phrase = ""
        norm_phrase = ""
        phrase_poses = {}
        while i < end_index:
          token = tokens[i]
          if token[1] in ["助詞", "助動詞"] and i - start_index >= 3: break
          if token[2] == "数":
            token[3] = token[0]
          if not token[3] or token[1] == "記号": break
          if token[3] in ["は", "が"] and token[1] == "助詞": break
          if i > start_index:
            norm_phrase += " "
          if token[1] in "助動詞" and token[0] == "な":
            phrase = mid_phrase + token[0]
          elif token[1] in "助動詞" and token[3] == "だ" and "adjective" in word_poses:
            phrase = mid_phrase + "な"
          else:
            phrase = mid_phrase + token[3]
          mid_phrase += token[0]
          norm_phrase += token[3]
          if token[1] == "名詞" and token[2] == "形容動詞語幹":
            phrase_poses = {"noun", "adjective"}
          elif token[1] == "名詞" and token[2] == "サ変接続":
            phrase_poses = {"noun", "verb"}
          elif token[1] == "名詞":
            phrase_poses = {"noun"}
          elif token[1] == "動詞":
            phrase_poses = {"verb"}
          elif token[1] == "形容詞":
            phrase_poses = {"adjective", "adverb"}
          pos_ok = False
          for word_pos in word_poses:
            if word_pos in phrase_poses:
              pos_ok = True
          all_ok = True
          if phrase in hints:
            pass
          elif not pos_ok:
            all_ok = False
          elif len(phrase) < 2 or len(phrase) > 12:
            all_ok = False
          elif token[1] == "助詞":
            all_ok = False
          elif norm_phrase in uniq_phrases:
            all_ok = False
          if all_ok:
            norm_phrases[norm_phrase].append(phrase)
            uniq_phrases.add(norm_phrase)
          i += 1
    candidates = []
    min_count = max(3, len(pos_list) / 10)
    for norm_phrase, surfaces in norm_phrases.items():
      count = len(surfaces)
      count_surfaces = collections.defaultdict(float)
      for surface in surfaces:
        count_surfaces[surface] += 1.2 if surface.endswith("る") else 1
      top_surface = sorted(count_surfaces.items(), key=lambda x: (-x[1], x[0]))[0][0]
      if top_surface in hints:
        count = int(count * 1.2 + 1)
      if count < min_count: continue
      if not regex.search("^[0-9]*[\p{Han}\p{Katakana}ー]{2,}", top_surface): continue
      ef_prob = count / len(pos_list)
      gen_prob = float(rev_prob_dbm.GetStr(norm_phrase) or 0) + 0.000001
      score = ef_prob / gen_prob
      if gen_prob >= 0.001: continue
      if score < 50.0: continue
      surfaces = set(surfaces)
      if top_surface.endswith("だ"):
        surfaces.add(top_surface[:-1] + "な")
      candidates.append((norm_phrase, top_surface, surfaces, score))
    if not candidates:
      return []
    colloc_trans = set()
    word_key = word.lower()
    colloc_word_counts = collections.defaultdict(int)
    for source in sources:
      for token in regex.split(r"\W", source):
        token = token.strip().lower()
        if token and token != word_key:
          colloc_word_counts[token] += 1
    for colloc_word, count in sorted(colloc_word_counts.items(), key=lambda x: (-x[1], x[0]))[:32]:
      if count < min_count: continue
      colloc_data = input_dbm.GetStr(colloc_word)
      if not colloc_data: continue
      colloc_entries = json.loads(colloc_data)
      for colloc_entry in colloc_entries:
        for colloc_tran in colloc_entry.get("translation") or []:
          colloc_trans.add(colloc_tran)
    candidates = sorted(candidates, key=lambda x: (-len(x[0]), norm_phrase, top_surface, score))
    new_candidates = []
    for norm_phrase, top_surface, surfaces, score in candidates:
      is_dup = False
      for surface in surfaces:
        if surface in colloc_trans:
          is_dup = True
      if is_dup: continue
      for sub_norm_phrase, sub_top_surface, sub_surfaces, sub_score in candidates:
        if len(norm_phrase) > len(sub_norm_phrase) and norm_phrase.find(sub_norm_phrase) >= 0:
          score += sub_score * 0.2
      new_candidates.append((norm_phrase, top_surface, score))
    new_candidates = sorted(new_candidates, key=lambda x: (-x[2], x[0], x[1]))
    final_records = []
    if new_candidates:
      min_score = new_candidates[0][2] * 0.2
      for norm_phrase, surface, score in new_candidates:
        if score < min_score: continue
        is_dup = False
        for final_norm_phrase, final_surface in final_records:
          if final_norm_phrase.find(norm_phrase) >= 0:
            is_dup = True
            break
        if is_dup: continue
        final_records.append((norm_phrase, surface))
    return [x[1] for x in final_records]

  def CheckTranMatch(self, target, tokens, query):
    if len(query) >= 2 and target.find(query) >= 0: return True
    start_index = 0
    while start_index < len(tokens):
      i = start_index
      end_index = min(start_index + 4, len(tokens))
      tmp_tokens = []
      while i < end_index:
        token = tokens[i]
        surface = token[0] if i < end_index - 1 else token[3]
        tmp_tokens.append(surface)
        token = "".join(tmp_tokens)
        if query == token:
          return True
        i += 1
      start_index += 1
    return False

  def GetTranslationStems(self, text):
    stem_trans = set()
    stem_trans.add(text)
    match = regex.search(
      r"^(.*[\p{Han}\p{Katakana}ー])(する|した|している|された|されて)$", text)
    if match:
      stem = match.group(1)
      stem_trans.add(stem + "する")
      stem_trans.add(stem + "した")
      stem_trans.add(stem + "している")
      stem_trans.add(stem + "された")
      stem_trans.add(stem + "されて")
    tokens = self.tokenizer.GetJaPosList(text)
    if tokens and tokens[-1][2] == "サ変接続":
      stem_trans.add(text + "する")
      stem_trans.add(text + "した")
      stem_trans.add(text + "している")
      stem_trans.add(text + "された")
      stem_trans.add(text + "されて")
    tokens[-1][0] = tokens[-1][3]
    phrase = ""
    for token in tokens:
      phrase += token[0]
    stem_trans.add(phrase)
    while len(tokens) >= 2:
      last_token = tokens[-1]
      if last_token[1] in ("助詞", "助動詞") or last_token[2] in ("接尾", "非自立"):
        tokens = tokens[:-1]
        tokens[-1][0] = tokens[-1][3]
        phrase = ""
        for token in tokens:
          phrase += token[0]
        stem_trans.add(phrase)
      else:
        break
    return stem_trans

  def SaveEntries(self, word_dict):
    start_time = time.time()
    logger.info("Saving started: output_path={}".format(self.output_path))
    output_dbm = tkrzw.DBM()
    num_buckets = len(word_dict) * 2
    output_dbm.Open(self.output_path, True, dbm="HashDBM", truncate=True,
                    align_pow=0, num_buckets=num_buckets)
    num_records = 0
    for key, serialized in word_dict.items():
      num_records += 1
      if num_records % 100000 == 0:
        logger.info("Saving: records={}".format(num_records))
      output_dbm.Set(key, serialized)
    output_dbm.Close().OrDie()
    logger.info("Saving done: elapsed_time={:.2f}s, records={}".format(
      time.time() - start_time, num_records))


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-body-final.tkh"
  index_paths = (tkrzw_dict.GetCommandFlag(args, "--index", 1) or "tanaka-index.tkh").split(",")
  rev_prob_path = tkrzw_dict.GetCommandFlag(args, "--rev_prob", 1) or ""
  hint_paths = (tkrzw_dict.GetCommandFlag(args, "--hint", 1) or "").split(",")
  max_examples = int(tkrzw_dict.GetCommandFlag(args, "--max_examples", 1) or 8)
  min_examples = int(tkrzw_dict.GetCommandFlag(args, "--min_examples", 1) or 3)
  max_tran_matches = int(tkrzw_dict.GetCommandFlag(args, "--max_tran_matches", 1) or 2)
  if not input_path:
    raise RuntimeError("the input path is required")
  if not output_path:
    raise RuntimeError("the output path is required")
  if not index_paths:
    raise RuntimeError("the index path is required")
  AttachExamplesBatch(input_path, output_path, index_paths, rev_prob_path, hint_paths,
                      max_examples, min_examples, max_tran_matches).Run()


if __name__=="__main__":
  main()
