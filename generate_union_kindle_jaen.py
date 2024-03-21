#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to generate files to make a JaEn Kindle dictionary from the union dictionary
#
# Usage:
#   generate_union_kindle_jaen.py [--input str] [--output str] [--tran_prob str] [--quiet]
#
# Example:
#   ./generate_union_kindle_jaen.py --input union-body.tkh --output union-dict-epub
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
import copy
import datetime
import html
import json
import logging
import math
import os
import pathlib
import regex
import sys
import time
import tkrzw
import tkrzw_dict
import tkrzw_tokenizer
import urllib
import uuid


logger = tkrzw_dict.GetLogger()
CURRENT_UUID = str(uuid.uuid1())
CURRENT_DATETIME = regex.sub(r"\..*", "Z", datetime.datetime.now(
  datetime.timezone.utc).isoformat())
PACKAGE_HEADER_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<package unique-identifier="pub-id" version="3.0" xmlns="http://www.idpf.org/2007/opf" xml:lang="ja">
<metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
<dc:identifier id="pub-id">urn:uuid:{}</dc:identifier>
<dc:publisher>dbmx.net</dc:publisher>
<dc:title>{}</dc:title>
<dc:language>ja</dc:language>
<dc:language>en</dc:language>
<dc:type id="tp">dictionary</dc:type>
<meta property="dcterms:modified">{}</meta>
<meta property="dcterms:type" refines="#tp">bilingual</meta>
<meta property="source-language">ja</meta>
<meta property="target-language">en</meta>
<x-metadata>
<DictionaryInLanguage>ja</DictionaryInLanguage>
<DictionaryOutLanguage>en</DictionaryOutLanguage>
<DefaultLookupIndex>ja</DefaultLookupIndex>
</x-metadata>
</metadata>
<manifest>
<item id="style" href="style.css" media-type="text/css"/>
<item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>
<item id="overview" href="overview.xhtml" media-type="application/xhtml+xml"/>
"""
PACKAGE_MIDDLE_TEXT = """</manifest>
<spine page-progression-direction="default">
<itemref idref="nav"/>
<itemref idref="overview"/>
"""
PACKAGE_FOOTER_TEXT = """</spine>
</package>
"""
STYLE_TEXT = """html,body { margin: 0; padding: 0; background: #fff; color: #000; font-size: 12pt;
  text-align: left; text-justify: none; direction: ltr; }
div, p, h1, h2, h3 { text-align: left; text-justify: none; }
span.word { font-weight: bold; }
span.pron { font-size: 90%; color: #444; }
span.gloss { font-size: 90%; color: #444; }
"""
NAVIGATION_HEADER_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops">
<head>
<title>{}: Contents</title>
<link rel="stylesheet" href="style.css"/>
</head>
<body>
<h1>{}</h1>
<article>
<h2>Index</h2>
<nav epub:type="toc">
<ol>
<li><a href="overview.xhtml">Overview</a></li>
"""
NAVIGATION_FOOTER_TEXT = """</ol>
</nav>
</article>
</body>
</html>
"""
OVERVIEW_TEXT = """<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops" lang="ja">
<head>
<title>{}: Overview</title>
<link rel="stylesheet" href="style.css"/>
</head>
<body>
<article>
<h2>Overview</h2>
<p>This dictionary is made from multiple data sources published as open-source data.  They include <a href="https://wordnet.princeton.edu/">WordNet</a>, <a href="http://compling.hss.ntu.edu.sg/wnja/index.en.html">Japanese WordNet</a>, <a href="https://ja.wiktionary.org/">Japanese Wiktionary</a>, <a href="https://en.wiktionary.org/">English Wiktionary</a>, <a href="http://www.edrdg.org/jmdict/edict.html">EDict2</a>, <a href="http://edrdg.org/wiki/index.php/Tanaka_Corpus">Tanaka corpus</a>, <a href="https://alaginrc.nict.go.jp/WikiCorpus/">Japanese-English Bilingual Corpus of Wikipedia's Kyoto Articles</a>, <a href="https://nlp.stanford.edu/projects/jesc/index_ja.html">Japanese-English Subtitle Corpus</a>, <a href="https://www.statmt.org/cc-aligned/">CCAligned</a>, <a href="https://anc.org/">Open American National Corpus</a>, and <a href="https://commoncrawl.org/">Common Crawl</a>.  See <a href="https://dbmx.net/dict/">the homepage</a> for details to organize the data.  Using and/or redistributing this data should be done according to the license of each data source.</p>
<p>In each word entry, the title word is shown in bold.  Some words have a pronounciation expression in hiragana, bracketed as "(...)".  A list of translation can come next.  Some have definitions of the words in English.</p>
<p>The number of words is {}.  The number of items is {}.</p>
<h2>Copyright</h2>
<div>WordNet Copyright 2021 The Trustees of Princeton University.</div>
<div>Japanese Wordnet Copyright 2009-2011 NICT, 2012-2015 Francis Bond and 2016-2017 Francis Bond, Takayuki Kuribayashi.</div>
<div>Wikipedia and Wiktionary data are copyrighted by each contributers and licensed under CC BY-SA and GFDL.</div>
<div>EDict2 Copyright 2017 The Electronic Dictionary Research and Development Group.</div>
<div>Japanese-English Bilingual Corpus of Wikipedia's Kyoto Articles Copyright 2010-2011 NICT.</div>
<div>Japanese-English Subtitle Corpus Copyright 2019 Stanford University, Google Brain, and Rakuten Institute of Technology. </div>
<div>CCAlign Copyright 2020 Ahmed El-Kishky, Vishrav Chaudhary, Francisco Guzman, Philipp Koehn.</div>
</article>
</body>
</html>
"""
MAIN_HEADER_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops" lang="ja" xmlns:mbp="https://kindlegen.s3.amazonaws.com/AmazonKindlePublishingGuidelines.pdf" xmlns:mmc="https://kindlegen.s3.amazonaws.com/AmazonKindlePublishingGuidelines.pdf" xmlns:idx="https://kindlegen.s3.amazonaws.com/AmazonKindlePublishingGuidelines.pdf">
<head>
<title>{}: {}</title>
<link rel="stylesheet" href="style.css"/>
</head>
<body epub:type="dictionary">
<mbp:frameset>
<h2>Words: {}</h2>
<br/>
"""
MAIN_FOOTER_TEXT = """</mbp:frameset>
</body>
</html>
"""
KANA_CONVERSION_MAP = {
  "が": ("か", 6), "ぎ": ("き", 6), "ぐ": ("く", 6), "げ": ("け", 6), "ご": ("こ", 6),
  "ざ": ("さ", 6), "じ": ("し", 6), "ず": ("す", 6), "ぜ": ("せ", 6), "ぞ": ("そ", 6),
  "だ": ("た", 6), "ぢ": ("ち", 6), "づ": ("つ", 6), "で": ("て", 6), "ど": ("と", 6),
  "ば": ("は", 6), "び": ("ひ", 6), "ぶ": ("ふ", 6), "べ": ("へ", 6), "ぼ": ("ほ", 6),
  "ぱ": ("は", 7), "ぴ": ("ひ", 7), "ぷ": ("ふ", 7), "ぺ": ("へ", 7), "ぽ": ("ほ", 7),
  "っ": ("つ", 4),
  "ぁ": ("あ", 4), "ぃ": ("い", 4), "ぅ": ("う", 4), "ぇ": ("え", 4), "ぉ": ("お", 4),
  "ゃ": ("や", 4), "ゅ": ("ゆ", 4), "ょ": ("よ", 4), "ゕ": ("か", 4), "ゖ": ("け", 4),
  "ゔ": ("う", 6),
}


def MakeYomiKey(yomi):
  norm_chars = []
  priorities = []
  last_norm_char = ""
  for char in yomi:
    priority = 5
    if char == "ー":
      if last_norm_char in "あかさたなはまやらわ":
        norm_char, priority = ("あ", 3)
      elif last_norm_char in "いきしちにひみりゐ":
        norm_char, priority = ("い", 3)
      elif last_norm_char in "うくすつぬふむゆる":
        norm_char, priority = ("う", 3)
      elif last_norm_char in "えけせてねへめれゑ":
        norm_char, priority = ("え", 3)
      elif last_norm_char in "おこそとのほもよろを":
        norm_char, priority = ("お", 3)
      elif last_norm_char in "ん":
        norm_char, priority = ("ん", 3)
      else:
        norm_char, priority = (char, 3)
    else:
      norm_char, priority = KANA_CONVERSION_MAP.get(char) or (char, 5)
    norm_chars.append(norm_char)
    priorities.append(str(priority))
    last_norm_char = norm_char[0]
  return "".join(norm_chars) + "\0" + "".join(priorities)


def esc(expr):
  if expr is None:
    return ""
  return html.escape(str(expr), True)


class GenerateUnionEPUBBatch:
  def __init__(self, input_path, output_path, supplement_labels,
               tran_prob_path, phrase_prob_path, rev_prob_path,
               yomi_paths, tran_aux_paths, rev_tran_aux_paths,
               conj_verb_path, conj_adj_path, title):
    self.input_path = input_path
    self.output_path = output_path
    self.supplement_labels = supplement_labels
    self.tran_prob_path = tran_prob_path
    self.phrase_prob_path = phrase_prob_path
    self.rev_prob_path = rev_prob_path
    self.yomi_paths = yomi_paths
    self.tran_aux_paths = tran_aux_paths
    self.rev_tran_aux_paths = rev_tran_aux_paths
    self.conj_verb_path = conj_verb_path
    self.conj_adj_path = conj_adj_path
    self.title = title
    self.tokenizer = tkrzw_tokenizer.Tokenizer()
    self.num_words = 0
    self.num_items = 0

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}".format(
      str(self.input_path), self.output_path))
    tran_prob_dbm = None
    if self.tran_prob_path:
      tran_prob_dbm = tkrzw.DBM()
      tran_prob_dbm.Open(self.tran_prob_path, False, dbm="HashDBM").OrDie()
    phrase_prob_dbm = None
    if self.phrase_prob_path:
      phrase_prob_dbm = tkrzw.DBM()
      phrase_prob_dbm.Open(self.phrase_prob_path, False, dbm="HashDBM").OrDie()
    rev_prob_dbm = None
    if self.rev_prob_path:
      rev_prob_dbm = tkrzw.DBM()
      rev_prob_dbm.Open(self.rev_prob_path, False, dbm="HashDBM").OrDie()
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False, dbm="HashDBM").OrDie()
    os.makedirs(self.output_path, exist_ok=True)
    aux_trans = self.ReadAuxTrans(self.tran_aux_paths)
    rev_aux_trans = self.ReadAuxTrans(self.rev_tran_aux_paths)
    conj_verbs = self.ReadConjWords(self.conj_verb_path)
    conj_adjs = self.ReadConjWords(self.conj_adj_path)
    word_dict = self.ReadEntries(input_dbm, tran_prob_dbm, aux_trans)
    yomi_map = collections.defaultdict(list)
    keywords = collections.defaultdict(int)
    dubious_yomi_map = collections.defaultdict(list)
    for yomi_path in self.yomi_paths:
      if not yomi_path: continue
      self.ReadYomiMap(yomi_path, yomi_map, keywords, dubious_yomi_map)
    self.AddAuxTrans(word_dict, tran_prob_dbm, aux_trans)
    self.AddKeywords(word_dict, keywords, aux_trans, rev_aux_trans)
    if phrase_prob_dbm and rev_prob_dbm:
      word_dict = self.FilterEntries(word_dict, phrase_prob_dbm, rev_prob_dbm, keywords)
    input_dbm.Close().OrDie()
    yomi_dict = self.MakeYomiDict(word_dict, yomi_map, dubious_yomi_map)
    self.MakeMain(yomi_dict, conj_verbs, conj_adjs, rev_prob_dbm)
    self.MakeNavigation(yomi_dict)
    self.MakeOverview()
    self.MakeStyle()
    self.MakePackage(yomi_dict)
    if phrase_prob_dbm:
      phrase_prob_dbm.Close().OrDie()
    if rev_prob_dbm:
      rev_prob_dbm.Close().OrDie()
    if tran_prob_dbm:
      tran_prob_dbm.Close().OrDie()
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def ReadAuxTrans(self, paths):
    aux_trans = collections.defaultdict(list)
    for path in paths:
      if not path: continue
      with open(path) as input_file:
        for line in input_file:
          fields = line.strip().split("\t")
          if len(fields) <= 2: continue
          word, trans = fields[0], fields[1:]
          aux_trans[word].extend(trans)
    return aux_trans

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

  def ReadEntries(self, input_dbm, tran_prob_dbm, aux_trans):
    logger.info("Reading entries: start")
    word_dict = collections.defaultdict(list)
    it = input_dbm.MakeIterator()
    it.First()
    num_entries = 0
    while True:
      record = it.GetStr()
      if not record: break
      key, serialized = record
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Reading entries: num_enties={}".format(num_entries))
      entry = json.loads(serialized)
      for word_entry in entry:
        self.ReadEntry(word_dict, word_entry, tran_prob_dbm, aux_trans)
      it.Next()
    logger.info("Reading entries: done: {}".format(len(word_dict)))
    return word_dict

  def ReadEntry(self, word_dict, entry, tran_prob_dbm, aux_trans):
    word = entry["word"]
    norm_word = tkrzw_dict.NormalizeWord(word)
    word_prob = float(entry.get("probability") or 0)
    trans = entry.get("translation") or []
    word_aux_trans = aux_trans.get(word)
    if word_aux_trans:
      word_aux_trans = set(word_aux_trans)
      trans.extend(word_aux_trans)
    dict_trans = set()
    for item in entry["item"]:
      label = item["label"]
      text = item["text"]
      if label in self.supplement_labels:
        for tran in text.split(","):
          tran = tran.strip()
          if tran:
            trans.append(tran)
            dict_trans.add(tran)
    tran_probs = {}
    if tran_prob_dbm:
      tsv = tran_prob_dbm.GetStr(norm_word)
      if tsv:
        fields = tsv.split("\t")
        for i in range(0, len(fields), 3):
          src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
          if src != word: continue
          tran_probs[trg] = prob
    word_prob_score = max(0.1, (word_prob ** 0.5))
    rank_score = 0.5
    uniq_trans = set()
    norm_trans = []
    for tran in trans:
      tran = regex.sub("[・]", "", tran).strip()
      if tran and tran not in uniq_trans:
        norm_trans.append(tran)
        uniq_trans.add(tran)
    for i, tran in enumerate(norm_trans):
      if tkrzw_dict.NormalizeWord(tran) == norm_word: continue
      tran_prob = tran_probs.get(tran) or 0
      tran_stem, tran_prefix, tran_suffix = self.tokenizer.StripJaParticles(tran)
      if tran_prefix:
        new_tran = tran_stem + tran_suffix
        new_prob = tran_probs.get(new_tran) or 0
        if (tran_prefix == "を" or regex.search(r"^[\p{Han}\p{Katakana}]", tran_stem) or
            (new_prob >= 0.01 and new_prob >= tran_prob)):
          tran = new_tran
          tran_prob = max(tran_prob, new_prob)
      match = regex.search(
        r"^(.{2,})(する|した|して|している|される|された|されて|されている)$", tran)
      if match:
        new_tran = match.group(1)
        new_prob = tran_probs.get(new_tran) or 0
        if new_prob > tran_prob:
          tran_prob = new_prob
      hit_aux_tran = word_aux_trans and tran in word_aux_trans
      if i == 0:
        pass
      elif i <= 1 and tran_prob >= 0.01:
        pass
      elif i <= 2 and tran_prob >= 0.02:
        pass
      elif i <= 3 and tran_prob >= 0.04:
        pass
      elif tran_prob >= 0.1:
        pass
      elif tran in dict_trans and (i <= 1 or hit_aux_tran):
        pass
      else:
        continue
      tran_prob_score = tran_prob ** 0.75
      dict_score = 0.1 if tran in dict_trans else 0.0
      if hit_aux_tran: dict_score += 0.1
      synsets = []
      for item in entry["item"]:
        if item["label"] != "wn": continue
        texts = item["text"].split(" [-] ")
        synset_id = ""
        gloss = texts[0]
        synonyms = []
        tran_match = False
        for text in texts[1:]:
          match = regex.search(r"^\[(\w+)\]: (.*)", text)
          if not match: continue
          name = match.group(1).strip()
          text = match.group(2).strip()
          if name == "synset":
            synset_id = text
          elif name == "synonym":
            for synonym in text.split(","):
              synonym = synonym.strip()
              if synonym:
                synonyms.append(synonym)
          elif name == "translation":
            for syn_tran in text.split(","):
              syn_tran = syn_tran.strip()
              if syn_tran == tran:
                tran_match = True
        if synset_id and tran_match:
          synsets.append((synset_id, gloss, synonyms))
      if synsets:
        dict_score += 0.1
      score = word_prob_score + rank_score + tran_prob_score + dict_score
      word_dict[tran].append((word, score, tran_prob, synsets))
      rank_score *= 0.8
    phrases = entry.get("phrase") or []
    for phrase in phrases:
      phrase_word = phrase.get("w")
      phrase_prob = float(phrase.get("p") or 0)
      if not phrase_word: continue
      if phrase_prob < 0.005 and not phrase.get("i"): continue
      score = word_prob_score + rank_score
      norm_phrase_word = tkrzw_dict.NormalizeWord(phrase_word)
      phrase_trans = phrase.get("x") or []
      phrase_aux_trans = aux_trans.get(phrase_word)
      if phrase_aux_trans:
        phrase_aux_trans = set(phrase_aux_trans)
        phrase_trans.extend(phrase_aux_trans)
      phrase_tran_probs = {}
      if tran_prob_dbm:
        tsv = tran_prob_dbm.GetStr(norm_phrase_word)
        if tsv:
          fields = tsv.split("\t")
          for i in range(0, len(fields), 3):
            src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
            if src != norm_phrase_word: continue
            phrase_tran_probs[trg] = prob
      norm_phrase_trans = []
      for phrase_tran in phrase.get("x"):
        phrase_tran = regex.sub(r"\(.*?\)", "", phrase_tran).strip()
        phrase_tran = regex.sub("[・]", "", phrase_tran).strip()
        if phrase_tran and phrase_tran not in uniq_trans:
          norm_phrase_trans.append(phrase_tran)
          uniq_trans.add(phrase_tran)
      for i, tran in enumerate(norm_phrase_trans):
        if tkrzw_dict.NormalizeWord(tran) == norm_word: continue
        tran_prob = phrase_tran_probs.get(tran) or 0
        tran_stem, tran_prefix, tran_suffix = self.tokenizer.StripJaParticles(tran)
        if tran_prefix:
          new_tran = tran_stem + tran_suffix
          new_prob = tran_probs.get(new_tran) or 0
          if (tran_prefix == "を" or regex.search(r"^[\p{Han}\p{Katakana}]", tran_stem) or
              (new_prob >= 0.01 and new_prob >= tran_prob)):
            tran = new_tran
            tran_prob = max(tran_prob, new_prob)
        match = regex.search(
          r"^(.{2,})(する|した|して|している|される|された|されて|されている)$", tran)
        if match:
          new_tran = match.group(1)
          new_prob = phrase_tran_probs.get(new_tran) or 0
          if new_prob > tran_prob:
            tran = new_tran
            tran_prob = new_prob
        hit_aux_tran = phrase_aux_trans and tran in phrase_aux_trans
        if i <= 1 and tran_prob >= 0.01:
          pass
        elif i <= 2 and tran_prob >= 0.02:
          pass
        elif i <= 3 and tran_prob >= 0.04:
          pass
        elif tran_prob >= 0.1:
          pass
        elif tran_prob >= 0.01 and hit_aux_tran:
          pass
        else:
          continue
        tran_prob_score = tran_prob ** 0.75
        dict_score = 0.1 if tran in dict_trans else 0.0
        if hit_aux_tran: dict_score += 0.1
        score = word_prob_score + rank_score + tran_prob_score + dict_score
        word_dict[tran].append((phrase_word, score, tran_prob, []))
        rank_score *= 0.95

  def AddAuxTrans(self, word_dict, tran_prob_dbm, aux_trans):
    if not tran_prob_dbm: return
    logger.info("Adding from auxiliary translations")
    count_added = 0
    for word, trans in aux_trans.items():
      norm_word = tkrzw_dict.NormalizeWord(word)
      trans = set(trans)
      tsv = tran_prob_dbm.GetStr(norm_word)
      if not tsv: continue
      tran_probs = {}
      fields = tsv.split("\t")
      for i in range(0, len(fields), 3):
        src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
        if src != word: continue
        tran_probs[trg] = prob
      for tran, tran_prob in tran_probs.items():
        if tran_prob < 0.1: continue
        if tran not in trans: continue
        if tkrzw_dict.NormalizeWord(tran) == norm_word: continue
        tran_stem, tran_prefix, tran_suffix = self.tokenizer.StripJaParticles(tran)
        if tran_prefix:
          new_tran = tran_stem + tran_suffix
          new_prob = tran_probs.get(new_tran) or 0
          if (tran_prefix == "を" or regex.search(r"^[\p{Han}\p{Katakana}]", tran_stem) or
              (new_prob >= 0.01 and new_prob >= tran_prob)):
            tran = new_tran
            tran_prob = max(tran_prob, new_prob)
        score = tran_prob ** 0.5
        word_dict[tran].append((word, score, tran_prob, []))
        count_added += 1
    logger.info("Adding from auxiliary translations: done: {}".format(count_added))

  def AddKeywords(self, word_dict, keywords, aux_trans, rev_aux_trans):
    logger.info("Adding from keywords")
    count_added = 0
    inv_aux_trans = collections.defaultdict(list)
    for word, trans in aux_trans.items():
      for tran in trans:
        inv_aux_trans[tran].append(word)
    for tran, count in keywords.items():
      if count < 3: continue
      if tran in word_dict: continue
      words = inv_aux_trans.get(tran) or []
      words.extend(rev_aux_trans.get(tran) or [])
      base_score = 1.0
      if words:
        for word in words[:2]:
          word_dict[tran].append((word, count * base_score * 0.01, 0.01, []))
          base_score *= 0.8
        count_added += 1
    logger.info("Adding from keywords: done: {}".format(count_added))

  def FilterEntries(self, word_dict, phrase_prob_dbm, rev_prob_dbm, keywords):
    logger.info("Filtering entries: before={}".format(len(word_dict)))
    new_word_dict1 = collections.defaultdict(list)
    num_entries = 0
    count_ok_keyword = 0
    count_ok_tran_only = 0
    count_ok_word_only = 0
    count_ok_word_tran = 0
    count_ok_phrase_tran = 0
    count_ng = 0
    for word, items in word_dict.items():
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Filtering entries R1: num_enties={}".format(num_entries))
      word_prob = self.GetPhraseProb(rev_prob_dbm, "ja", word)
      max_tran_prob = 0
      max_phrase_prob = 0
      new_items = []
      for tran, score, tran_prob, synsets in items:
        max_tran_prob = max(max_tran_prob, tran_prob)
        phrase_prob = self.GetPhraseProb(phrase_prob_dbm, "en", tran)
        max_phrase_prob = max(max_phrase_prob, phrase_prob)
        score += min(0.2, phrase_prob ** 0.33)
        new_items.append((tran, score, tran_prob, synsets))
      if word in keywords:
        count_ok_keyword += 1
      elif max_tran_prob >= 0.2:
        count_ok_tran_only += 1
      elif word_prob >= 0.00001:
        count_ok_word_only += 1
      elif word_prob >= 0.000001 and max_tran_prob >= 0.02:
        count_ok_word_tran += 1
      elif max_phrase_prob >= 0.00001 and max_tran_prob >= 0.02:
        count_ok_phrase_tran += 1
      else:
        count_ng += 1
        continue
      new_word_dict1[word].extend(new_items)
    logger.info("Filtering entries R1 done: "
                "after={}, k={}, t={}, w={}, wt={}, pt={}, ng={}".format(
                  len(new_word_dict1), count_ok_keyword, count_ok_tran_only, count_ok_word_only,
                  count_ok_word_tran, count_ok_phrase_tran, count_ng))
    new_word_dict2 = collections.defaultdict(list)
    count_dup_affix = 0
    for word, items in new_word_dict1.items():
      num_entries += 1
      if num_entries % 10000 == 0:
        logger.info("Filtering entries R2: num_enties={}".format(num_entries))
      stems = set()
      stem, prefix, suffix = self.tokenizer.StripJaParticles(word)
      if stem != word:
        stems.add(stem)
      match = regex.search(
        r"^(.{2,})(する|した|して|している|される|された|されて|されている)$", word)
      if match:
        stems.add(match.group(1))
      stem_trans = set()
      for stem in stems:
        stem_items = new_word_dict1.get(stem)
        if stem_items:
          for stem_tran, _, _, _ in stem_items:
            stem_trans.add(stem_tran.lower())
      if stem_trans:
        has_unique = False
        for tran, _, _, _ in items:
          if tran.lower() not in stem_trans:
            has_unique = True
        if not has_unique:
          count_dup_affix += 1
          continue
      new_word_dict2[word].extend(items)
    logger.info("Filtering entries R2 done: after={}, da={}".format(
      len(new_word_dict2), count_dup_affix))
    return new_word_dict2

  def MakeYomiDict(self, word_dict, yomi_map, dubious_yomi_map):
    yomi_dict = collections.defaultdict(list)
    for word, items in word_dict.items():
      word_yomi = ""
      part_yomis = yomi_map.get(word)
      dubious_part_yomis = dubious_yomi_map.get(word)
      dubious_word_yomi = ""
      if part_yomis:
        if len(part_yomis) == 1 and dubious_part_yomis and part_yomis[0] in dubious_part_yomis:
          dubious_word_yomi = part_yomis[0]
        else:
          word_yomi = self.ChooseBestYomi(word, part_yomis, False)
      if not word_yomi:
        trg_word = word
        stem, prefix, suffix = self.tokenizer.StripJaParticles(word)
        if stem != word:
          part_yomis = yomi_map.get(stem)
          if part_yomis:
            part_yomis = [prefix + x + suffix for x in part_yomis]
            trg_word = self.ChooseBestYomi(word, part_yomis, True)
        match = regex.search(
          "^([\p{Han}]{2,})(する|して|される|されて|にする|できる|できない|のない"
          "を|に|が|へ|や|の|と|から|で|より|な)$", word)
        if match:
          stem = match.group(1)
          suffix = match.group(2)
          part_yomis = yomi_map.get(stem)
          dubious_part_yomis = dubious_yomi_map.get(stem)
          if part_yomis and (len(part_yomis) > 1 or not dubious_part_yomis or
                             part_yomis[0] not in dubious_part_yomis):
            part_yomis = [prefix + x + suffix for x in part_yomis]
            trg_word = self.ChooseBestYomi(word, part_yomis, True)
        if dubious_word_yomi and not regex.fullmatch(r"[\p{Hiragana}ー]+", trg_word):
          word_yomi = dubious_word_yomi
        else:
          word_yomi = self.tokenizer.GetJaYomi(trg_word)
      if not word_yomi: continue
      word_yomi_key = MakeYomiKey(word_yomi)
      first = word_yomi_key[0]
      if regex.search(r"^[\p{Hiragana}]", first):
        yomi_dict[first].append((word_yomi_key, word_yomi, word, items))
      else:
        yomi_dict["他"].append((word_yomi_key, word_yomi, word, items))
    sorted_yomi_dict = []
    for first, items in sorted(yomi_dict.items()):
      items = sorted(items)
      dedup_items = []
      i = 0
      while i < len(items):
        _, word_yomi, word, yomi_items = items[i]
        if regex.search(r"\p{Hiragana}", word) and not regex.search(r"\p{Han}", word):
          uniq_items = []
          for yomi_item in yomi_items:
            is_dup = False
            j = i + 1
            while j < len(items):
              _, next_word_yomi, next_word, next_yomi_items = items[j]
              if next_word_yomi != word_yomi: break
              for next_yomi_item in next_yomi_items:
                if yomi_item[0] == next_yomi_item[0]:
                  is_dup = True
                for yomi_item_synset in yomi_item[3]:
                  for next_yomi_item_synset in next_yomi_item[3]:
                    if yomi_item_synset[0] == next_yomi_item_synset[0]:
                      is_dup = True
                    if yomi_item[0] in next_yomi_item_synset[2]:
                      is_dup = True
                  if next_yomi_item[0] in yomi_item_synset[2]:
                    is_dup = True
              j += 1
            if not is_dup:
              uniq_items.append(yomi_item)
          yomi_items = uniq_items
        if yomi_items:
          dedup_items.append((word_yomi, word, yomi_items))
        i += 1
      sorted_yomi_dict.append((first, dedup_items))
    return sorted_yomi_dict

  def ReadYomiMap(self, path, yomi_map, keywords, dubious_yomi_map):
    has_keyword = False
    is_dubious = False
    if path.startswith("+"):
      path = path[1:]
      has_keyword = True
    if path.startswith("-"):
      path = path[1:]
      is_dubious = True
    with open(path) as input_file:
      for line in input_file:
        fields = line.strip().split("\t")
        if len(fields) < 2: continue
        kanji = fields[0]
        yomis = []
        for yomi in fields[1:]:
          if not regex.fullmatch(r"[\p{Hiragana}ー]+", yomi) or len(yomi) > 30: continue
          yomis.append(yomi)
          if is_dubious: break
        yomi_map[kanji].extend(yomis)
        if has_keyword:
          keywords[kanji] += 1
        if is_dubious:
          dubious_yomi_map[kanji].extend(yomis)

  def ChooseBestYomi(self, word, yomis, sort_by_length):
    if len(yomis) == 1:
      return yomis[0]
    yomis = yomis + [self.tokenizer.GetJaYomi(word)]
    counts = {}
    i = 0
    while i < len(yomis):
      yomi = yomis[i]
      score = len(yomi) if sort_by_length else i
      values = counts.get(yomi) or [0, score]
      values[0] += 1
      counts[yomi] = values
      i += 1
    counts = sorted(counts.items(), key=lambda x: (x[1][0], -x[1][-1]), reverse=True)
    return counts[0][0]

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

  def MakeMain(self, yomi_dict, conj_verbs, conj_adjs, rev_prob_dbm):
    page_id = 0
    for first, items in yomi_dict:
      page_id += 1
      page_path = os.path.join(self.output_path, "main-{:02d}.xhtml".format(page_id))
      logger.info("Creating: {}".format(page_path))
      with open(page_path, "w") as out_file:
        print(MAIN_HEADER_TEXT.format(esc(self.title), esc(first), esc(first)),
              file=out_file, end="")
        for item in items:
          self.MakeMainEntry(out_file, item, conj_verbs, conj_adjs, rev_prob_dbm)
        print(MAIN_FOOTER_TEXT, file=out_file, end="")

  def MakeMainEntry(self, out_file, entry, conj_verbs, conj_adjs, rev_prob_dbm):
    def P(*args, end="\n"):
      esc_args = []
      for arg in args[1:]:
        if isinstance(arg, str):
          arg = esc(arg)
        esc_args.append(arg)
      print(args[0].format(*esc_args), end=end, file=out_file)
    self.num_words += 1
    yomi, word, trans = entry
    variants = {}
    variants[yomi] = True
    pos = self.tokenizer.GetJaLastPos(word)
    word_prob = 0
    if rev_prob_dbm:
      word_prob = self.GetPhraseProb(rev_prob_dbm, "ja", word)
    if word.endswith(pos[3]):
      prefix = word[:-len(pos[3])]
      for focus_pos, conj_map in [("動詞", conj_verbs), ("形容詞", conj_adjs)]:
        if pos[1] != focus_pos: continue
        conjs = conj_map.get(word)
        if prefix and not conjs and word_prob >= 0.00001:
          part_conjs = conj_map.get(pos[3])
          if part_conjs:
            conjs = [prefix + x for x in part_conjs]
        if conjs:
          for conj in sorted(conjs):
            variants[conj] = True
    stem, prefix, suffix = self.tokenizer.StripJaParticles(word)
    if stem != word:
      if prefix == "を" or regex.search(r"[\p{Han}\p{Katakana}]", stem):
        prefix = ""
      new_word = prefix + stem
      variants[new_word] = True
    for suffix in ("する", "した", "される", "された"):
      if word.endswith(suffix):
        stem = word[:-len(suffix)]
        if self.tokenizer.IsJaWordSahenNoun(stem):
          variants[stem] = True
    for suffix in ("な", "に", "と"):
      if word.endswith(suffix):
        stem = word[:-len(suffix)]
        if self.tokenizer.IsJaWordAdjvNoun(stem):
          variants[stem] = True
    if word in variants:
      del variants[word]
    trans = sorted(trans, key=lambda x: x[1], reverse=True)
    P('<idx:entry>')
    P('<div>')
    P('<span class="word">')
    P('<idx:orth>{}', word)
    if variants:
      P('<idx:infl>')
      for variant, _ in variants.items():
        P('<idx:iform value="{}"/>', variant)
      P('</idx:infl>')
    P('</idx:orth>')
    P('</span>')
    if yomi != word:
      P('&#x2003;<span class="pron">({})</span>', yomi)
    P('</div>')
    uniq_trans = set()
    uniq_synsets = set()
    misc_trans = []
    for tran, score, tran_prob, synsets in trans[:8]:
      norm_tran = tkrzw_dict.NormalizeWord(tran)
      if norm_tran in uniq_trans: continue
      uniq_trans.add(norm_tran)
      self.num_items += 1
      hit_syn = False
      for syn_id, syn_gloss, syn_words in synsets:
        if syn_id in uniq_synsets: continue
        uniq_synsets.add(syn_id)
        hit_syn = True
        P('<div>{}', ", ".join([tran] + syn_words), end="")
        P(' <span class="gloss">- {}</span>', syn_gloss, end="")
        P('</div>')
        for synonym in syn_words:
          norm_syn = tkrzw_dict.NormalizeWord(synonym)
          uniq_trans.add(norm_syn)
      if not hit_syn:
        misc_trans.append(tran)
    if misc_trans:
      P('<div>{}</div>', ', '.join(misc_trans[:8]))
    P('</idx:entry>')
    P('<br/>')

  def MakeNavigation(self, yomi_dict):
    out_path = os.path.join(self.output_path, "nav.xhtml")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(NAVIGATION_HEADER_TEXT.format(esc(self.title), esc(self.title)),
            file=out_file, end="")
      page_id = 0
      for first, items in yomi_dict:
        page_id += 1
        page_path = "main-{:02d}.xhtml".format(page_id)
        print('<li><a href="{}">Words: {}</a></li>'.format(esc(page_path), esc(first)),
              file=out_file)
      print(NAVIGATION_FOOTER_TEXT, file=out_file, end="")

  def MakeOverview(self):
    out_path = os.path.join(self.output_path, "overview.xhtml")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(OVERVIEW_TEXT.format(esc(self.title), self.num_words, self.num_items),
            file=out_file, end="")

  def MakeStyle(self):
    out_path = os.path.join(self.output_path, "style.css")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(STYLE_TEXT, file=out_file, end="")

  def MakePackage(self, yomi_dict):
    out_path = os.path.join(self.output_path, "package.opf")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(PACKAGE_HEADER_TEXT.format(CURRENT_UUID, esc(self.title), CURRENT_DATETIME),
            file=out_file, end="")
      page_id = 0
      for first, items in yomi_dict:
        page_id += 1
        page_path = "main-{:02d}.xhtml".format(page_id)
        print('<item id="page{:02d}" href="{}" media-type="application/xhtml+xml"/>'.format(
          page_id, page_path), file=out_file)
      print(PACKAGE_MIDDLE_TEXT, file=out_file, end="")
      for i in range(1, page_id + 1):
        print('<itemref idref="page{:02d}"/>'.format(i), file=out_file)
      print(PACKAGE_FOOTER_TEXT, file=out_file, end="")


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-dict-jaen-kindle"
  supplement_labels = set((tkrzw_dict.GetCommandFlag(args, "--supplement", 1) or "xs").split(","))
  tran_prob_path = tkrzw_dict.GetCommandFlag(args, "--tran_prob", 1) or ""
  phrase_prob_path = tkrzw_dict.GetCommandFlag(args, "--phrase_prob", 1) or ""
  rev_prob_path = tkrzw_dict.GetCommandFlag(args, "--rev_prob", 1) or ""
  yomi_paths = (tkrzw_dict.GetCommandFlag(args, "--yomi", 1) or "").split(",")
  tran_aux_paths = (tkrzw_dict.GetCommandFlag(args, "--tran_aux", 1) or "").split(",")
  rev_tran_aux_paths = (tkrzw_dict.GetCommandFlag(args, "--rev_tran_aux", 1) or "").split(",")
  conj_verb_path = tkrzw_dict.GetCommandFlag(args, "--conj_verb", 1)
  conj_adj_path = tkrzw_dict.GetCommandFlag(args, "--conj_adj", 1)
  title = tkrzw_dict.GetCommandFlag(args, "--title", 1) or "Union Japanese-English Dictionary"
  if not input_path:
    raise RuntimeError("an input path is required")
  if not output_path:
    raise RuntimeError("an output path is required")
  GenerateUnionEPUBBatch(
    input_path, output_path, supplement_labels, tran_prob_path, phrase_prob_path, rev_prob_path,
    yomi_paths, tran_aux_paths, rev_tran_aux_paths, conj_verb_path, conj_adj_path, title).Run()


if __name__=="__main__":
  main()
