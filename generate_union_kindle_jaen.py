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
PACKAGE_HEADER_TEXT = """<?xml version="1.0" encoding="utf-8"?>
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
STYLE_TEXT = """html,body { margin: 0; padding: 0; background: #fff; color: #000; font-size: 12pt; }
span.word { font-weight: bold; }
span.pron { font-size: 90%; color: #444; }
span.gross { font-size: 90%; color: #444; }
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
OVERVIEW_TEXT = """
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops" lang="ja">
<head>
<title>{}: Overview</title>
<link rel="stylesheet" href="style.css"/>
</head>
<body>
<article>
<h2>Overview</h2>
<p>This dictionary is made from data sources published as open-source data.  It uses <a href="https://wordnet.princeton.edu/">WordNet</a>, <a href="http://compling.hss.ntu.edu.sg/wnja/index.en.html">Japanese WordNet</a>, <a href="https://ja.wiktionary.org/">Japanese Wiktionary</a>, <a href="https://en.wiktionary.org/">English Wiktionary</a>, and <a href="http://www.edrdg.org/jmdict/edict.html">EDict2</a>.  See <a href="https://dbmx.net/dict/">the homepage</a> for details to organize the data.  Using and/or redistributing this data should be done according to the license of each data source.</p>
<p>In each word entry, the title word is shown in bold.  Some words have a pronounciation expression in hiragana, bracketed as "(...)".  A list of translation can come next.  Some have definitions of the words in English.</p>
<p>The number of words is {}.  The number of items is {}.</p>
<h2>Copyright</h2>
<div>WordNet Copyright© 2021 The Trustees of Princeton University.</div>
<div>Japanese Wordnet Copyright© 2009-2011 NICT, 2012-2015 Francis Bond and 2016-2017 Francis Bond, Takayuki Kuribayashi.</div>
<div>Wiktionary data is copyrighted by each contributers and licensed under CC BY-SA and GFDL.</div>
<div>EDict2 Copyright© 2017 The Electronic Dictionary Research and Development Group.</div>
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
"""
MAIN_FOOTER_TEXT = """</mbp:frameset>
</body>
</html>
"""


def esc(expr):
  if expr is None:
    return ""
  return html.escape(str(expr), True)


class GenerateUnionEPUBBatch:
  def __init__(self, input_path, output_path, supplement_labels,
               tran_prob_path, yomi_path, tran_aux_paths, title):
    self.input_path = input_path
    self.output_path = output_path
    self.supplement_labels = supplement_labels
    self.tran_prob_path = tran_prob_path
    self.yomi_path = yomi_path
    self.tran_aux_paths = tran_aux_paths
    self.title = title
    self.tokenizer = tkrzw_tokenizer.Tokenizer()
    self.num_words = 0
    self.num_items = 0

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}".format(
      str(self.input_path), self.output_path))
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False, dbm="HashDBM").OrDie()
    os.makedirs(self.output_path, exist_ok=True)
    tran_prob_dbm = None
    if self.tran_prob_path:
      tran_prob_dbm = tkrzw.DBM()
      tran_prob_dbm.Open(self.tran_prob_path, False, dbm="HashDBM").OrDie()
    aux_trans = self.ReadAuxTrans()
    word_dict = self.ReadEntries(input_dbm, tran_prob_dbm, aux_trans)
    self.AddAuxTrans(word_dict, tran_prob_dbm, aux_trans)
    if tran_prob_dbm:
      tran_prob_dbm.Close().OrDie()
    input_dbm.Close().OrDie()
    yomi_dict = self.MakeYomiDict(word_dict)
    self.MakeMain(yomi_dict)
    self.MakeNavigation(yomi_dict)
    self.MakeOverview()
    self.MakeStyle()
    self.MakePackage(yomi_dict)
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def ReadAuxTrans(self):
    aux_trans = collections.defaultdict(list)
    for path in self.tran_aux_paths:
      if not path: continue
      with open(path) as input_file:
        for line in input_file:
          fields = line.strip().split("\t")
          if len(fields) <= 2: continue
          word, trans = fields[0], fields[1:]
          aux_trans[word].extend(trans)
    return aux_trans

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
    logger.info("Reading entries: done")
    return word_dict

  def ReadEntry(self, word_dict, entry, tran_prob_dbm, aux_trans):
    word = entry["word"]
    word_prob = float(entry.get("probability") or 0)
    trans = entry.get("translation")
    if not trans: return
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
      key = tkrzw_dict.NormalizeWord(word)
      tsv = tran_prob_dbm.GetStr(key)
      if tsv:
        fields = tsv.split("\t")
        for i in range(0, len(fields), 3):
          src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
          if src != word: continue
          tran_probs[trg] = prob
    word_prob_score = max(0.1, (word_prob ** 0.5))
    rank_score = 0.5
    for i, tran in enumerate(trans):
      tran_prob = tran_probs.get(tran) or 0
      tran_stem, tran_prefix, tran_suffix = self.tokenizer.StripJaParticles(tran)
      if tran_prefix:
        new_tran = tran_stem + tran_suffix
        new_prob = tran_probs.get(new_tran) or 0
        if (tran_prefix == "を" or regex.search(r"^[\p{Han}\p{Katakana}]", tran_stem) or
            (new_prob >= 0.01 and new_prob >= tran_prob)):
          tran = new_tran
          tran_prob = max(tran_prob, new_prob)
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
      elif tran in dict_trans:
        pass
      else:
        continue
      tran_prob_score = tran_prob ** 0.75
      dict_score = 0.1 if tran in dict_trans else 0.0
      if word_aux_trans and tran in word_aux_trans: dict_score += 0.1
      score = word_prob_score + rank_score + tran_prob_score + dict_score
      synsets = []
      for item in entry["item"]:
        if item["label"] != "wn": continue
        texts = item["text"].split(" [-] ")
        synset_id = ""
        gross = texts[0]
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
          synsets.append((synset_id, gross, synonyms))
      word_dict[tran].append((word, score, synsets))
      rank_score *= 0.8

  def AddAuxTrans(self, word_dict, tran_prob_dbm, aux_trans):
    if not tran_prob_dbm: return
    for word, trans in aux_trans.items():
      trans = set(trans)
      key = tkrzw_dict.NormalizeWord(word)
      tsv = tran_prob_dbm.GetStr(key)
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
        tran_stem, tran_prefix, tran_suffix = self.tokenizer.StripJaParticles(tran)
        if tran_prefix:
          new_tran = tran_stem + tran_suffix
          new_prob = tran_probs.get(new_tran) or 0
          if (tran_prefix == "を" or regex.search(r"^[\p{Han}\p{Katakana}]", tran_stem) or
              (new_prob >= 0.01 and new_prob >= tran_prob)):
            tran = new_tran
            tran_prob = max(tran_prob, new_prob)
        score = tran_prob ** 0.5
        word_dict[tran].append((word, score, []))

  def MakeYomiDict(self, word_dict):
    yomi_map = {}
    if self.yomi_path:
      with open(self.yomi_path) as input_file:
        for line in input_file:
          fields = line.strip().split("\t")
          if len(fields) != 2: continue
          kanji, yomi = fields
          yomi_map[kanji] = yomi
    yomi_dict = collections.defaultdict(list)
    for word, items in word_dict.items():
      yomi = yomi_map.get(word)
      if not yomi:
        yomi = self.tokenizer.GetJaYomi(word)
      if not yomi: continue
      first = yomi[0]
      if regex.search(r"^[\p{Hiragana}]", first):
        yomi_dict[first].append((yomi, word, items))
      else:
        yomi_dict["他"].append((yomi, word, items))
    sorted_yomi_dict = []
    for first, items in sorted(yomi_dict.items()):
      items = sorted(items)
      sorted_yomi_dict.append((first, items))
    return sorted_yomi_dict

  def MakeMain(self, yomi_dict):
    page_id = 0
    for first, items in yomi_dict:
      page_id += 1
      page_path = os.path.join(self.output_path, "main-{:02d}.xhtml".format(page_id))
      logger.info("Creating: {}".format(page_path))
      with open(page_path, "w") as out_file:
        print(MAIN_HEADER_TEXT.format(esc(self.title), esc(first), esc(first)),
              file=out_file, end="")
        for item in items:
          self.MakeMainEntry(out_file, item)
        print(MAIN_FOOTER_TEXT, file=out_file, end="")

  def MakeMainEntry(self, out_file, entry):
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
    P('<div class="head">')
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
    num_lines = 0
    for tran, score, synsets in trans:
      norm_tran = tkrzw_dict.NormalizeWord(tran)
      if norm_tran in uniq_trans: continue
      uniq_trans.add(norm_tran)
      self.num_items += 1
      hit_syn = False
      for syn_id, syn_gross, syn_words in synsets:
        if syn_id in uniq_synsets: continue
        uniq_synsets.add(syn_id)
        hit_syn = True
        P('<div>{}', ", ".join([tran] + syn_words), end="")
        P(' <span class="gross">- {}</span>', syn_gross, end="")
        P('</div>')
        num_lines += 1
        for synonym in syn_words:
          norm_syn = tkrzw_dict.NormalizeWord(synonym)
          uniq_trans.add(norm_syn)
      if not hit_syn and num_lines < 8:
        P('<div>{}</div>', tran)
        num_lines += 1
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
  yomi_path = tkrzw_dict.GetCommandFlag(args, "--yomi", 1) or ""
  tran_aux_paths = (tkrzw_dict.GetCommandFlag(args, "--tran_aux", 1) or "").split(",")
  tkrzw_dict.GetCommandFlag(args, "--tran_aux", 1) or ""
  title = tkrzw_dict.GetCommandFlag(args, "--title", 1) or "Union Japanese-English Dictionary"
  if not input_path:
    raise RuntimeError("an input path is required")
  if not output_path:
    raise RuntimeError("an output path is required")
  GenerateUnionEPUBBatch(input_path, output_path, supplement_labels,
                         tran_prob_path, yomi_path, tran_aux_paths, title).Run()


if __name__=="__main__":
  main()
