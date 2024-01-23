#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to generate files to make a EnJa Kindle dictionary from the union dictionary
#
# Usage:
#   generate_union_kindle_enja.py [--input str] [--output str] [--keyword str] [--quiet]
#
# Example:
#   ./generate_union_kindle_enja.py --input union-body.tkh --output union-dict-epub
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
POSES = {
  "noun": "名",
  "verb": "動",
  "adjective": "形",
  "adverb": "副",
  "pronoun": "代名",
  "auxverb": "助動",
  "preposition": "前置",
  "determiner": "限定",
  "article": "冠",
  "interjection": "間投",
  "conjunction": "接続",
  "prefix": "接頭",
  "suffix": "接尾",
  "abbreviation": "省略",
  "phrase": "句",
  "misc": "他",
}
INFLECTIONS = [
  [("noun_plural", "複数")],
  [("verb_singular", "三単"),
   ("verb_present_participle", "現分"),
   ("verb_past", "過去"),
   ("verb_past_participle", "過分")],
  [("adjective_comparative", "形比"),
   ("adjective_superlative", "形最")],
  [("adverb_comparative", "副比"),
   ("adverb_superlative", "副最")]]
WORDNET_ATTRS = {
  "translation": "翻訳",
  "synonym": "同義",
  "antonym": "対義",
  "hypernym": "上位",
  "hyponym": "下位",
  "holonym": "全体",
  "meronym": "部分",
  "attribute": "属性",
  "derivative": "派生",
  "entailment": "随伴",
  "cause": "原因",
  "seealso": "参考",
  "group": "集合",
  "similar": "類義",
  "perticiple": "分詞",
  "pertainym": "関連",
  "topic": "話題",
  "region": "地域",
  "usage": "用法",
}
TEXT_ATTRS = {
  "可算": "C",
  "不可算": "U",
  "自動詞": "自",
  "他動詞": "他",
  "countable": "C",
  "uncountable": "U",
  "intransitive": "自",
  "transitive": "他",
  "derivative": "派",
}
ARTICLES = {
  "a", "the", "an",
}
PARTICLES = {
  "aback", "about", "above", "abroad", "across", "after", "against", "ahead", "along",
  "amid", "among", "apart", "around", "as", "at", "away", "back", "before", "behind",
  "below", "beneath", "between", "beside", "beyond", "by", "despite", "during", "down",
  "except", "for", "forth", "from", "in", "inside", "into", "near", "of", "off", "on",
  "onto", "out", "outside", "over", "per", "re", "since", "than", "through", "throughout",
  "till", "to", "together", "toward", "under", "until", "up", "upon", "with", "within",
  "without", "via",
}
BASE_VETTED_VERBS = {
  "be", "have", "see",
}
CURRENT_UUID = str(uuid.uuid1())
CURRENT_DATETIME = regex.sub(r"\..*", "Z", datetime.datetime.now(
  datetime.timezone.utc).isoformat())
PACKAGE_HEADER_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<package unique-identifier="pub-id" version="3.0" xmlns="http://www.idpf.org/2007/opf" xml:lang="ja">
<metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
<dc:identifier id="pub-id">urn:uuid:{}</dc:identifier>
<dc:publisher>dbmx.net</dc:publisher>
<dc:title>{}</dc:title>
<dc:language>en</dc:language>
<dc:language>ja</dc:language>
<dc:type id="tp">dictionary</dc:type>
<meta property="dcterms:modified">{}</meta>
<meta property="dcterms:type" refines="#tp">bilingual</meta>
<meta property="source-language">en</meta>
<meta property="target-language">ja</meta>
<x-metadata>
<DictionaryInLanguage>en</DictionaryInLanguage>
<DictionaryOutLanguage>ja</DictionaryOutLanguage>
<DefaultLookupIndex>en</DefaultLookupIndex>
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
span.word { font-weight: bold; }
span.pron { font-size: 90%; color: #444; }
span.pos,span.attr { font-size: 80%; color: #555; word-spacing: 0; letter-spacing: 0; }
span.exja { font-size: 85%; color: #444; }
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
<p>In each word entry, the title word is shown in bold.  Some words have a pronounciation expression in the IPA format, bracketed as "/.../".  A list of translations can come next.  Then, definitions of the word come in English or Japanese.  Each definition is led by a part of speech label.  Additional information such as inflections and varints can come next.</p>
<p>The number of words is {}.  The number of words with translations is {}.  The number of definition items is {}.  The number of inflections is {}.  The number of redirections is {}.  The number of collocations is {}.</p>
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


def esc(expr):
  if expr is None:
    return ""
  return html.escape(str(expr), True)


def CutTextByWidth(text, width):
  result = ""
  for c in text:
    if width < 0:
      is_cut = False
      i = len(result) - 1
      word_head_min = max(i - 40, 20)
      while i >= word_head_min:
        if result[i] == ";":
          result = result[:i].strip()
          is_cut = True
          break
        i -= 1
      if not is_cut:
        i = len(result) - 1
        word_head_min = max(i - 20, 20)
        while i >= word_head_min:
          if not regex.search(r"[-_\p{Latin}]", result[i]):
            result = result[:i].strip()
            break
          i -= 1
        result += "..."
      break
    result += c
    width -= 2 if ord(c) > 256 else 1
  return result


def ConvertWordToID(word):
  word = word.replace(" ", "_")
  word = urllib.parse.quote(word)
  word = word.replace("%", "~")
  return word


def GetKeyPrefix(key):
  if key[0] < "a" or key[0] > "z":
    return "_"
  prefix = key[0]
  return regex.sub(r"[^a-zA-Z0-9]", "_", prefix)


_regex_nonsafe_text = regex.compile(
  r"[^\x20-\x7F\p{Han}\p{Hiragana}\p{Katakana}ー・、。々]")
_regex_nonsafe_ascii_symbols = regex.compile(r"[\\{}`]")
def CheckSafeText(text):
  if regex.search(_regex_nonsafe_text, text):
    return False
  if regex.search(_regex_nonsafe_ascii_symbols, text):
    return False
  return True


_regex_invalid_scripts = regex.compile(
  r"[^\s\p{Common}\p{Latin}\p{Cyrillic}\p{Greek}\p{Runic}" +
  r"\p{Han}\p{Hangul}\p{Hiragana}\p{Katakana}ー]")
_regex_nonbasic_scripts = regex.compile(r"[^\u0000-\uFFFF]")
_regex_space_scripts = regex.compile(r"[\s\p{C}]+")
def SanitizeText(text):
  text = regex.sub(_regex_invalid_scripts, "□", text)
  text = regex.sub(_regex_nonbasic_scripts, "□", text)
  text = regex.sub(_regex_space_scripts, " ", text).strip()
  return text


class GenerateUnionEPUBBatch:
  def __init__(self, input_path, output_path, keyword_path,
               best_labels, vetted_labels, preferable_labels, trustable_labels,
               supplement_labels, title,
               min_prob_normal, min_prob_capital, min_prob_multi, sufficient_prob,
               shrink, fallback, example_only):
    self.input_path = input_path
    self.output_path = output_path
    self.keyword_path = keyword_path
    self.best_labels = best_labels
    self.vetted_labels = vetted_labels
    self.preferable_labels = preferable_labels
    self.trustable_labels = trustable_labels
    self.supplement_labels = supplement_labels
    self.title = title
    self.min_prob_normal = min_prob_normal
    self.min_prob_capital = min_prob_capital
    self.min_prob_multi = min_prob_multi
    self.sufficient_prob = sufficient_prob
    self.shrink = shrink
    self.fallback = fallback
    self.example_only = example_only
    self.num_words = 0
    self.num_trans = 0
    self.num_items = 0
    self.num_aux_items = 0
    self.num_inflections = 0
    self.num_redirections = 0
    self.num_collocations = 0
    self.label_counters = collections.defaultdict(int)
    self.tokenizer = tkrzw_tokenizer.Tokenizer()

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}".format(
      str(self.input_path), self.output_path))
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False, dbm="HashDBM").OrDie()
    os.makedirs(self.output_path, exist_ok=True)
    etym_dict = collections.defaultdict(list)
    infl_dict = collections.defaultdict(list)
    words = self.ListUpWords(input_dbm, etym_dict, infl_dict)
    keys = sorted(set([tkrzw_dict.NormalizeWord(word) for word, prob in words.items()]))
    key_prefixes = set()
    for key in keys:
      key_prefixes.add(GetKeyPrefix(key))
    key_prefixes = sorted(list(key_prefixes), key=lambda x: 1000 if x == "_" else ord(x))
    self.MakeMain(input_dbm, keys, words, etym_dict, infl_dict)
    self.MakeNavigation(key_prefixes)
    self.MakeOverview()
    self.MakeStyle()
    self.MakePackage(key_prefixes)
    input_dbm.Close().OrDie()
    for label, count in self.label_counters.items():
      logger.info("Adopted label: {}: {}".format(label, count))
    logger.info("Stats: words={}, trans={}, items={}, aux_items={}, infls={}, redirs={}".format(
      self.num_words, self.num_trans, self.num_items, self.num_aux_items,
      self.num_inflections, self.num_redirections))
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def ListUpWords(self, input_dbm, etym_dict, infl_dict):
    logger.info("Checking words")
    keywords = set()
    if self.keyword_path:
      with open(self.keyword_path) as input_file:
        for line in input_file:
          line = line.strip()
          if line:
            keywords.add(line)
    words = {}
    it = input_dbm.MakeIterator()
    it.First()
    while True:
      rec = it.GetStr()
      if rec == None: break
      entries = json.loads(rec[1])
      for entry in entries:
        word = entry["word"]
        core = entry.get("etymology_core")
        if core:
          etym_dict[core].append(word)
        for attr_list in INFLECTIONS:
          for name, label in attr_list:
            infls = entry.get(name)
            if infls:
              for infl in infls[:1]:
                infl_dict[word].append(infl)
        if not self.IsGoodEntry(entry, input_dbm, keywords): continue
        prob = float(entry.get("probability") or "0")
        words[word] = max(words.get(word) or 0.0, prob)
      it.Next()
    logger.info("Checking words done: {}/{}".format(len(words), input_dbm.Count()))
    return words

  def IsGoodEntry(self, entry, input_dbm, keywords):
    word = entry["word"]
    if regex.search(r"[^\x20-\x7F\p{Latin}]", word):
      return False
    if regex.fullmatch(r"\d+(th)?", word):
      return False
    if self.fallback:
      return True
    prob = float(entry.get("probability") or "0")
    poses = set()
    labels = set()
    for item in entry["item"]:
      poses.add(item["pos"])
      if item["text"].startswith("[translation]:"): continue
      labels.add(item["label"])
    has_good_label = False
    for label in labels:
      if label in self.trustable_labels:
        return True
      if label not in self.supplement_labels:
        has_good_label = True
    if not has_good_label:
      return False
    if word in keywords:
      return True
    if regex.search(r"[A-Z]", word) and prob < self.min_prob_capital:
      return False
    if word.find(" ") >= 0 and prob < self.min_prob_multi:
      return False
    if prob < self.min_prob_normal:
      return False
    if prob >= self.sufficient_prob:
      return True
    if "verb" in poses and regex.fullmatch(r"[a-z ]+", word):
      tokens = word.split(" ")
      if len(tokens) >= 2 and tokens[0] in keywords:
        particle_suffix = True
        for token in tokens[1:]:
          if not token in PARTICLES:
            particle_suffix = False
            break
        if particle_suffix:
          return True
    translations = entry.get("translation")
    if translations and len(labels) >= 2 and not self.example_only:
      if "verb" in poses or "adjective" in poses or "adverb" in poses:
        return True
      if regex.fullmatch("[a-z]+", word) and "we" in labels:
        return True
    has_parent = False
    parents = entry.get("parent")
    if parents:
      for parent in parents:
        parent_entry = input_dbm.Get(parent)
        if not parent_entry: return
        parent_entries = json.loads(parent_entry)
        for parent_entry in parent_entries:
          match_infl = False
          if float(parent_entry.get("probability") or "0") < 0.00005: continue
          for attr_list in INFLECTIONS:
            for name, label in attr_list:
              value = parent_entry.get(name)
              if value and word in value:
                match_infl = True
          if not match_infl:
            return True
    if (regex.search(r"(^| )[\p{Lu}\p{P}\p{S}\d]", word) and "we" not in labels):
      return False
    if regex.search(r" ", word):
      return False
    if len(labels) == 1:
      return False
    return True

  def MakeMain(self, input_dbm, keys, words, etym_dict, infl_dict):
    infl_probs = {}
    rel_probs = {}
    for key in keys:
      serialized = input_dbm.GetStr(key)
      if not serialized: continue
      entries = json.loads(serialized)
      for entry in entries:
        word = entry["word"]
        prob = float(entry.get("probability") or 0)
        for attr_list in INFLECTIONS:
          for name, label in attr_list:
            value = entry.get(name)
            if value:
              for infl in value:
                old_rec = infl_probs.get(infl)
                if not old_rec or prob > old_rec[0]:
                  infl_probs[infl] = (prob, word)
        for rel_words, base_score in [(entry.get("parent"), 10), (entry.get("child"), 1)]:
          if not rel_words: continue
          for rel_word in rel_words:
            old_rec = rel_probs.get(rel_word)
            if not old_rec or prob > old_rec[0]:
              rel_probs[rel_word] = (base_score * prob, word)
    inflections = {}
    for infl, pair in infl_probs.items():
      inflections[infl] = pair[1]
    boss_words = {}
    for rel_word, pair in rel_probs.items():
      boss_words[rel_word] = pair[1]
    out_files = {}
    for key in keys:
      key_prefix = GetKeyPrefix(key)
      out_file = out_files.get(key_prefix)
      if not out_file:
        out_path = os.path.join(self.output_path, "main-{}.xhtml".format(key_prefix))
        logger.info("Creating: {}".format(out_path))
        out_file = open(out_path, "w")
        out_files[key_prefix] = out_file
        page_title = "etc." if key_prefix == "_" else key_prefix.upper()
        print(MAIN_HEADER_TEXT.format(esc(self.title), esc(page_title), esc(page_title)),
              file=out_file, end="")
      serialized = input_dbm.GetStr(key)
      if not serialized: continue
      entries = json.loads(serialized)
      for entry in entries:
        word = entry["word"]
        share = entry.get("share")
        min_share = 0.3 if regex.search("[A-Z]", word) else 0.2
        if share and float(share) < min_share: break
        self.MakeMainEntry(out_file, entry, input_dbm, etym_dict, infl_dict,
                           keys, inflections, boss_words)
    for key_prefix, out_file in out_files.items():
      print(MAIN_FOOTER_TEXT, file=out_file, end="")
      out_file.close()

  def MakeMainEntry(self, out_file, entry, input_dbm, etym_dict, infl_dict,
                    keys, inflections, boss_words):
    def P(*args, end="\n"):
      esc_args = []
      for arg in args[1:]:
        if isinstance(arg, str):
          arg = esc(arg)
        esc_args.append(arg)
      print(args[0].format(*esc_args), end=end, file=out_file)
    word = entry["word"]
    prob = float(entry.get("probability") or "0")
    pronunciation = entry.get("pronunciation")
    translations = entry.get("translation")
    examples = entry.get("example")
    is_major_word = prob >= 0.00001 and not regex.search("[A-Z]", word)
    if self.example_only and not examples: return
    poses = set()
    sub_poses = set()
    is_vetted_verb = False
    verb_labels = set()
    for item in entry["item"]:
      label = item["label"]
      pos = item["pos"]
      if label in self.supplement_labels:
        sub_poses.add(pos)
      else:
        poses.add(pos)
      if pos == "verb":
        if label in self.vetted_labels:
          is_vetted_verb = True
        verb_labels.add(label)
    if len(verb_labels) >= 2:
      is_vetted_verb = True
    if not poses:
      poses = sub_poses
    infl_groups = collections.defaultdict(list)
    if not regex.search(r"[A-Z].*[A-Z]", word):
      for attr_list in INFLECTIONS:
        for name, label in attr_list:
          pos, suffix = name.split("_", 1)
          if pos not in poses: continue
          if name == "verb_singular":
            suffix = "present 3ps"
          else:
            suffix = suffix.replace("_", " ")
          value = entry.get(name)
          if value:
            for infl in value:
              infl_groups[pos].append((suffix, infl, label))
    main_labels = set()
    label_items = collections.defaultdict(list)
    for item in entry["item"]:
      label = item["label"]
      if label in self.preferable_labels:
        main_labels.add(label)
      label_items[label].append(item)
    best_label = None
    is_stop = word in ARTICLES or word in PARTICLES
    if len(main_labels) >= 2:
      min_cost = None
      for label in main_labels:
        is_best = label in self.best_labels
        is_vetted = not is_stop and label in self.vetted_labels
        num_items = 0
        length_cost = 0
        for item in label_items[label]:
          text = item["text"]
          if not is_best and not is_vetted and not CheckSafeText(text):
            length_cost += 10.0
          if text.startswith("[translation]:"): continue
          text = regex.sub(r" \[-+\] .*", "", text).strip()
          if not text: continue
          num_items += 1
          text = regex.sub(r"[^-_\p{Latin}\d']+", " ", text).strip()
          num_words = text.count(" ") + 1
          length_cost += abs(math.log(9) - math.log(num_words))
        if not num_items: continue
        item_cost = abs(math.log(5) - math.log(num_items))
        length_cost = length_cost / num_items
        if is_best:
          quality_cost = 0.8
        elif is_vetted:
          quality_cost = 1.0
        else:
          quality_cost = 1.4
        cost = (item_cost + 0.5) * (length_cost + 1.0) * quality_cost
        if not min_cost or cost < min_cost:
          best_label = label
          min_cost = cost
    elif len(main_labels) >= 1:
      best_label = list(main_labels)[0]
    else:
      best_label = entry["item"][0]["label"]
    self.label_counters[best_label] += 1
    items = []
    sub_items = []
    tran_items = []
    for item in entry["item"]:
      label = item["label"]
      text = item["text"]
      if text.startswith("[translation]:"):
        tran_items.append(item)
      elif label == best_label:
        items.append(item)
      elif label in main_labels and is_major_word and not regex.search(r"\w{20,}", text):
        sub_items.append(item)
    if not items:
      items = sub_items
    if not items:
      items = tran_items
    if not items: return
    items = self.MergeShownItems(items, sub_items)
    phrases = []
    for phrase in entry.get("phrase") or []:
      is_good = False
      phrase_word = phrase["w"]
      phrase_prob = float(phrase.get("p") or 0)
      if phrase_word.find(" ") < 0 or phrase_prob > 0.01:
        phrases.append(phrase)
    self.num_words += 1
    P('<idx:entry>')
    P('<div>')
    P('<span class="word">')
    P('<idx:orth>{}', word)
    for pos, values in infl_groups.items():
      kind_infls = []
      for kind, infl, label in values:
        if inflections.get(infl) != word: continue
        kind_infls.append((kind, infl))
      if not kind_infls: continue
      P('<idx:infl inflgrp="{}">', pos)
      for kind, infl in kind_infls:
        P('<idx:iform name="{}" value="{}"/>', kind, infl)
        self.num_inflections += 1
      P('</idx:infl>')
    sub_words = []
    for label, rel_words in [("parent", entry.get("parent")), ("child", entry.get("child")),
                             ("relative", etym_dict.get(word))]:
      if not rel_words: continue
      for rel_word in rel_words:
        rel_norm = tkrzw_dict.NormalizeWord(rel_word)
        if not rel_norm or rel_norm in keys or rel_norm in inflections:
          continue
        boss = boss_words.get(rel_norm)
        if boss and boss != word: continue
        sub_words.append((label, rel_word))
        for rel_infl in infl_dict.get(rel_word) or []:
          infl_norm = tkrzw_dict.NormalizeWord(rel_infl)
          if not infl_norm or infl_norm in keys or infl_norm in inflections:
            continue
          sub_words.append((label, rel_infl))
    for rel_word in entry.get("alternative") or []:
      rel_norm = tkrzw_dict.NormalizeWord(rel_word)
      if not rel_norm or rel_norm in keys or rel_norm in inflections or rel_norm in boss_words:
        continue
      sub_words.append(("alternative", rel_word))
    for phrase in phrases:
      rel_word = phrase["w"]
      rel_norm = tkrzw_dict.NormalizeWord(rel_word)
      if not rel_norm or rel_norm in keys or rel_norm in inflections or rel_norm in boss_words:
        continue
      sub_words.append(("phrase", rel_word))
      rel_infls = infl_dict.get(rel_word) or []
      rel_tokens = rel_word.split(" ")
      if not rel_infls:
        i = 0
        while i < len(rel_tokens):
          if rel_tokens[i] == word:
            copy_rel_tokens = rel_tokens.copy()
            for rel_pos, rel_attrs in infl_groups.items():
              for _, rel_infl, _ in rel_attrs:
                copy_rel_tokens[i] = rel_infl
                sub_words.append(("phrase", " ".join(copy_rel_tokens)))
            break
          i += 1
      for rel_infl in rel_infls:
        infl_norm = tkrzw_dict.NormalizeWord(rel_infl)
        if not infl_norm or infl_norm in keys or infl_norm in inflections:
          continue
        sub_words.append(("phrase", rel_infl))
    if sub_words and not self.example_only:
      uniq_sub_words = set()
      P('<idx:infl inflgrp="common">')
      for sub_name, sub_word in sub_words:
        if sub_word in uniq_sub_words: continue
        uniq_sub_words.add(sub_word)
        P('<idx:iform name="{}" value="{}"/>', sub_name, sub_word)
        self.num_redirections += 1
      P('</idx:infl>')
    P('</idx:orth>')
    def GetParticiples(infl_names, first_only):
      participles = []
      for infl_name in infl_names:
        infl_value = entry.get(infl_name)
        if not infl_value: continue
        for participle in infl_value:
          participle = participle.strip()
          if not participle: continue
          if participle not in participles and participle != word:
            participles.append(participle)
          if first_only: break
      return participles
    if word in BASE_VETTED_VERBS:
      for participle in GetParticiples([
          "verb_singular", "verb_present_participle",
          "verb_past", "verb_past_participle", "alternative"], False):
        P('<idx:orth value="{}"></idx:orth>', participle)
    elif (regex.fullmatch("[a-z]+", word) and word not in PARTICLES and
          pronunciation and translations and is_vetted_verb and prob >= 0.000001 and
          len(label_items) >= 2):
      for participle in GetParticiples(["verb_present_participle", "verb_past_participle"], True):
        P('<idx:orth value="{}"></idx:orth>', participle)
    P('</span>')
    if pronunciation and not self.fallback:
      P('&#x2003;<span class="pron">/{}/</span>', pronunciation)
    P('</div>')
    if translations:
      self.num_trans += 1
      P('<div>{}</div>', ", ".join(translations[:6]))
    if self.fallback:
      if not translations:
        P('<div>{}</div>', SanitizeText(items[0]["text"]))
    elif self.example_only:
      if examples:
        for example in examples[:5]:
          self.MakeMainEntryExampleItem(P, example, entry)
    else:
      for item in items:
        self.MakeMainEntryItem(P, item)
      for phrase in phrases:
        self.MakeMainEntryPhraseItem(P, phrase)
      parents = entry.get("parent") or []
      etym_core = entry.get("etymology_core")
      if etym_core and etym_core not in parents:
        parents.append(etym_core)
      if parents:
        for parent in parents:
          self.MakeMainEntryParentItem(P, parent, input_dbm)
      for pos, values in infl_groups.items():
        P('<div>')
        for kind, value, label in values:
          P('<span class="attr">[{}]</span> {}', label, value)
        P('</div>')
    P('</idx:entry>')
    P('<br/>')

  def MakeMainEntryItem(self, P, item):
    pos = item["pos"]
    text = SanitizeText(item["text"])
    annots = []
    attr_match = regex.search(r"^\[([a-z]+)\]: ", text)
    if attr_match:
      if attr_match.group(1) == "translation":
        annots.append("訳語")
      text = text[len(attr_match.group(0)):].strip()
    while True:
      attr_label = None
      attr_match = regex.search(r"^ *[,、]*[\(（〔]([^\)）〕]+)[\)）〕]", text)
      if not attr_match: break
      for name in regex.split(r"[ ,、]", attr_match.group(1)):
        attr_label = TEXT_ATTRS.get(name)
        if attr_label: break
      if not attr_label: break
      text = text[len(attr_match.group(0)):].strip()
      annots.append(attr_label)
    self.num_items += 1
    text = regex.sub(r" \[-+\] .*", "", text).strip()
    text = CutTextByWidth(text, 160)
    P('<div>')
    leader = ""
    if item.get("is_aux"):
      leader = "+ "
      self.num_aux_items += 1
    P('<span class="pos">{}[{}]</span>', leader, POSES.get(pos) or pos)
    for annot in annots:
      P('<span class="attr">[{}]</span>', annot)
    P('{}', text)
    P('</div>')

  def MakeMainEntryPhraseItem(self, P, phrase):
    P('<div>')
    P('<span class="attr">[句]</span>')
    P('{} : {}', phrase["w"], ", ".join(phrase["x"]))
    P('</div>')
    self.num_collocations += 1

  def MakeMainEntryParentItem(self, P, parent, input_dbm):
    parent_entry = input_dbm.Get(parent)
    if not parent_entry: return
    entries = json.loads(parent_entry)
    for entry in entries:
      word = entry["word"]
      share = entry.get("share")
      min_share = 0.5 if regex.search("[A-Z]", word) else 0.25
      if share and float(share) < min_share: break
      translations = entry.get("translation")
      if translations:
        text = ", ".join(translations[:4])
      else:
        text = entry["item"][0]["text"]
        text = regex.sub(r" \[-+\] .*", "", text).strip()
      if text:
        P('<div>')
        P('<span class="attr">[語幹]</span>')
        P('<span>{} : {}</span>', word, text)
        P('</div>')

  def MakeMainEntryExampleItem(self, P, example, entry):
    highlights = {entry["word"].lower()}
    for attr_list in INFLECTIONS:
      fields = []
      for name, label in attr_list:
        value = entry.get(name)
        if value:
          for infl in value:
            highlights.add(infl)
    P('<div>')
    P('<span class="attr">&#x2023;</span>')
    P('<span class="exen">', end="")
    highlights = [regex.escape(x) for x in highlights]
    core_expr = "|".join(highlights)
    phrase_regex = regex.compile(r"(?i)(^|\W)(" + core_expr + r")(\W|$)")
    text = example["e"]
    while True:
      match = phrase_regex.search(text)
      if match:
        pos = match.span()[0]
        if pos > 0:
          P('{}', text[:pos], end="")
        P('{}', match.group(1), end="")
        P('<b>{}</b>', match.group(2), end="")
        P('{}', match.group(3), end="")
        pos = match.span()[1]
        text = text[pos:]
      else:
        if text:
          P('{}', text, end="")
        break
    P('</span>')
    P('<span class="exja">({})</span>', example["j"])
    P('</div>')

  def MakeNavigation(self, key_prefixes):
    out_path = os.path.join(self.output_path, "nav.xhtml")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(NAVIGATION_HEADER_TEXT.format(esc(self.title), esc(self.title)),
            file=out_file, end="")
      for key_prefix in key_prefixes:
        main_path = "main-{}.xhtml".format(key_prefix)
        page_title = "etc." if key_prefix == "_" else key_prefix.upper()
        print('<li><a href="{}">Words: {}</a></li>'.format(esc(main_path), esc(page_title)),
              file=out_file)
      print(NAVIGATION_FOOTER_TEXT, file=out_file, end="")

  def MakeOverview(self):
    out_path = os.path.join(self.output_path, "overview.xhtml")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(OVERVIEW_TEXT.format(esc(self.title), self.num_words, self.num_trans, self.num_items,
                                 self.num_inflections, self.num_redirections,
                                 self.num_collocations),
            file=out_file, end="")

  def MakeStyle(self):
    out_path = os.path.join(self.output_path, "style.css")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(STYLE_TEXT, file=out_file, end="")

  def MakePackage(self, key_prefixes):
    out_path = os.path.join(self.output_path, "package.opf")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(PACKAGE_HEADER_TEXT.format(CURRENT_UUID, esc(self.title), CURRENT_DATETIME),
            file=out_file, end="")
      main_ids = []
      for key_prefix in key_prefixes:
        main_path = "main-{}.xhtml".format(key_prefix)
        main_id = "main_" + key_prefix
        print('<item id="{}" href="{}" media-type="application/xhtml+xml"/>'.format(
          main_id, main_path), file=out_file)
        main_ids.append(main_id)
      print(PACKAGE_MIDDLE_TEXT, file=out_file, end="")
      for main_id in main_ids:
        print('<itemref idref="{}"/>'.format(main_id), file=out_file)
      print(PACKAGE_FOOTER_TEXT, file=out_file, end="")

  def TokenizeForDupCheck(self, text):
    tokens = []
    for token in self.tokenizer.Tokenize("en", text, True, True):
      token = regex.sub(r"[^-_\p{Latin}\d']+", "", token).strip()
      if not token or token in ARTICLES: continue
      tokens.append(token)
    return tokens

  def MergeShownItems(self, items, sub_items):
    if self.shrink:
      min_shown_items = 3
      mid_shown_items = 5
      max_shown_items = 8
    else:
      min_shown_items = 4
      mid_shown_items = 6
      max_shown_items = 12
    max_dup_score = 0.3
    merged_items = []
    for item in items:
      if len(merged_items) >= max_shown_items: break
      merged_items.append(item)
    if len(merged_items) < min_shown_items and sub_items:
      references = []
      for item in merged_items:
        text = item["text"]
        text = regex.sub(r" \[-+\] .*", "", text).strip()
        text = regex.sub(r"\(.*?\)", "", text).strip()
        text = regex.sub(r"\[.*?\]", "", text).strip()
        tokens = self.TokenizeForDupCheck(text)
        if tokens:
          references.append(tokens)
      for item in sub_items:
        if len(merged_items) >= mid_shown_items: break
        text = item["text"]
        if not CheckSafeText(text): continue
        if references:
          text = regex.sub(r" \[-+\] .*", "", text).strip()
          text = regex.sub(r"\(.*?\)", "", text).strip()
          text = regex.sub(r"\[.*?\]", "", text).strip()
          candidate = self.TokenizeForDupCheck(text)
          if not candidate: continue
          dup_score = tkrzw_dict.ComputeNGramPresision(candidate, references, 3)
          if dup_score >= max_dup_score: continue
        item["is_aux"] = True
        merged_items.append(item)
    return merged_items


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-dict-kindle"
  keyword_path = tkrzw_dict.GetCommandFlag(args, "--keyword", 1) or ""
  best_labels = set((tkrzw_dict.GetCommandFlag(args, "--best", 1) or "xa").split(","))
  vetted_labels = set((tkrzw_dict.GetCommandFlag(args, "--vetted", 1) or "wn").split(","))
  preferable_labels = set((tkrzw_dict.GetCommandFlag(
    args, "--preferable", 1) or "xa,xz,wn,ox,we").split(","))
  trustable_labels = set((tkrzw_dict.GetCommandFlag(
    args, "--trustable", 1) or "xa").split(","))
  supplement_labels = set((tkrzw_dict.GetCommandFlag(args, "--supplement", 1) or "xs").split(","))
  title = tkrzw_dict.GetCommandFlag(args, "--title", 1) or "Union English-Japanese Dictionary"
  min_prob_normal = float(tkrzw_dict.GetCommandFlag(args, "--min_prob_normal", 1) or 0.0000003)
  min_prob_capital = float(tkrzw_dict.GetCommandFlag(args, "--min_prob_multi", 1) or 0.000003)
  min_prob_multi = float(tkrzw_dict.GetCommandFlag(args, "--min_prob_capital", 1) or 0.000003)
  sufficient_prob = float(tkrzw_dict.GetCommandFlag(args, "--sufficient_prob", 1) or 0.00003)
  shrink = tkrzw_dict.GetCommandFlag(args, "--shrink", 0)
  fallback = tkrzw_dict.GetCommandFlag(args, "--fallback", 0)
  example_only = tkrzw_dict.GetCommandFlag(args, "--example_only", 0)
  if not input_path:
    raise RuntimeError("an input path is required")
  if not output_path:
    raise RuntimeError("an output path is required")
  GenerateUnionEPUBBatch(
    input_path, output_path, keyword_path,
    best_labels, vetted_labels, preferable_labels, trustable_labels, supplement_labels,
    title, min_prob_normal, min_prob_capital, min_prob_multi, sufficient_prob,
    shrink, fallback, example_only).Run()


if __name__=="__main__":
  main()
