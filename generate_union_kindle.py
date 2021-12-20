#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to generate files to make an EPUB archive from the union dictionary
#
# Usage:
#   generate_union_kindle.py [--input str] [--output str] [--quiet]
#
# Example:
#   ./generate_union_kindle.py --input union-body.tkh --output union-dict-epub
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
import datetime
import html
import json
import logging
import os
import pathlib
import regex
import sys
import time
import tkrzw
import tkrzw_dict
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
  "可算": "c",
  "不可算": "u",
  "自動詞": "vi",
  "他動詞": "vt",
  "countable": "c",
  "uncountable": "u",
  "intransitive": "vi",
  "transitive": "vt",
}
PARTICLES = set([
  "aback", "about", "above", "abroad", "across", "after", "against", "ahead", "along",
  "amid", "among", "apart", "around", "as", "at", "away", "back", "before", "behind",
  "below", "beneath", "between", "beside", "beyond", "by", "despite", "during", "down",
  "except", "for", "forth", "from", "in", "inside", "into", "near", "of", "off", "on",
  "onto", "out", "outside", "over", "per", "re", "since", "than", "through", "throughout",
  "till", "to", "together", "toward", "under", "until", "up", "upon", "with", "within",
  "without", "via",
])
CURRENT_UUID = str(uuid.uuid1())
CURRENT_DATETIME = regex.sub(r"\..*", "Z", datetime.datetime.now(
  datetime.timezone.utc).isoformat())
PACKAGE_HEADER_TEXT = """<?xml version="1.0" encoding="utf-8"?>
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
STYLE_TEXT = """html,body { margin: 0; padding: 0; background: #fff; color: #000; font-size: 12pt; }
span.word { font-weight: bold; }
span.pron { font-size: 90%; color: #444; }
span.pos,span.attr { font-size: 80%; color: #555; }
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
<p>In each word entry, the title word is shown in bold.  Some words have a pronounciation expression in the IPA format, bracketed as "/.../".  A list of translation can come next.  Then, definitions of the word come in English or Japanese.  Each definition is led by a part of speech label.  Additional information such as inflections and varints can come next.</p>
<p>The number of words is {}.  The number of words with translations is {}.  The number of definition items is {}.</p>
</article>
</body>
</html>
"""
MAIN_HEADER_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops" lang="ja" xmlns:mbp="https://kindlegen.s3.amazonaws.com/AmazonKindlePublishingGuidelines.pdf" xmlns:mmc="https://kindlegen.s3.amazonaws.com/AmazonKindlePublishingGuidelines.pdf" xmlns:idx="https://kindlegen.s3.amazonaws.com/AmazonKindlePublishingGuidelines.pdf">
<head>
<title>{}</title>
<link rel="stylesheet" href="style.css"/>
</head>
<body epub:type="dictionary">
<mbp:frameset>
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


class GenerateUnionEPUBBatch:
  def __init__(self, input_path, output_path, keyword_path, title, min_prob, sufficient_prob):
    self.input_path = input_path
    self.output_path = output_path
    self.keyword_path = keyword_path
    self.title = title
    self.min_prob = min_prob
    self.sufficient_prob = sufficient_prob
    self.num_words = 0
    self.num_trans = 0
    self.num_items = 0

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}".format(
      str(self.input_path), self.output_path))
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False, dbm="HashDBM").OrDie()
    os.makedirs(self.output_path, exist_ok=True)
    words = self.ListUpWords(input_dbm)
    keys = sorted(set([tkrzw_dict.NormalizeWord(word) for word, prob in words.items()]))
    key_prefixes = set()
    for key in keys:
      key_prefixes.add(GetKeyPrefix(key))
    key_prefixes = sorted(list(key_prefixes))
    self.MakeMain(input_dbm, keys, words)
    self.MakeNavigation(key_prefixes)
    self.MakeOverview()
    self.MakeStyle()
    self.MakePackage(key_prefixes)
    input_dbm.Close().OrDie()
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def ListUpWords(self, input_dbm):
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
        if not self.IsGoodEntry(entry, input_dbm, keywords): continue
        word = entry["word"]
        prob = float(entry.get("probability") or "0")
        words[word] = max(words.get(word) or 0.0, prob)
      it.Next()
    logger.info("Checking words done: {}".format(len(words)))
    return words

  def IsGoodEntry(self, entry, input_dbm, keywords):
    word = entry["word"]
    prob = float(entry.get("probability") or "0")
    if word in keywords:
      return True
    if prob < self.min_prob:
      return False
    if prob >= self.sufficient_prob:
      return True
    poses = set()
    labels = set()
    for item in entry["item"]:
      poses.add(item["pos"])
      if item["text"].startswith("[translation]:"): continue
      labels.add(item["label"])
    if "wj" in labels: return True
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
    if translations:
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
              if value and value == word:
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

  def MakeMain(self, input_dbm, keys, words):
    inflections = set()
    for key in keys:
      serialized = input_dbm.GetStr(key)
      if not serialized: continue
      entries = json.loads(serialized)
      for entry in entries:
        for attr_list in INFLECTIONS:
          for name, label in attr_list:
            value = entry.get(name)
            if value:
              value = tkrzw_dict.NormalizeWord(value)
              if value:
                inflections.add(value)
    out_files = {}
    for key in keys:
      key_prefix = GetKeyPrefix(key)
      out_file = out_files.get(key_prefix)
      if not out_file:
        out_path = os.path.join(self.output_path, "main-{}.xhtml".format(key_prefix))
        logger.info("Creating: {}".format(out_path))
        out_file = open(out_path, "w")
        out_files[key_prefix] = out_file
        print(MAIN_HEADER_TEXT.format(esc(self.title)), file=out_file, end="")
      serialized = input_dbm.GetStr(key)
      if not serialized: continue
      entries = json.loads(serialized)
      for entry in entries:
        word = entry["word"]
        share = entry.get("share")
        min_share = 0.3 if regex.search("[A-Z]", word) else 0.2
        if share and float(share) < min_share: break
        self.MakeMainEntry(out_file, entry, input_dbm, keys, inflections)
    for key_prefix, out_file in out_files.items():
      print(MAIN_FOOTER_TEXT, file=out_file, end="")
      out_file.close()

  def MakeMainEntry(self, out_file, entry, input_dbm, keys, inflections):
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
    poses = set()
    for item in entry["item"][:8]:
      poses.add(item["pos"])
    infl_groups = collections.defaultdict(list)
    if not regex.search(r"[A-Z]", word):
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
            infl_groups[pos].append((suffix, value, label))
    items = []
    first_label = None
    for item in entry["item"]:
      label = item["label"]
      if first_label:
        if first_label != label:
          break
      else:
        first_label = label
      items.append(item)
    self.num_words += 1
    P('<idx:entry name="en" scriptable="yes" spell="yes">')
    P('<div class="head">')
    P('<span class="word">')
    P('<idx:orth>{}', word)
    for pos, values in infl_groups.items():
      P('<idx:infl inflgrp="{}">', pos)
      for kind, value, label in values:
        for infl in value.split(","):
          infl = infl.strip()
          if not infl: continue
          P('<idx:iform name="{}" value="{}"/>', kind, infl)
      P('</idx:infl>')
    alternatives = entry.get("alternative")
    if alternatives:
      alt_words = []
      for alternative in alternatives:
        alt_norm = tkrzw_dict.NormalizeWord(alternative)
        if not alt_norm or alt_norm in keys or alt_norm in inflections:
          continue
        alt_words.append(alternative)
      if alt_words:
        P('<idx:infl inflgrp="common">')
        for alt_word in alt_words:
          P('<idx:iform name="alternative" value="{}"/>', alt_word)
        P('</idx:infl>')
    P('</idx:orth>')
    P('</span>')
    if pronunciation:
      P('&#x2003;<span class="pron">/{}/</span>', pronunciation)
    P('</div>')
    if translations:
      self.num_trans += 1
      P('<div class="tran">{}</div>', ", ".join(translations[:6]))
    for item in items[:10]:
      self.num_items += 1
      self.MakeMainEntryItem(P, item, False)
    phrases = entry.get("phrase")
    if phrases:
      for phrase in phrases:
        self.MakeMainEntryPhraseItem(P, phrase)
    parents = entry.get("parent")
    if parents:
      for parent in parents:
        self.MakeMainEntryParentItem(P, parent, input_dbm)
    for pos, values in infl_groups.items():
      P('<div class="infl">')
      for kind, value, label in values:
        P('<span class="col"><span class="attr">[{}]</span> {}</span>', label, value)
      P('</div>')
    P('</idx:entry>')
    P('<br/>')

  def MakeMainEntryItem(self, P, item, simple):
    pos = item["pos"]
    text = item["text"]
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
    text = regex.sub(r" \[-+\] .*", "", text).strip()
    if simple:
      text = CutTextByWidth(text, 100)
    P('<div class="item">')
    P('<span class="pos">[{}]</span>', POSES.get(pos) or pos)
    for annot in annots:
      P('<span class="attr">[{}]</span>', annot)
    P('<span class="text">{}</span>', text)
    P('</div>')

  def MakeMainEntryPhraseItem(self, P, phrase):
    P('<div class="item">')
    P('<span class="attr">[句]</span>')
    P('<span class="text">{} : {}</span>', phrase["w"], ", ".join(phrase["x"]))
    P('</div>')

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
        P('<div class="item">')
        P('<span class="attr">[語幹]</span>')
        P('<span class="text">{} : {}</span>', word, text)
        P('</div>')

  def MakeNavigation(self, key_prefixes):
    out_path = os.path.join(self.output_path, "nav.xhtml")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(NAVIGATION_HEADER_TEXT.format(esc(self.title), esc(self.title)),
            file=out_file, end="")
      for key_prefix in key_prefixes:
        main_path = "main-{}.xhtml".format(key_prefix)
        print('<li><a href="{}">Word: {}</a></li>'.format(main_path, key_prefix),
              file=out_file)
      print(NAVIGATION_FOOTER_TEXT, file=out_file, end="")

  def MakeOverview(self):
    out_path = os.path.join(self.output_path, "overview.xhtml")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(OVERVIEW_TEXT.format(esc(self.title), self.num_words, self.num_trans, self.num_items),
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


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-dict-kindle"
  keyword_path = tkrzw_dict.GetCommandFlag(args, "--keyword", 1) or ""
  title = tkrzw_dict.GetCommandFlag(args, "--title", 1) or "Union English-Japanese Dictionary"
  min_prob = float(tkrzw_dict.GetCommandFlag(args, "--min_prob", 1) or 0)
  sufficient_prob = float(tkrzw_dict.GetCommandFlag(args, "--sufficient_prob", 1) or 0.00001)
  if not input_path:
    raise RuntimeError("an input path is required")
  if not output_path:
    raise RuntimeError("an output path is required")
  GenerateUnionEPUBBatch(
    input_path, output_path, keyword_path, title, min_prob, sufficient_prob).Run()


if __name__=="__main__":
  main()
