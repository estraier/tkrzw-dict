#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to generate files to make an EPUB archive from the union dictionary
#
# Usage:
#   generate_union_epub.py [--input str] [--output str] [--quiet]
#
# Example:
#   ./generate_union_epub.py --input union-body.tkh --output union-dict-epub
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
CONTAINER_TEXT = """<?xml version="1.0"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
<rootfiles>
<rootfile full-path="OEBPS/package.opf" media-type="application/oebps-package+xml"/>
</rootfiles>
</container>
"""
CURRENT_UUID = str(uuid.uuid1())
CURRENT_DATETIME = regex.sub(r"\..*", "Z", datetime.datetime.now(
  datetime.timezone.utc).isoformat())
PACKAGE_HEADER_TEXT = """<?xml version="1.0" encoding="utf-8"?>
<package unique-identifier="pub-id" version="3.0" xmlns="http://www.idpf.org/2007/opf" xml:lang="ja">
<metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
<dc:identifier id="pub-id">urn:uuid:{}</dc:identifier>
<dc:publisher>dbmx.net</dc:publisher>
<dc:title>統合英和辞書</dc:title>
<dc:language>ja</dc:language>
<dc:language>en</dc:language>
<dc:type id="tp">dictionary</dc:type>
<meta property="dcterms:modified">{}</meta>
<meta property="dcterms:type" refines="#tp">bilingual</meta>
<meta property="source-language">en</meta>
<meta property="target-language">ja</meta>
</metadata>
<manifest>
<item id="skmap" properties="search-key-map dictionary" href="skmap.xml" media-type="application/vnd.epub.search-key-map+xml"/>
<item id="style" href="style.css" media-type="text/css"/>
<item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>
<item id="overview" href="overview.xhtml" media-type="application/xhtml+xml"/>
""".format(CURRENT_UUID, CURRENT_DATETIME)
PACKAGE_MIDDLE_TEXT = """</manifest>
<spine page-progression-direction="default">
<itemref idref="nav"/>
<itemref idref="overview"/>
"""
PACKAGE_FOOTER_TEXT = """</spine>
</package>
"""
SKMAP_HEADER_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<search-key-map xmlns="http://www.idpf.org/2007/ops" xml:lang="en">
"""
SKMAP_FOOTER_TEXT = """</search-key-map>
"""
STYLE_TEXT = """html,body { margin: 0; padding: 0; background: #ffffff; color: #000000; font-size: 12pt; }
article { margin: 1.2ex 0; }
a { color: #001188; }
.pron { display: inline-block; margin-left: 2ex; vertical-align: 2%;
  color: #111111; font-size: 85%; }
.pron:before,.pron:after { content: "/"; color: #888888; font-size: 80%; }
dfn { font-weight: bold; font-style: normal; }
.item_list { list-style: none; margin: 0; padding: 0; font-size: 90%; color: #999999; }
.item_list li { margin: 0; padding: 0 0 0 0.5ex; }
.attr_name { background: #f4f4f4; border: solid 1pt #dddddd; border-radius: 0.5ex;
  font-size: 70%; color: #444444;
  display: inline-block; min-width: 4.0ex; text-align: center; padding: 0; margin-left: -0.5ex; }
.attr_value { color: #000000; }
"""
NAVIGATION_HEADER_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops">
<head>
<title>統合英和辞書: 目次</title>
<link rel="stylesheet" href="style.css"/>
</head>
<body>
<h1>統合英和辞書</h1>
<article>
<h2>目次</h2>
<nav epub:type="toc">
<ol>
<li><a href="overview.xhtml">概要</a></li>
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
<title>統合英和辞書: 概要</title>
<link rel="stylesheet" href="style.css"/>
</head>
<body>
<article>
<h2>概要</h2>
<p>これは、オープンなデータから作成された英和辞書である。このデータは<a href="http://idpf.org/epub/dict/epub-dict.html">EPUB Dictionaries and Glossaries 1.0</a>の仕様に準拠しているので、EPUBの閲覧用システムにインストールすれば、検索機能を備える電子辞書として利用することができる。辞書データは<a href="https://wordnet.princeton.edu/">WordNet</a>と<a href="http://compling.hss.ntu.edu.sg/wnja/index.en.html">日本語WordNet</a>と<a href="https://ja.wiktionary.org/">Wiktionary日本語版</a>と<a href="https://en.wiktionary.org/">Wiktionary英語版</a>と<a href="http://www.edrdg.org/jmdict/edict.html">EDict2</a>を統合したものだ。作成方法については<a href="https://dbmx.net/dict/">公式サイト</a>を参照のこと。利用や再配布の権利については各データのライセンスに従うべきだ。</p>
<p>見出し語は太字で表示される。IPA発音記号の情報がある場合、見出し語の右に「//」で括って表示される。訳語の情報がある場合、見出し語の下に訳語のリストが表示される。語義の各項目の先頭には品詞のラベルが付いている。その後に、英語か日本語の語義説明が来る。「複数」「三単」「現分」「過去」「過分」「比較」「最上」は、名詞や動詞や形容詞の屈折形を示す。</p>
</article>
</body>
</html>
"""
MAIN_HEADER_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops" lang="ja">
<head>
<title>統合英和辞書</title>
<link rel="stylesheet" href="style.css"/>
</head>
<body epub:type="dictionary">
"""
MAIN_FOOTER_TEXT = """</body>
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
  def __init__(self, input_path, output_path, min_prob, multi_min_prob):
    self.input_path = input_path
    self.output_path = output_path
    self.min_prob = min_prob
    self.multi_min_prob = multi_min_prob

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}".format(
      str(self.input_path), self.output_path))
    input_dbm = tkrzw.DBM()
    input_dbm.Open(self.input_path, False, dbm="HashDBM").OrDie()
    os.makedirs(self.output_path, exist_ok=True)
    meta_dir_path = os.path.join(self.output_path, "META-INF")
    os.makedirs(meta_dir_path, exist_ok=True)
    data_dir_path = os.path.join(self.output_path, "OEBPS")
    os.makedirs(data_dir_path, exist_ok=True)
    words = self.ListUpWords(input_dbm)
    keys = sorted(set([tkrzw_dict.NormalizeWord(x) for x in words]))
    key_prefixes = set()
    for key in keys:
      key_prefixes.add(GetKeyPrefix(key))
    key_prefixes = sorted(list(key_prefixes))
    self.MakeMimeType()
    self.MakeContainer(meta_dir_path)
    self.MakePackage(data_dir_path, key_prefixes)
    self.MakeSearchKeyMap(data_dir_path, input_dbm, keys, words)
    self.MakeStyle(data_dir_path)
    self.MakeNavigation(data_dir_path, key_prefixes)
    self.MakeOverview(data_dir_path)
    self.MakeMain(data_dir_path, input_dbm, keys, words)
    input_dbm.Close().OrDie()
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def ListUpWords(self, input_dbm):
    logger.info("Checking words")
    words = set()
    it = input_dbm.MakeIterator()
    it.First()
    while True:
      rec = it.GetStr()
      if rec == None: break
      entries = json.loads(rec[1])
      for entry in entries:
        if not self.IsGoodEntry(entry): continue
        words.add(entry["word"])
      it.Next()
    logger.info("Checking words done: {}".format(len(words)))
    return words

  def IsGoodEntry(self, entry):
    word = entry["word"]
    prob = float(entry.get("probability") or "0")
    if prob < self.min_prob:
      return False
    labels = set()
    for item in entry["item"]:
      if item["text"].startswith("[translation]:"): continue
      labels.add(item["label"])
    if "wj" in labels: return True
    if (regex.search(r"(^| )[\p{Lu}\p{P}\p{S}\d]", word) and "we" not in labels):
      return False
    if prob < self.multi_min_prob and (regex.search(r" ", word) or len(labels) == 1):
      return False
    return True

  def MakeMimeType(self):
    out_path = os.path.join(self.output_path, "mimetype")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print("application/epub+zip", file=out_file, end="")

  def MakeContainer(self, meta_dir_path):
    out_path = os.path.join(meta_dir_path, "container.xml")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(CONTAINER_TEXT, file=out_file, end="")

  def MakePackage(self, data_dir_path, key_prefixes):
    out_path = os.path.join(data_dir_path, "package.opf")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(PACKAGE_HEADER_TEXT, file=out_file, end="")
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

  def MakeSearchKeyMap(self, data_dir_path, input_dbm, keys, words):
    out_path = os.path.join(data_dir_path, "skmap.xml")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      def P(*args, end="\n"):
        esc_args = []
        for arg in args[1:]:
          if isinstance(arg, str):
            arg = esc(arg)
          esc_args.append(arg)
        print(args[0].format(*esc_args), end=end, file=out_file)
      print(SKMAP_HEADER_TEXT, file=out_file, end="")
      for key in keys:
        key_prefix = GetKeyPrefix(key)
        main_path = "main-{}.xhtml".format(key_prefix)
        serialized = input_dbm.GetStr(key)
        if not serialized: continue
        entries = json.loads(serialized)
        for entry in entries:
          word = entry["word"]
          if word not in words: continue
          P('<search-key-group href="{}#{}">', main_path, ConvertWordToID(word))
          P('<match value="{}">', word)
          uniq_infls = set([word])
          for infl_rules in INFLECTIONS:
            for infl_name, infl_label in infl_rules:
              infl_value = entry.get(infl_name)
              if infl_value:
                for infl_word in infl_value:
                  if infl_word not in uniq_infls:
                    P('<value value="{}"/>', infl_word)
                  uniq_infls.add(infl_word)
          P('</match>')
          P('</search-key-group>')
      print(SKMAP_FOOTER_TEXT, file=out_file, end="")

  def MakeStyle(self, data_dir_path):
    out_path = os.path.join(data_dir_path, "style.css")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(STYLE_TEXT, file=out_file, end="")

  def MakeNavigation(self, data_dir_path, key_prefixes):
    out_path = os.path.join(data_dir_path, "nav.xhtml")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(NAVIGATION_HEADER_TEXT, file=out_file, end="")
      for key_prefix in key_prefixes:
        main_path = "main-{}.xhtml".format(key_prefix)
        print('<li><a href="{}">見出し語: {}</a></li>'.format(main_path, key_prefix),
              file=out_file)
      print(NAVIGATION_FOOTER_TEXT, file=out_file, end="")

  def MakeOverview(self, data_dir_path):
    out_path = os.path.join(data_dir_path, "overview.xhtml")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(OVERVIEW_TEXT, file=out_file, end="")

  def MakeMain(self, data_dir_path, input_dbm, keys, words):
    out_files = {}
    for key in keys:
      key_prefix = GetKeyPrefix(key)
      out_file = out_files.get(key_prefix)
      if not out_file:
        out_path = os.path.join(data_dir_path, "main-{}.xhtml".format(key_prefix))
        logger.info("Creating: {}".format(out_path))
        out_file = open(out_path, "w")
        out_files[key_prefix] = out_file
        print(MAIN_HEADER_TEXT, file=out_file, end="")
      serialized = input_dbm.GetStr(key)
      if not serialized: continue
      entries = json.loads(serialized)
      for entry in entries:
        if entry["word"] not in words: continue
        self.MakeMainEntry(out_file, entry)
    for key_prefix, out_file in out_files.items():
      print(MAIN_FOOTER_TEXT, file=out_file, end="")
      out_file.close()

  def MakeMainEntry(self, out_file, entry):
    def P(*args, end="\n"):
      esc_args = []
      for arg in args[1:]:
        if isinstance(arg, str):
          arg = esc(arg)
        esc_args.append(arg)
      print(args[0].format(*esc_args), end=end, file=out_file)
    word = entry["word"]
    pronunciation = entry.get("pronunciation")
    translations = entry.get("translation")
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
    P('<article id="{}">', ConvertWordToID(word))
    P('<aside epub:type="condensed-entry" hidden="hidden">')
    P('<dfn>{}</dfn>', word)
    if pronunciation:
      P('<span epub:type="phonetic-transcription" lang="en-fonipa" class="pron">{}</span>',
        pronunciation)
    P('<ul class="item_list">')
    if translations:
      P('<li class="top_attr">')
      P('<span class="attr_value">{}</span>', ", ".join(translations[:6]))
      P('</li>')
    for item in items[:5]:
      self.MakeMainEntryItem(P, item, True)
    P('</ul>')
    P('</aside>')
    P('<dfn>{}</dfn>', word)
    if pronunciation:
      P('<span epub:type="phonetic-transcription" lang="en-fonipa" class="pron">{}</span>',
        pronunciation)
    P('<ul class="item_list">')
    if translations:
      P('<li class="top_attr">')
      P('<span class="attr_value">{}</span>', ", ".join(translations[:8]))
      P('</li>')
    for item in items[:10]:
      self.MakeMainEntryItem(P, item, False)
    for attr_list in INFLECTIONS:
      fields = []
      for name, label in attr_list:
        value = entry.get(name)
        if value:
          value = ('<span class="attr_name">{}</span>'
                   ' <span class="attr_value">{}</span>').format(esc(label), esc(", ".join(value)))
          fields.append(value)
      if fields:
        P('<li class="top_attr">')
        print(", ".join(fields), file=out_file, end="")
        P('</li>')
    P('</ul>')
    P('</article>')

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
    P('<li class="item">')
    P('<span class="attr_name">{}</span>', POSES.get(pos) or pos)
    for annot in annots:
      P('<span class="attr_name">{}</span>', annot)
    P('<span class="attr_value">{}</span>', text)
    P('</li>')


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-dict-epub"
  min_prob = float(tkrzw_dict.GetCommandFlag(args, "--min_prob", 1) or 0)
  multi_min_prob = float(tkrzw_dict.GetCommandFlag(args, "--multi_min_prob", 1) or 0.00002)
  if not input_path:
    raise RuntimeError("an input path is required")
  if not output_path:
    raise RuntimeError("an output path is required")
  GenerateUnionEPUBBatch(input_path, output_path, min_prob, multi_min_prob).Run()


if __name__=="__main__":
  main()
