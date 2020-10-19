#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to build a union database by merging TSV dictionaries
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
   ("adjective_superative", "形最")],
  [("adverb_comparative", "副比"),
   ("adverb_superative", "副最")]]
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
PACKAGE_TEXT = """<?xml version="1.0" encoding="utf-8"?>
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
<item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>
<item id="skmap" properties="search-key-map dictionary" href="skmap.xml" media-type="application/vnd.epub.search-key-map+xml"/>
<item id="main" href="main.xhtml" media-type="application/xhtml+xml"/>
<item id="style" href="style.css" media-type="text/css"/>
</manifest>
<spine page-progression-direction="default">
<itemref idref="nav"/>
<itemref idref="main"/>
</spine>
</package>
""".format(CURRENT_UUID, CURRENT_DATETIME)
NAVIGATION_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops">
<head>
<title>nav</title>
</head>
<body>
<nav epub:type="toc">
<ol>
<li><a href="main.xhtml">英和辞書</a></li>
</ol>
</nav>
</body>
</html>
"""
MAIN_HEADER_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops" lang="ja">
<head>
<title>統合英和辞書</title>
<link rel="stylesheet" href="style.css"/>
</head>
<body>
<section epub:type="titlepage" class="titlepage">
<h1 epub:type="title">統合英和辞書</h1>
<p>辞書だよーん。</p>
</section>
"""
MAIN_FOOTER_TEXT = """</body>
</html>
"""
STYLE_TEXT = """html,body { margin: 0; padding: 0; background: #ffffff; color: #000000; font-size: 12pt; }
.titlepage h1,.titlepage p { margin: 1ex 0; }
article { margin: 1ex 0; }
dfn { font-weight: bold; }
.cond_pron { padding-left: 2ex; font-size: 85%; color: #333333; }
.item_list { list-style: none; margin: 0; padding: 0; font-size: 90%; color: #999999; }
.item_list li { margin: 0; padding: 0 0 0 0.5ex; }
.attr_name { background: #eeeeee; border: solid 1pt #dddddd; border-radius: 0.5ex;
  font-size: 80%; color: #555555;
  display: inline-block; min-width: 4ex; text-align: center; padding: 0; }
.attr_value { color: #111111; }
"""
SKMAP_HEADER_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<search-key-map xmlns="http://www.idpf.org/2007/ops" xml:lang="en">
"""
SKMAP_FOOTER_TEXT = """</search-key-map>
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

class GenerateUnionEPUBBatch:
  def __init__(self, input_path, output_path):
    self.input_path = input_path
    self.output_path = output_path

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

    keys = []
    it = input_dbm.MakeIterator()
    it.First()
    while True:
      key = it.GetKeyStr()
      if key == None: break
      keys.append(key)
      it.Next()
    keys = sorted(keys)

    #keys = keys[:1000]
    #keys = ["juxtapose", "cornbread", "saw", "see", "train", "unix"]

    self.MakeMimeType()
    self.MakeContainer(meta_dir_path)
    self.MakePackage(data_dir_path)
    self.MakeNavigation(data_dir_path)
    self.MakeMain(data_dir_path, input_dbm, keys)
    self.MakeStyle(data_dir_path)
    self.MakeSearchKeyMap(data_dir_path, input_dbm, keys)
    input_dbm.Close().OrDie()
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

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

  def MakePackage(self, data_dir_path):
    out_path = os.path.join(data_dir_path, "package.opf")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(PACKAGE_TEXT, file=out_file, end="")

  def MakeNavigation(self, data_dir_path):
    out_path = os.path.join(data_dir_path, "nav.xhtml")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(NAVIGATION_TEXT, file=out_file, end="")

  def MakeMain(self, data_dir_path, input_dbm, keys):
    out_path = os.path.join(data_dir_path, "main.xhtml")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(MAIN_HEADER_TEXT, file=out_file, end="")
      print('<section epub:type="dictionary">', file=out_file)
      for key in keys:
        serialized = input_dbm.GetStr(key)
        if not serialized: continue
        entries = json.loads(serialized)
        for entry in entries:
          self.MakeMainEntry(out_file, entry)
      print('</section>', file=out_file)
      print(MAIN_FOOTER_TEXT, file=out_file, end="")

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
    P('<article id="{}">', urllib.parse.quote(word))
    P('<aside epub:type="condensed-entry" hidden="hidden">')
    P('<dfn>{}</dfn>', word)
    P('<ul class="item_list">')
    if translations:
      P('<li class="top_attr">')
      P('<span class="attr_name">訳語</span>')
      P('<span class="attr_value">{}</span>', ", ".join(translations[:8]))
      P('</li>')
    if pronunciation:
      P('<li class="top_attr">')
      P('<span class="attr_name">発音</span>')
      P('<span epub:type="phonetic-transcription" lang="en-fonipa" class="attr_value">{}</span>',
        pronunciation)
      P('</li>')
    for item in entry["item"][:5]:
      self.MakeMainEntryItem(P, item, False)
    P('</ul>')
    P('</aside>')
    P('<dfn>{}</dfn>', word)
    P('<ul class="item_list">')
    if translations:
      P('<li class="top_attr">')
      P('<span class="attr_name">訳語</span>')
      P('<span class="attr_value">{}</span>', ", ".join(translations[:8]))
      P('</li>')
    if pronunciation:
      P('<li class="top_attr">')
      P('<span class="attr_name">発音</span>')
      P('<span epub:type="phonetic-transcription" lang="en-fonipa" class="attr_value">{}</span>',
        pronunciation)
      P('</li>')
    for attr_list in INFLECTIONS:
      fields = []
      for name, label in attr_list:
        value = entry.get(name)
        if value:
          value = ('<span class="attr_name">{}</span>'
                   ' <span class="attr_value">{}</span>').format(esc(label), esc(value))
          fields.append(value)
      if fields:
        P('<li class="top_attr">')
        print(", ".join(fields), file=out_file, end="")
        P('</li>')
    for item in entry["item"]:
      self.MakeMainEntryItem(P, item, False)
    P('</ul>')
    P('</article>')

  def MakeMainEntryItem(self, P, item, simple):
    label = item["label"]
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
      text = CutTextByWidth(text, 80)
    P('<li class="item">')
    P('<span class="attr_name">{}</span>', label.upper())
    P('<span class="attr_name">{}</span>', POSES.get(pos) or pos)
    for annot in annots:
      P('<span class="attr_name">{}</span>', annot)
    P('<span class="attr_value">{}</span>', text)
    P('</li>')

  def MakeStyle(self, data_dir_path):
    out_path = os.path.join(data_dir_path, "style.css")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(STYLE_TEXT, file=out_file, end="")

  def MakeSearchKeyMap(self, data_dir_path, input_dbm, keys):
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
        serialized = input_dbm.GetStr(key)
        if not serialized: continue
        entries = json.loads(serialized)
        for entry in entries:
          word = entry["word"]
          P('<search-key-group href="main.xhtml#{}">', urllib.parse.quote(word))
          P('<match value="{}">', word)
          uniq_infls = set([word])
          for infl_rules in INFLECTIONS:
            for infl_name, infl_label in infl_rules:
              infl_value = entry.get(infl_name)
              if infl_value and infl_value not in uniq_infls:
                P('<value value="{}"/>', infl_value)
                uniq_infls.add(infl_value)
          P('</match>')
          P('</search-key-group>')
      print(SKMAP_FOOTER_TEXT, file=out_file, end="")


def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-dict-epub"
  if not input_path:
    raise RuntimeError("an input path is required")
  if not output_path:
    raise RuntimeError("an output path is required")
  GenerateUnionEPUBBatch(input_path, output_path).Run()


if __name__=="__main__":
  main()
