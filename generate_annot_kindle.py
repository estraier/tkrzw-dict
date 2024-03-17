#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to generate files to make a EnJa annotation book for Kindle.
#
# Usage:
#   generate_annot_kindle.py [--input str] [--output str]
#
# Example:
#   ./generate_annot_kindle.py --input anne01-annotated.tsv --output annot-anne01
#
# Copyright 2024 Mikio Hirabayashi
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
import unicodedata
import urllib
import uuid


POS_LABELS = {
  "noun": "名",
  "verb": "動",
  "phrasal verb": "動",
  "linking verb": "動",
  "adjective": "形",
  "demonstrative adjective": "形",
  "adverb": "副",
  "pronoun": "代",
  "auxverb": "助",
  "auxiliary verb": "助",
  "modal verb": "助",
  "preposition": "前",
  "prepositional": "前",
  "determiner": "限",
  "article": "冠",
  "interjection": "間",
  "conjunction": "接",
  "relative pronoun": "関",
  "relative adverb": "関",
  "prefix": "頭",
  "suffix": "尾",
  "abbreviation": "省",
  "phrase": "句",
  "idiom": "熟",
  "numeral": "数",
  "conditional": "条",
  "expression": "慣",
}

logger = tkrzw_dict.GetLogger()

CURRENT_UUID = str(uuid.uuid1())
CURRENT_DATETIME = regex.sub(r"\..*", "Z", datetime.datetime.now(
  datetime.timezone.utc).isoformat())
PACKAGE_HEADER_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<package unique-identifier="pub-id" version="3.0" xmlns="http://www.idpf.org/2007/opf" xml:lang="ja">
<metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
<dc:identifier id="pub-id">urn:uuid:{}</dc:identifier>
<dc:publisher>dbmx.net</dc:publisher>
<dc:title>[EJAB] {}</dc:title>
<dc:language>en</dc:language>
<dc:language>ja</dc:language>
<dc:creator>{}</dc:creator>
<dc:type id="tp">dictionary</dc:type>
<meta property="dcterms:modified">{}</meta>
<meta property="dcterms:type" refines="#tp">bilingual</meta>
</metadata>
<manifest>
<item id="style" href="style.css" media-type="text/css"/>
<item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>
"""
PACKAGE_MIDDLE_TEXT = """</manifest>
<spine page-progression-direction="default">
<itemref idref="nav"/>
"""
PACKAGE_FOOTER_TEXT = """</spine>
</package>
"""
STYLE_TEXT = """html,body {
  margin: 0; padding: 0; background: #fff; color: #000; font-size: 12pt;
  text-align: left; text-justify: none; direction: ltr;
}
h1, h2, h3, h4, h5, h6, p {
  margin: 2ex 0 2ex 0;
}
div.titletran {
  margin: 0 0 2ex 0;
}
div.author {
  margin: 1ex 0;
}
div.stats {
  color: #333;
}
div.source {
  margin-top: 1ex;
}
div.target {
  margin-left: 3ex;
  font-weight: normal;
  color: #444;
}
div.vocab {
  margin-left: 4ex;
  font-weight: normal;
  color: #666;
}
h2 div.vocab, h3 div.vocab {
  font-size: 12pt;
}
span.vphrase {
  color: #111;
}
div.navi {
  text-align: right;
  display: none;
  font-size: 80%;
  font-family: monospace;
}
div.navi a {
  color: #58e;
  text-decoration: none;
}
div.navi span {
  color: #ccc;
}
a:hover {
  text-decoration: underline;
}
@media screen and (min-width:800px) {
  html {
    text-align: center;
    background: #eee;
  }
  body {
    display: inline-block;
    width: 700px;
    margin: 1ex 1ex;
    padding: 1ex 2ex;
    border: 1pt solid #ccc;
    border-radius: 1ex;
    text-align: left;
  }
  div.target {
    color: #333;
  }
  div.vocab {
    color: #555;
  }
  span.vphrase {
    color: #118;
  }
  div.navi {
    display: block;
  }
}
"""
NAVIGATION_HEADER_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops" lang="en">
<head>
<title>[EJAB] {}: Contents</title>
<link rel="stylesheet" href="style.css"/>
</head>
<body>
<div>[English-Japanese Annotation Book]</div>
<h1>{}</h1>
<div class="titletran">{}</div>
<div class="author">by <b>{}</b></div>
<div class="stats"><small>{}</small></div>
<article>
<h2>Contents</h2>
<nav epub:type="toc">
<ol>
"""
NAVIGATION_FOOTER_TEXT = """</ol>
</nav>
</article>
</body>
</html>
"""
MAIN_HEADER_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops" xmlns:mbp="https://kindlegen.s3.amazonaws.com/AmazonKindlePublishingGuidelines.pdf" xmlns:idx="https://kindlegen.s3.amazonaws.com/AmazonKindlePublishingGuidelines.pdf" lang="en">
<head>
<title>[EJAB] {}: {}</title>
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


class Batch:
  def __init__(self, input_path, output_path):
    self.input_path = input_path
    self.output_path = output_path
    self.title = ""
    self.meta_title = ""
    self.title_tran = ""
    self.meta_author = ""
    self.vocab_uniq_tran = set()
    self.vocab_uniq_gloss = set()

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_path={}, output_path={}".format(
      str(self.input_path), self.output_path))
    self.ReadInput()
    os.makedirs(self.output_path, exist_ok=True)
    self.MakePackage()
    self.MakeStyle()
    self.MakeNavigation()
    for i, section in enumerate(self.sections, 1):
      self.MakeMain(i, section)
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def ReadInput(self):
    lines = []
    with open(self.input_path) as input_file:
      for line in input_file:
        line = unicodedata.normalize("NFKC", line).strip()
        fields = line.split("\t")
        source = fields[0]
        target = fields[1] if len(fields) > 1 else ""
        annots = fields[2:]
        lines.append((source, target, annots))
    sections = []
    for source, target, annots in lines:
      match = regex.search(r"^# (.*)$", source)
      if match:
        self.title = match.group(1).strip()
        self.title_tran = target
      match = regex.search(r"^- *@title +(.*)$", source)
      if match:
        self.meta_title = match.group(1).strip()
      match = regex.search(r"^- *@author +(.*)$", source)
      if match:
        self.meta_author = match.group(1).strip()
      match = regex.search(r"^## (.*)$", source)
      if match:
        section_title = match.group(1).strip()
        section_title_tran = target
        sections.append((section_title, section_title_tran, []))
      if sections:
        sections[-1][2].append((source, target, annots))
    self.sections = []
    for section_title, section_title_tran, section_lines in sections:
      paragraphs = [[]]
      for source, target, annots in section_lines:
        if source:
          paragraphs[-1].append([source, target, annots])
          if regex.search(r"^#+ ", source):
            paragraphs.append([])
        else:
          paragraphs.append([])
      paragraphs = [x for x in paragraphs if x]
      self.sections.append((section_title, section_title_tran, paragraphs))
    logger.info("title={}, meta_title={}, meta_author={}".format(
      self.title, self.meta_title, self.meta_author))
    for i, (section_title, section_title_tran, paragraphs) in enumerate(self.sections, 1):
      logger.info("section-{}: title={}, paragraphs={}".format(
        i, section_title, len(paragraphs)))

  def MakePackage(self):
    out_path = os.path.join(self.output_path, "package.opf")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(PACKAGE_HEADER_TEXT.format(
        CURRENT_UUID, esc(self.meta_title), esc(self.meta_author), CURRENT_DATETIME),
            file=out_file, end="")
      for i, _ in enumerate(self.sections, 1):
        main_path = "main-{:03d}.xhtml".format(i)
        print('<item id="main{:03d}" href="{}" media-type="application/xhtml+xml"/>'.format(
          i, main_path), file=out_file)
      print(PACKAGE_MIDDLE_TEXT, file=out_file, end="")
      for i, _ in enumerate(self.sections, 1):
        print('<itemref idref="main{:03d}"/>'.format(i), file=out_file)
      print(PACKAGE_FOOTER_TEXT, file=out_file, end="")

  def MakeStyle(self):
    out_path = os.path.join(self.output_path, "style.css")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(STYLE_TEXT, file=out_file, end="")

  def MakeNavigation(self):
    out_path = os.path.join(self.output_path, "nav.xhtml")
    logger.info("Creating: {}".format(out_path))
    num_sections = len(self.sections)
    num_paragraphs = 0
    num_sentences = 0
    num_words = 0
    num_characters = 0
    for section in self.sections:
      title, title_tran, paragraphs = section
      num_paragraphs += len(paragraphs)
      for sentences in paragraphs:
        num_sentences += len(sentences)
        for src_text, trg_text, annots in sentences:
          num_characters += len(src_text)
          src_text = regex.sub("(\p{Latin})['’]", r"\1_", src_text)
          src_text = regex.sub("(\d)[.,](\d)", r"\1_\2", src_text)
          words = regex.split("[^-_\p{Latin}\d]+", src_text)
          words = [x for x in words if x]
          num_words += len(words)
    logger.info("Stats: sections={}, paragraphs={}, sentences={}, words={}, characters={}".format(
      num_sections, num_paragraphs, num_sentences, num_words, num_characters))
    stats_html = "<div>sections={}, paragraphs={}, sentences={}</div>\n".format(
      num_sections, num_paragraphs, num_sentences)
    stats_html += "<div>words={}, characters={}</div>\n".format(
      num_words, num_characters)
    with open(out_path, "w") as out_file:
      print(NAVIGATION_HEADER_TEXT.format(
        esc(self.title), esc(self.title), esc(self.title_tran),
        esc(self.meta_author), stats_html),
            file=out_file, end="")
      for i, (title, _, _) in enumerate(self.sections, 1):
        main_path = "main-{:03d}.xhtml".format(i)
        print('<li><a href="{}">{}</a></li>'.format(esc(main_path), esc(title)),
              file=out_file)
      print(NAVIGATION_FOOTER_TEXT, file=out_file, end="")

  def MakeMain(self, sec_id, section):
    out_path = os.path.join(self.output_path, "main-{:03d}.xhtml".format(sec_id))
    logger.info("Creating: {}".format(out_path))
    title, title_tran, paragraphs = section
    with open(out_path, "w") as out_file:
      def P(*args, end="\n"):
        esc_args = []
        for arg in args[1:]:
          if isinstance(arg, str):
            arg = esc(arg)
          esc_args.append(arg)
        print(args[0].format(*esc_args), end=end, file=out_file)
      print(MAIN_HEADER_TEXT.format(esc(self.title), esc(title)), file=out_file, end="")
      self.WriteNavi(P, sec_id)
      for sentences in paragraphs:
        tag = 'p'
        line = sentences[0][0]
        match = regex.search(r"^(#+) +(.*)$", line)
        if match:
          tag = 'h' + str(len(match.group(1)))
          sentences[0][0] = match.group(2).strip()
        P('<idx:entry>')
        P('<' + tag + '>')
        for src_text, trg_text, annots in sentences:
          self.WriteSentence(P, src_text, trg_text, annots)
        P('</' + tag + '>')
        P('</idx:entry>')
      self.WriteNavi(P, sec_id)
      print(MAIN_FOOTER_TEXT, file=out_file, end="")

  def WriteNavi(self, P, sec_id):
    P('<div class="navi">')
    if sec_id > 1:
      P('<a href="main-{:03d}.xhtml">[←]</a>', sec_id - 1)
    else:
      P('<span>[←]</span>')
    P('<a href="nav.xhtml">[↑]</a>')
    if sec_id < len(self.sections):
      P('<a href="main-{:03d}.xhtml">[→]</a>', sec_id + 1)
    else:
      P('<span>[→]</span>')
    P('</div>')

  def WriteSentence(self, P, src_text, trg_text, annots):
    P('<div class="sentence">')
    P('<div lang="en" class="source">{}</div>', src_text)
    for annot in annots:
      fields = annot.split("|", 3)
      if len(fields) != 4: continue
      phrase, tran, pos, gloss = fields
      norm_tran = phrase.lower() + ":"
      norm_tran += regex.sub(r"[^\p{Han}\p{Hiragana}\p{Katakana}]", "", tran).strip()
      if norm_tran in self.vocab_uniq_tran:
        continue
      self.vocab_uniq_tran.add(norm_tran)
      norm_gloss = phrase.lower() + ":"
      norm_gloss += regex.sub(r"[^\p{Latin} ]", "", gloss).lower()[:20].strip()
      if norm_gloss in self.vocab_uniq_gloss:
        continue
      self.vocab_uniq_gloss.add(norm_gloss)
      pos = regex.sub(r" +phrase$", "", pos)
      if pos == "adjective" and phrase.startswith("be "):
        pos = "verb"
      pos_label = POS_LABELS.get(pos) or "他"
      P('<div class="vocab"><small><small>')
      P('<span class="vphrase">{}</span>', phrase)
      P('<span class="vtran">({})</span>', tran)
      P('<span class="vpos">[{}]</span>', pos_label)
      P('<span class="vgloss">{}</span>', gloss)
      P('</small></small></div>')
    if trg_text:
      P('<div lang="ja" class="target"><small><small>{}</small></small></div>', trg_text)
    P('</div>')
    

def main():
  args = sys.argv[1:]
  input_path = tkrzw_dict.GetCommandFlag(args, "--input", 1) or "union-body.tkh"
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-dict-kindle"
  if not input_path:
    raise RuntimeError("an input path is required")
  if not output_path:
    raise RuntimeError("an output path is required")
  Batch(input_path, output_path).Run()


if __name__=="__main__":
  main()
