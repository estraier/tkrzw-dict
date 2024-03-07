#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to generate files to make a EnJa Kindle dictionary from the union dictionary
#
# Usage:
#   generate_parallel_kindle.py [--input str] [--output str]
#
# Example:
#   ./generate_parallel_kindle.py --input anne01-translated.txt --output parallel-anne01
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


logger = tkrzw_dict.GetLogger()

CURRENT_UUID = str(uuid.uuid1())
CURRENT_DATETIME = regex.sub(r"\..*", "Z", datetime.datetime.now(
  datetime.timezone.utc).isoformat())
PACKAGE_HEADER_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<package unique-identifier="pub-id" version="3.0" xmlns="http://www.idpf.org/2007/opf" xml:lang="ja">
<metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
<dc:identifier id="pub-id">urn:uuid:{}</dc:identifier>
<dc:publisher>dbmx.net</dc:publisher>
<dc:title>[PT] {}</dc:title>
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
div.t {
  color: #888; font-size: 80%;
  margin-left: 4ex;
}
"""
NAVIGATION_HEADER_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops" lang="en">
<head>
<title>[PT] {}: Contents</title>
<link rel="stylesheet" href="style.css"/>
</head>
<body>
<div>[English-Japanese Parallel Text]</div>
<h1>{}</h1>
<div class="author">Author: {}</div>
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
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops" xmlns:mbp="https://kindlegen.s3.amazonaws.com/AmazonKindlePublishingGuidelines.pdf" lang="en">
<head>
<title>[PT] {}: {}</title>
<link rel="stylesheet" href="style.css"/>
</head>
<body epub:type="chapter">
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
    self.meta_author = ""

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
        line = unicodedata.normalize("NFKC", line)
        line = regex.sub(r"\s+", " ", line).strip()
        if not lines and not line:
          continue
        lines.append(line)
      while lines:
        if lines[-1]:
          break
        lines.pop()
    sections = []
    for i, line in enumerate(lines):
      match = regex.search(r"^# (.*)$", line)
      if match:
        self.title = match.group(1).strip()
      match = regex.search(r"^- *@title +(.*)$", line)
      if match:
        self.meta_title = match.group(1).strip()
      match = regex.search(r"^- *@author +(.*)$", line)
      if match:
        self.meta_author = match.group(1).strip()
      match = regex.search(r"^## (.*)$", line)
      if match:
        section_title = match.group(1).strip()
        section_title_tran = ""
        if i < len(lines) - 1:
          next_line = lines[i+1]
          next_match = regex.search(r"^%% (.*)$", next_line)
          if next_match:
            section_title_tran = next_match.group(1).strip()
        sections.append((section_title, section_title_tran, []))
      if sections:
        sections[-1][2].append(line)
    self.sections = sections
    logger.info("title={}, meta_title={}, meta_author".format(
      self.title, self.meta_title, self.meta_author))
    for i, (section_title, section_title_tran, section_lines) in enumerate(self.sections, 1):
      logger.info("section-{}: title={}, lines={}".format(
        i, section_title, len(section_lines)))

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
    with open(out_path, "w") as out_file:
      print(NAVIGATION_HEADER_TEXT.format(
        esc(self.title), esc(self.title), esc(self.meta_author)),
            file=out_file, end="")
      for i, (title, _, _) in enumerate(self.sections, 1):
        main_path = "main-{:03d}.xhtml".format(i)
        print('<li><a href="{}">{}</a></li>'.format(esc(main_path), esc(title)),
              file=out_file)
      print(NAVIGATION_FOOTER_TEXT, file=out_file, end="")

  def MakeMain(self, sec_id, section):
    out_path = os.path.join(self.output_path, "main-{:03d}.xhtml".format(sec_id))
    logger.info("Creating: {}".format(out_path))
    title, title_tran, lines = section
    paragraphs = [[]]
    for i, line in enumerate(lines):
      if line:
        if line.startswith("%% "): continue
        tran = ""
        if i < len(lines) - 1:
          next_line = lines[i+1]
          next_line.startswith("%% ")
          tran = next_line[3:].strip()
        paragraphs[-1].append([line, tran])
        if regex.search(r"^#+ ", line):
          paragraphs.append([])
      else:
        paragraphs.append([])
    with open(out_path, "w") as out_file:
      print(MAIN_HEADER_TEXT.format(esc(self.title), esc(title)), file=out_file, end="")
      for sentences in paragraphs:
        if not sentences: continue
        tag = 'p'
        line = sentences[0][0]
        match = regex.search(r"^(#+) +(.*)$", line)
        if match:
          tag = 'h' + str(len(match.group(1)))
          sentences[0][0] = match.group(2).strip()
        print('<' + tag + '>', file=out_file)
        for text, tran in sentences:
          self.WriteSentence(out_file, text, tran)
        print('</' + tag + '>', file=out_file)
      print(MAIN_FOOTER_TEXT, file=out_file, end="")

  def WriteSentence(self, out_file, src_text, trg_text):
    def P(*args, end="\n"):
      esc_args = []
      for arg in args[1:]:
        if isinstance(arg, str):
          arg = esc(arg)
        esc_args.append(arg)
      print(args[0].format(*esc_args), end=end, file=out_file)
    P('<div class="x">')
    P('<div class="s" lang="en">{}</div>', src_text)
    if trg_text:
      P('<div class="t" lang="ja">{}</div>', trg_text)
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
