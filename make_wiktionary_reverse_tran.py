#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to parse Japanese Wiktionary XML stream and export word information
#
# Usage:
#   make_wiktionary_reverse_tran.py [--sampling num] [--max num] [--quiet]
#   (It reads the standard input and prints the result on the standard output.)
#
# Example:
#   $ bzcat jawiktionary-latest-pages-articles.xml.bz2 |
#     ./make_wiktionary_reverse_tran.py > wiktionary-tran-rev.tsv
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
import logging
import html
import random
import regex
import sys
import tkrzw_dict
import xml.sax
import xml.sax.handler

random.seed(19780211)
logger = tkrzw_dict.GetLogger()


class XMLHandler(xml.sax.handler.ContentHandler):
  def __init__(self, sampling_ratio, max_outputs):
    self.sampling_ratio = sampling_ratio
    self.max_outputs = max_outputs
    self.num_articles = 0
    self.num_outputs = 0
    self.tags = []
    self.title = None
    self.is_redirect = False
    self.has_restrictions = False
    self.model = None
    self.format = None
    self.text = None

  def startDocument(self):
    logger.info("Start the document")

  def endDocument(self):
    logger.info("End the document")

  def startElement(self, name, attrs):
    self.tags.append(name)
    if self.tags == ['mediawiki', 'page']:
      self.title = None
      self.is_redirect = False
      self.has_restrictions = False
    if self.tags == ['mediawiki', 'page', 'title']:
      self.title = ""
    if self.tags == ['mediawiki', 'page', 'redirect']:
      self.is_redirect = True
    if self.tags == ['mediawiki', 'page', 'restrictions']:
      self.has_restrictions = True
    if self.tags == ['mediawiki', 'page', 'revision', 'model']:
      self.model = ""
    if self.tags == ['mediawiki', 'page', 'revision', 'format']:
      self.format = ""
    if self.tags == ['mediawiki', 'page', 'revision', 'text']:
      self.text = ""

  def endElement(self, name):
    if self.tags == ['mediawiki', 'page', 'revision']:
      if (self.title and not self.is_redirect and not self.has_restrictions and
          self.model == 'wikitext' and self.format == 'text/x-wiki' and self.text):
        self.num_articles += 1
        if self.num_articles % 1000 == 0:
          logger.info("Article {}".format(self.num_articles))
        if random.random() <= self.sampling_ratio:
          self.processText()
      self.model = None
      self.format = None
      self.text = None

    self.tags.pop()
    if self.num_outputs >= self.max_outputs:
      logger.info("reached max outputs ({})".format(self.max_outputs))
      raise xml.sax.SAXException("reached max articles")

  def characters(self, content):
    if self.tags == ['mediawiki', 'page', 'title']:
      self.title += content
    if self.tags == ['mediawiki', 'page', 'revision', 'model']:
      self.model += content
    if self.tags == ['mediawiki', 'page', 'revision', 'format']:
      self.format += content
    if self.tags == ['mediawiki', 'page', 'revision', 'text']:
      self.text += content

  def processText(self):
    title = self.title
    if title.find(":") >= 0: return
    if not regex.search(r"[\p{Han}\p{Hiragana}\p{Katakana}]", title): return
    fulltext = html.unescape(self.text)
    fulltext = regex.sub(r"<!--.*?-->", "", fulltext)
    fulltext = regex.sub(r"(\n==+[^=]+==+)", "\\1\n", fulltext)
    is_jap_head = False
    faces = []
    trans = collections.defaultdict(set)
    for line in fulltext.split("\n"):
      line = line.strip()
      if regex.search(r"^==([^=]+)==$", line):
        lang = regex.sub(r"^==([^=]+)==$", r"\1", line).strip()
        lang = lang.lower()
        if lang in ("{{ja}}", "{{jap}}", "{{japanese}}", "日本語", "japanese"):
          is_jap_head = True
          is_tran = False
          faces = []
        elif lang.startswith("{{") or lang.endswith("語"):
          is_jap_head = False
          is_tran = False
      elif is_jap_head:
        match = regex.search(r"^===(.*?)===$", line)
        if match:
          mode = match.group(1)
          mode = regex.sub(r"^=*(.*?)=*$", r"\1", mode)
          match = regex.search(r":([\p{Han}\p{Hiragana}ー]+)", mode)
          if match:
            faces = []
            for face in regex.split(r"[,;、。；,，]", match.group(1)):
              face = face.strip()
              if face:
                faces.append(face)
        match = regex.search(r"^'''(.*?)'''.*【(.*?)】", line)
        if match and match.group(1) == title:
          faces = []
          note = match.group(2)
          note = regex.sub("\[\[(.*?)\]\]", r"\1", note)
          note = regex.sub("\((.*?)\)", r"\1", note)
          note = regex.sub("（(.*?)）", r"\1", note)
          for face in regex.split(r"[,;、。；,，]", note):
            face = face.strip()
            if face:
              faces.append(face)
        match = regex.search(r"^\* *\{\{en\}\}:(.*)", line)
        if match:
          text = match.group(1).strip()
          text = regex.sub(r"{{[a-z+]*\|[a-z]*\|(.*?)}}", r"\1", text)
          text = regex.sub(r"{{.*?}}", "", text)
          text = regex.sub(r"\[\[(.*?)\]\]", r"\1", text)
          text = regex.sub(r"''+(.*?)''+", r"\1", text)
          text = regex.sub(r"（.*?）", r"", text)
          text = regex.sub(r"\(.*?\)", r"", text)
          for tran in regex.split(r"[,;、。；,，]", text):
            tran = regex.sub(r"\s+", " ", tran).strip()
            if not regex.fullmatch("[-_\p{Latin}' ]+", tran): continue
            rep_faces = set([title])
            if regex.fullmatch(r"[\p{Hiragana}ー]+", title):
              for face in faces:
                if not regex.fullmatch(r"[\p{Han}\p{Hiragana}ー]+", face): continue
                rep_faces.add(face)
            for face in rep_faces:
              trans[tran].add(face)
    for source, targets in trans.items():
      print("{}\t{}".format(source, "\t".join(targets)))


def main():
  args = sys.argv[1:]
  sampling_ratio = float(tkrzw_dict.GetCommandFlag(args, "--sampling", 1) or 1.0)
  max_outputs = int(tkrzw_dict.GetCommandFlag(args, "--max", 1) or sys.maxsize)
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  if sampling_ratio <= 0 or sampling_ratio > 1:
    raise ValueError("invalid sampling ratio")
  if max_outputs < 0:
    raise ValueError("invalid max outputs")
  logger.info("Process started")
  parser = xml.sax.make_parser()
  handler = XMLHandler(sampling_ratio, max_outputs)
  parser.setContentHandler(handler)
  try:
    parser.parse(sys.stdin)
  except xml.sax.SAXException:
    pass
  logger.info("Process done")


if __name__=="__main__":
  main()
