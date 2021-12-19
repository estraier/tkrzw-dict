#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to parse English Wiktionary XML stream and export word information
#
# Usage:
#   parse_wiktionary_en_tran.py [--quiet]
#   (It reads the standard input and prints the result on the standard output.)
#
# Example:
#   $ bzcat enwiktionary-latest-pages-articles.xml.bz2 |
#     ./make_wikipedia_gross_jaen.py > wiktionary-gross-jaen.tsv
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

import logging
import html
import regex
import sys
import tkrzw_dict
import unicodedata
import xml.sax
import xml.sax.handler

logger = tkrzw_dict.GetLogger()


class XMLHandler(xml.sax.handler.ContentHandler):
  def __init__(self):
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
        self.processText()
      self.model = None
      self.format = None
      self.text = None
    self.tags.pop()

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
    if not regex.fullmatch("[\p{Han}\p{Hiragana}\p{Katakana}]+", title): return
    fulltext = html.unescape(self.text)
    fulltext = regex.sub(r"<!--.*?-->", "", fulltext)
    fulltext = regex.sub(r"(\n==+[^=]+==+)", "\\1\n", fulltext)
    sections = []
    is_ja = False
    for line in fulltext.split("\n"):
      line = line.strip()
      match = regex.search(r"{{ja-kanjitab\|(.*?|)?alt=(.*?)(\|.*)?}}", line)
      if match:
        word = match.group(2).strip()
        if word:
          out_fields = []
          out_fields.append(title)
          out_fields.append("alternative")
          out_fields.append(word)
          print("\t".join(out_fields))
        continue
      if regex.search(r"^==([^=]+)==$", line):
        lang = regex.sub(r"^==([^=]+)==$", r"\1", line).strip()
        lang = lang.lower()
        if lang in ("{{ja}}", "japanese"):
          is_ja = True
        else:
          is_ja = False
        mode = ""
        submode = ""
        tran_top = ""
      elif regex.search(r"^===([^=]+)===$", line):
        mode = regex.sub(r"^===([^=]+)===$", r"\1", line).strip()
        mode = regex.sub(r":.*", "", mode).strip()
        mode = mode.lower()
        sections.append((mode,[]))
        submode = ""
        tran_top = ""
      elif regex.search(r"^====+([^=]+)=+===$", line):
        submode = regex.sub(r"^====+([^=]+)=+===$", r"\1", line).strip()
        submode = regex.sub(r":.*", "", submode).strip()
        submode = submode.lower()
        if submode in ("{{noun}}", "{{name}}", "noun",
                       "{{verb}}", "verb",
                       "{{adj}}", "{{adjective}}", "adjective",
                       "{{adv}}", "{{adverb}}", "adverb", "prepositional phrase",
                       "pronoun", "preposition", "article", "interjection", "conjunction"):
          mode = submode
          sections.append((mode,[]))
          submode = ""
          tran_top = ""
      elif is_ja:
        if sections and not submode:
          section = sections[-1]
          section[1].append(line)
    for mode, lines in sections:
      mode = regex.sub(r":.*", "", mode).strip()
      mode = regex.sub(r"[0-9]+$", "", mode).strip()
      if mode in ("{{noun}}", "{{name}}", "noun", "proper noun"):
        mode = "noun"
      elif mode in ("{{verb}}", "verb"):
        mode = "verb"
      elif mode in ("{{adj}}", "{{adjective}}", "adjective"):
        mode = "adjective"
      elif mode in ("{{adv}}", "{{adverb}}", "adverb", "prepositional phrase"):
        mode = "adverb"
      elif mode in ("{{pronoun}}", "pronoun"):
        mode = "pronoun"
      elif mode in ("{{prep}}", "preposition"):
        mode = "preposition"
      elif mode in ("{{det}}", "determiner"):
        mode = "determiner"
      elif mode in ("{{article}}", "article"):
        mode = "article"
      elif mode in ("{{interj}}", "interjection"):
        mode = "interjection"
      elif mode in ("{{conj}}", "conjunction"):
        mode = "conjunction"
      elif mode in ("{{pref}}", "{{prefix}}", "prefix"):
        mode = "prefix"
      elif mode in ("{{suf}}", "{{suffix}}", "suffix"):
        mode = "suffix"
      elif mode in ("{{abbr}}", "{{abbreviation}}", "abbreviation"):
        mode = "abbreviation"
      elif mode in ("{{alter}}", "alternative", "alternative forms", "alternative form"):
        mode = "alternative"
      else:
        continue
      last_mode = mode
      cat_lines = []
      for line in lines:
        if cat_lines and line.startswith("|"):
          cat_lines[:-1] += line
        else:
          cat_lines.append(line)
      last_level = 0
      for line in cat_lines:
        if not regex.search(r"^[#\*:]", line):
          last_level = 0
          continue
        prefix = regex.sub(r"^([#\*:]+).*", r"\1", line)
        level = len(prefix)
        text = line[level:]
        if level > last_level + 1:
          continue
        last_level = level
        if text.find("{{quote") >= 0: continue
        word = title
        match = regex.search("{{ja-def\|(.*?)}}(.*)", text)
        if match:
          word = match.group(1).strip()
          text = match.group(2).strip()
        text = self.MakePlainText(text)
        text = regex.sub(r"\(.*?\)", "", text).strip()
        text = regex.sub(r"\s+", " ", text).strip()
        if text.startswith("cf."): continue
        if (level == 1 and
            regex.fullmatch(r"[\p{Han}\p{Hiragana}\p{Katakana}]+", word) and
            regex.search(r"[\p{Latin}\p{Han}\p{Hiragana}\p{Katakana}]", text)):
          out_fields = []
          out_fields.append(word)
          out_fields.append(mode)
          out_fields.append(text)
          print("\t".join(out_fields))

  def MakePlainText(self, text):
    text = regex.sub(r"^[#\*]+", "", text)
    text = regex.sub(r"^--+", "", text)
    text = regex.sub(r"\{\{lb\|\en(\|\w+)*(\|countable)(\|\w+)*\}\}", r"(countable)", text)
    text = regex.sub(r"\{\{lb\|\en(\|\w+)*(\|uncountable)(\|\w+)*\}\}", r"(uncountable)", text)
    text = regex.sub(r"\{\{lb\|\en(\|\w+)*(\|transitive\+?)(\|\w+)*\}\}", r"(transitive)", text)
    text = regex.sub(r"\{\{lb\|\en(\|\w+)*(\|intransitive\+?)(\|\w+)*\}\}",
                     r"(intransitive)", text)
    text = regex.sub(r"\{\{\.\.\.\}\}", "...", text)
    text = regex.sub(r"(\{\{[^{}]+)\{\{[^{}]+\}\}([^}]*\}\})", r"\1\2", text)
    text = regex.sub(r"\{\{(context|lb|tag|label|infl)\|[^\}]*\}\}", "", text)
    text = regex.sub(r"\{\{abbreviation of(\|en)?\|([^|}]+)\}\}", r"\2", text)
    text = regex.sub(r"\{\{w\|(lang=[a-z]+\|)?([^\}\|]*)(\|[^\}]*)?\}\}", r"\2", text)
    text = regex.sub(r"\{\{(m|ux|l)\|[a-z]+\|([^\|\}]+)(\|[^\}\|]+)*\}\}", r"\2", text)
    text = regex.sub(r"\{\{(n-g|non-gloss definition)\|([^\|\}]+)(\|[^\}\|]+)*\}\}", r"\2", text)
    text = regex.sub(r"\{\{&lit\|en\|(.*?)\|(.*?)\|(.*?)(\|.*?)*?\}\}", r"cf. \1, \2, \3 ", text)
    text = regex.sub(r"\{\{&lit\|en\|(.*?)\|(.*?)(\|.*?)*?\}\}", r"cf. \1, \2 ", text)
    text = regex.sub(r"\{\{&lit\|en\|(.*?)(\|.*?)*?\}\}", r"cf. \1 ", text)
    text = regex.sub(r"\{\{(vern|taxlink)\|(.*?)(\|.*?)*\}\}", r"\2", text)
    text = regex.sub(r"\{\{syn of\|en\|(.*?)(\|.*?)*\}\}", r"Synonym of \1", text)
    text = regex.sub(r"\{\{syn\|en\|(.*?)\|(.*?)\|(.*?)(\|.*?)*?\}\}",
                     r"Synonyms: \1, \2, \3 ", text)
    text = regex.sub(r"\{\{syn\|en\|(.*?)\|(.*?)(\|.*?)*?\}\}", r"Synonyms: \1, \2 ", text)
    text = regex.sub(r"\{\{syn\|en\|(.*?)(\|.*?)*?\}\}", r"Synonym: \1 ", text)
    text = regex.sub(r"\{\{rfdate[a-z]+\|[a-z]+\|([^\|\}]+)(\|[^\}\|]+)*\}\}", r"\1", text)
    text = regex.sub(r"\{\{(RQ|Q):([^\|\}]+)(\|[^\|\}]+)*\|passage=([^\|\}]+)(\|[^\|\}]+)*\}\}",
                     r"\2 -- \4", text)
    text = regex.sub(r"\{\{(RQ|R):([^\|\}]+)(\|[^\}\|]+)*\}\}", "", text)
    text = regex.sub(r"\{\{[^}]*\}\}", r"", text)
    text = regex.sub(r"\{\}", r"", text)
    text = regex.sub(r"\}\}", r"", text)
    text = regex.sub(r"\[\[w:[a-z]+:([^\]\|]+)(\|[^\]\|]+)\]\]", r"\1", text)
    text = regex.sub(r"\[\[(category):[^\]]*\]\]", "", text, regex.IGNORECASE)
    text = regex.sub(r"\[\[([^\]\|]+\|)?([^\]]*)\]\]", r"\2", text)
    text = regex.sub(r"\[(https?://[^ ]+ +)([^\]]+)\]", r"\2", text)
    text = regex.sub(r"\[https?://.*?\]", r"", text)
    text = regex.sub(r"\[\[", r"", text)
    text = regex.sub(r"\]\]", r"", text)
    text = regex.sub(r"'''", "", text)
    text = regex.sub(r"''", "", text)
    text = regex.sub(r"\( *\)", "", text)
    text = regex.sub(r"<ref>.*?</ref>", "", text)
    text = regex.sub(r"</?[a-z]+[^>]*>", "", text)
    text = regex.sub(r"<!-- *", "(", text)
    text = regex.sub(r" *-->", ")", text)
    text = regex.sub(r"^ *[,:;] *", "", text)
    text = unicodedata.normalize('NFKC', text)
    return regex.sub(r"\s+", " ", text).strip()


def main():
  args = sys.argv[1:]
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  logger.info("Process started")
  parser = xml.sax.make_parser()
  handler = XMLHandler()
  parser.setContentHandler(handler)
  try:
    parser.parse(sys.stdin)
  except xml.sax.SAXException:
    pass
  logger.info("Process done")


if __name__=="__main__":
  main()
