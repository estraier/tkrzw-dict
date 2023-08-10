#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to parse English Wiktionary XML stream and export word information
#
# Usage:
#   parse_wiktionary_en.py [--sampling num] [--max num] [--quiet]
#   (It reads the standard input and prints the result on the standard output.)
#
# Example:
#   $ bzcat enwiktionary-latest-pages-articles.xml.bz2 |
#     ./parse_wikipedia_en.py > wiktionary-en.tsv
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
import random
import regex
import sys
import tkrzw_dict
import unicodedata
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
    tran_mode = False
    if regex.search(r"^[-\p{Latin}0-9 ]+/translations$", title):
      title = regex.sub(r"/.*", "", title)
      tran_mode = True
    if not regex.search(r"^[-\p{Latin}0-9 ]+$", title): return
    fulltext = html.unescape(self.text)
    fulltext = regex.sub(r"<!--.*?-->", "", fulltext)
    fulltext = regex.sub(r"(\n==+[^=]+==+)", "\\1\n", fulltext)
    fulltext = self.ConcatNestLines(fulltext)
    output = []
    ipa_us = ""
    ipa_misc = ""
    noun_plural = ""
    verb_singular = ""
    verb_present_participle = ""
    verb_past = ""
    verb_past_participle = ""
    adjective_comparative = ""
    adjective_superlative = ""
    adverb_comparative = ""
    adverb_superlative = ""
    infl_modes = set()
    is_eng = False
    mode = ""
    submode = ""
    sections = []
    synonyms = []
    hypernyms = []
    hyponyms = []
    antonyms = []
    etym_core = None
    etym_prefix = None
    etym_suffix = None
    derivatives = []
    relations = []
    translations = {}
    alsos = []
    for line in fulltext.split("\n"):
      line = line.strip()
      if regex.search(r"^{{also\|(.*)}}", line):
        expr = regex.sub(r"^{{also\|(.*)}}", r"\1", line)
        for also in expr.split("|"):
          also = also.strip()
          if also:
            alsos.append(also)
      elif regex.search(r"^==([^=]+)==$", line):
        lang = regex.sub(r"^==([^=]+)==$", r"\1", line).strip()
        lang = lang.lower()
        if lang in ("{{en}}", "{{eng}}", "english"):
          is_eng = True
        else:
          is_eng = False
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
      elif is_eng:
        if sections and not submode:
          section = sections[-1]
          section[1].append(line)
        def CheckMode(labels):
          if mode and submode in labels:
            return True
          if mode in labels and not submode:
            return True
          return False
        rel_words = None
        if CheckMode(("{{syn}}", "synonym", "synonyms")):
          rel_words = synonyms
        elif CheckMode(("{{hyper}}", "hypernym", "hypernyms")):
          rel_words = hypernyms
        elif CheckMode(("{{hypo}}", "hyponym", "hyponyms")):
          rel_words = hyponyms
        elif CheckMode(("{{ant}}", "antonym", "antonyms")):
          rel_words = antonyms
        elif CheckMode(("{{derived}}", "derived terms", "derived term", "派生語")):
          rel_words = derivatives
        elif CheckMode(("{{rel}}", "related terms", "related term", "関連語")):
          rel_words = relations
        if rel_words != None:
          for rel_word in regex.findall(r"\{\{l\|en\|([- \p{Latin}]+?)\}\}", line):
            rel_words.append(rel_word)
          for rel_word in regex.findall(r"\[\[([- \p{Latin}]+?)\]\]", line):
            rel_words.append(rel_word)
        if mode == "etymology":
          match = regex.search(r"\{\{([a-z]+)\|en\|(.*?)\}\}", line)
          if match and not etym_core and not etym_prefix and not etym_suffix:
            label = match.group(1)
            values = []
            for value in match.group(2).split("|"):
              if value.find("=") >= 0: continue
              values.append(value)
            if (len(values) == 2 and regex.fullmatch("[-A-Za-z]+", values[0]) and
                regex.fullmatch("[-A-Za-z]+", values[1])):
              if label == "prefix":
                etym_prefix = regex.sub(r"-$", "", values[0])
                etym_core = values[1]
              elif label == "suffix":
                etym_core = values[0]
                etym_suffix = regex.sub(r"^-", "", values[1])
              elif label == "affix":
                if values[0].endswith("-") and values[1].startswith("-"):
                  etym_prefix = values[0][:-1]
                  etym_suffix = values[1][1:]
                elif values[0].endswith("-"):
                  etym_prefix = values[0][:-1]
                  etym_core = values[1]
                elif values[1].startswith("-"):
                  etym_core = values[0]
                  etym_suffix = values[1][1:]
        if mode and submode in ("translation", "translations"):
          for tr, expr in regex.findall(r"\{\{(trans-top|checktrans-top)\|(.*?)\}\}", line):
            tran_top = regex.sub(r"^id=[^\|]+\|", "", expr)
            break
          if regex.search(r"^[#\*:]", line):
            prefix = regex.sub(r"^([#\*:]+).*", r"\1", line)
            level = len(prefix)
            text = line[level:].strip()
            if level in (1, 2) and text.startswith("Japanese:"):
              text = regex.sub(r"^[^:]+:", "", text).strip()
              if text:
                old_values = translations.get(mode) or []
                old_values.append((tran_top, text))
                translations[mode] = old_values
      if regex.search(r"\{\{ipa[-a-z]*\|en\|([^}]+)\}\}", line, regex.IGNORECASE):
        value = regex.sub(r".*\{\{ipa[-a-z]*\|en\|([^}]+)\}\}.*", r"\1",
                          line, flags=regex.IGNORECASE)
        value = regex.sub(r"(qual[\d]=[^|]+\|)+", "", value)
        value = regex.sub(r"\|.*", "", value)
        value = regex.sub(r"</?[a-z]+[^>]*>", "", value)
        value = regex.sub(r"^/(.*)/$", r"\1", value)
        value = regex.sub(r"lang=[a-z]*\|", "", value)
        value = regex.sub(r"[,\|].*", "", value)
        value = regex.sub(r"^/(.*)/$", r"\1", value)
        value = regex.sub(r"/ ?\(.*", "", value)
        value = regex.sub(r"/", "", value)
        value = regex.sub(r"[\[\]]", "", value)
        value = value.strip()
        if value:
          if line.find("|US") >= 0 or line.find("|GA") >= 0:
            if not ipa_us:
              ipa_us = value
          else:
            if not ipa_misc:
              ipa_misc = value
      if regex.search(r"\{\{en-noun\|?([^\}]*)\}\}", line):
        if "noun" in infl_modes: continue
        infl_modes.add("noun")
        value = regex.sub(r".*\{\{en-noun[a-z]*\|?([^\}]*)\}\}.*", r"\1", line).strip()
        value = regex.sub(r"\[\[:en:#[^\]]*?\|(.*?)\]\]", r"\1", value)
        values = value.split("|") if value else []
        values = self.TrimInflections(values)
        stop = False
        for value in values:
          if value.startswith("head="):
            stop = True
        if not stop:
          if regex.search(r"(s|ch|sh|x|o)$", title):
            noun_plural = title + "es"
          elif regex.search(r"([^aeiou])y$", title):
            noun_plural = title[:-1] + "ies"
          else:
            noun_plural = title + "s"
          if len(values) == 1 and values[0] == "s":
            noun_plural = title + "s"
          elif len(values) == 1 and values[0] == "es":
            noun_plural = title + "es"
          elif len(values) == 1 and values[0] in ("~", "+"):
            pass
          elif len(values) == 1 and values[0] == "-":
            noun_plural = None
          elif len(values) == 1:
            noun_plural = values[0]
          elif len(values) == 2 and values[0] == "+":
            pass
          elif (len(values) == 2 and values[0] in ("-", "~") and
                values[1] not in ("s", "es", "+", "-", "~","?", "!")):
            noun_plural = values[1]
          elif len(values) == 2 and values[0] == "es":
            noun_plural = title + "es"
          elif len(values) == 2 and values[1] == "es":
            stem = title if values[0] in ("+", "-", "~") else values[0]
            noun_plural = stem + "es"
          elif len(values) == 2 and values[1] == "ies":
            stem = title if values[0] in ("+", "-", "~") else values[0]
            noun_plural = stem + "ies"
          elif len(values) == 1 and values[0].startswith("pl="):
            noun_plural = regex.sub(".*=", "", values[0])
          elif len(values) == 2 and values[0].startswith("sg=") and values[1] == "es":
            noun_plural = title + "es"
          elif len(values) == 2 and values[0].startswith("sg=") and values[1].startswith("pl="):
            noun_plural = regex.sub(".*=", "", values[1])
          elif len(values) > 0 and values[0] not in ("s", "es", "ies", "+", "-", "~", "?", "!"):
            noun_plural = values[0]
      if regex.search(r"\{\{en-verb\|?([^\}]*)\}\}", line):
        if "verb" in infl_modes: continue
        infl_modes.add("verb")
        value = regex.sub(r".*\{\{en-verb[a-z]*\|?([^\}]*)\}\}.*", r"\1", line).strip()
        value = regex.sub(r"\[\[:en:#[^\]]*?\|(.*?)\]\]", r"\1", value)
        values = value.split("|") if value else []
        value_attrs = {}
        values = self.TrimInflections(values, value_attrs)
        stop = False
        if values and values[0].startswith("head="):
          if values[0][5:] != title:
            stop = True
          values.pop(0)
        if title.find(" ") >= 0 and len(values) != 4:
          stop = True
        for value in values:
          if value.startswith("head="):
            stop = True
          if value.find("*") >= 0:
            stop = True
        if not stop:
          verb_singular = title + "s"
          if regex.search(r"(s|ch|sh|x|o)$", title):
            verb_singular = title + "es"
          elif regex.search(r"([^aeiou])y$", title):
            verb_singular = title[:-1] + "ies"
          else:
            verb_singular = title + "s"
          if title.endswith("e"):
            verb_present_participle = title[:-1] + "ing"
          elif regex.fullmatch(r"^[bcdfghklmnpqrstvwxz]+[aeiou][bcdfgklmnpqrstvz]$", title):
            verb_present_participle = title + title[-1] + "ing"
          else:
            verb_present_participle = title + "ing"
          if title.endswith("e"):
            verb_past = title + "d"
            verb_past_participle = title + "d"
          elif regex.search(r"([^aeiou])y$", title):
            verb_past = title[:-1] + "ied"
            verb_past_participle = title[:-1] + "ied"
          elif regex.fullmatch(r"^[bcdfghklmnpqrstvwxz]+[aeiou][bcdfgklmnpqrstvz]$", title):
            verb_past = title + title[-1] + "ed"
            verb_past_participle = title + title[-1] + "ed"
          else:
            verb_past = title + "ed"
            verb_past_participle = title + "ed"
          if values == ["++"] or values == ["++", "++"] or values == ["++", "++", "~"]:
            verb_present_participle = title + title[-1] + "ing"
            verb_past = title + title[-1] + "ed"
            verb_past_participle = title + title[-1] + "ed"
            values = []
          if len(values) > 0 and (values[0] == "+" or values[0].startswith("~")):
            values[0] = verb_singular
          if len(values) > 0 and values[0] == "++":
            values[0] = verb_singular
          if len(values) > 1 and (values[1] == "+" or values[1].startswith("~")):
            values[1] = verb_present_participle
          if len(values) > 1 and values[1] == "++":
            values[1] = title + title[-1] + "ing"
          if len(values) > 2 and (values[2] == "+" or values[2].startswith("~")):
            values[2] = verb_past
          if len(values) > 2 and values[2] == "++":
            values[2] = title + title[-1] + "ed"
          if len(values) > 3 and (values[3] == "+" or values[3].startswith("~")):
            values[3] = verb_past_participle
          if len(values) > 3 and values[3] == "++":
            values[3] = title + title[-1] + "ed"
          elif len(values) == 1 and values[0] == "es":
            verb_singular = title + "es"
          elif len(values) == 1 and values[0] == "d":
            verb_past = title + "d"
            verb_past_participle = title + "d"
          elif len(values) == 1 and values[0] == "ing":
            verb_present_participle = title + "ing"
          elif len(values) == 1 and values[0] == "ies":
            stem = regex.sub(r"([^aeiou])y$", r"\1", title)
            verb_singular = stem + "ies"
          elif len(values) == 1:
            verb_present_participle = values[0] + "ing"
            verb_past = values[0] + "ed"
            verb_past_participle = values[0] + "ed"
          elif len(values) == 2 and values[1] == "es":
            verb_singular = values[0] + "es"
            verb_present_participle = values[0] + "ing"
            verb_past = values[0] + "ed"
            verb_past_participle = values[0] + "ed"
          elif len(values) == 2 and values[1] == "ies":
            verb_singular = values[0] + "ies"
            verb_present_participle = values[0] + "ying"
            verb_past = values[0] + "ied"
            verb_past_participle = values[0] + "ied"
          elif len(values) == 2 and values[1] == "ed":
            verb_singular = title + "s"
            verb_present_participle = values[0] + "ing"
            verb_past = values[0] + "ed"
            verb_past_participle = values[0] + "ed"
          elif len(values) == 2 and values[1] == "d":
            verb_singular = values[0] + "es"
            verb_present_participle = values[0] + "ing"
            verb_past = values[0] + "d"
            verb_past_participle = values[0] + "d"
          elif len(values) == 2 and values[1] == "ing":
            verb_singular = values[0] + "es"
            verb_present_participle = values[0] + "ing"
            verb_past = values[0] + "ed"
            verb_past_participle = values[0] + "ed"
          elif len(values) == 2:
            verb_singular = values[0]
            verb_present_participle = values[1]
            stem = regex.sub(r"e$", "", title)
            verb_past = stem + "ed"
            verb_past_participle = stem + "ed"
          elif len(values) == 3 and values[2] == "es":
            verb_singular = values[0] + values[1] + "es"
            verb_present_participle = values[0] + values[1] + "ing"
            verb_past = values[0] + values[1] + "ed"
            verb_past_participle = values[0] + values[1] + "ed"
          elif len(values) == 3 and values[1] == "i" and values[2] == "ed":
            verb_singular = values[0] + "ies"
            verb_present_participle = values[0] + "ying"
            verb_past = values[0] + "ied"
            verb_past_participle = values[0] + "ied"
          elif len(values) == 3 and values[2] == "ed":
            verb_present_participle = values[0] + values[1] + "ing"
            verb_past = values[0] + values[1] + "ed"
            verb_past_participle = values[0] + values[1] + "ed"
          elif len(values) == 3 and values[1] == "y" and values[2] == "ing":
            verb_singular = values[0] + "ies"
            verb_present_participle = values[0] + "ying"
            verb_past = values[0] + "ied"
            verb_past_participle = values[0] + "ied"
          elif len(values) == 3 and len(values[1]) == 1 and values[2] == "ing":
            verb_present_participle = values[0] + values[1] + "ing"
            verb_past = values[0] + values[1] + "ed"
            verb_past_participle = values[0] + values[1] + "ed"
          elif len(values) == 3:
            verb_singular = values[0]
            verb_present_participle = values[1]
            verb_past = values[2]
            verb_past_participle = values[2]
          elif len(values) == 4:
            verb_singular = values[0]
            verb_present_participle = values[1]
            verb_past = values[2]
            verb_past_participle = values[3]
        past_alt = value_attrs.get("past2")
        if past_alt:
          if verb_past:
            verb_past = verb_past + ", " + past_alt
          if verb_past_participle:
            verb_past_participle = verb_past_participle + ", " + past_alt
      if regex.search(r"\{\{en-adj\|?([^\}]*)\}\}", line):
        if "adjective" in infl_modes: continue
        infl_modes.add("adjective")
        value = regex.sub(r".*\{\{en-adj[a-z]*\|?([^\}]*)\}\}.*", r"\1", line).strip()
        value = regex.sub(r"\[\[:en:#[^\]]*?\|(.*?)\]\]", r"\1", value)
        values = value.split("|") if value else []
        values = self.TrimInflections(values)
        stop = False
        if values and values[0].startswith("head="):
          if values[0][5:] != title:
            stop = True
          values.pop(0)
        if title.find(" ") >= 0 and len(values) != 2:
          stop = True
        for value in values:
          if value.startswith("head="):
            stop = True
          if value in ("+", "-", "~", "?", "!"):
            stop = True
        if not stop:
          if len(values) == 1 and values[0] == "further":
            values = []
          if len(values) >= 2 and values[-1] == "further":
            values = values[:-1]
          adjective_comparative = None
          adjective_superlative = None
          stem = title
          stem = regex.sub(r"e$", "", stem)
          stem = regex.sub(r"([^aeiou])y$", r"\1i", stem)
          if len(values) == 1 and values[0] == "er":
            adjective_comparative = stem + "er"
            adjective_superlative = stem + "est"
          elif len(values) == 1 and values[0].endswith("er"):
            adjective_comparative = values[0]
            adjective_superlative = values[0][:-2] + "est"
          elif len(values) == 2 and values[0] == "er":
            adjective_comparative = stem + "er"
            adjective_superlative = stem + "est"
          elif len(values) == 2 and values[1] == "er":
            if values[0] in ("-", "more"):
              adjective_comparative = stem + "er"
              adjective_superlative = stem + "est"
            else:
              adjective_comparative = values[0] + "er"
              adjective_superlative = values[0] + "est"
          elif len(values) == 2 and values[0] in ("-", "~") and values[1] in "more":
            pass
          elif len(values) == 2 and values[0] == "more" and values[1] in ("-", "~"):
            pass
          elif len(values) == 2 and values[0] == "r" and values[1] == "more":
            adjective_comparative = title + "r"
            adjective_superlative = ""
          elif len(values) == 2 and values[0] == "er" and values[1] == "more":
            adjective_comparative = stem + "er"
            adjective_superlative = stem + "est"
          elif len(values) == 2 and values[0] == "more" and values[1] != "most":
            adjective_comparative = values[1]
            adjective_superlative = regex.sub("er$", "est", values[1])
          elif len(values) == 2:
            adjective_comparative = values[0]
            adjective_superlative = values[1]
          if adjective_comparative == "-":
            adjective_comparative = ""
          if adjective_superlative == "-":
            adjective_superlative = ""
          if adjective_superlative == "more":
            adjective_superlative = regex.sub("er$", "est", adjective_comparative)
          if adjective_comparative and adjective_comparative.startswith("more "):
            adjective_comparative = ""
          if adjective_superlative and adjective_superlative.startswith("most "):
            adjective_superlative = ""
      if regex.search(r"\{\{en-adv\|?([^\}]*)\}\}", line):
        if "adverb" in infl_modes: continue
        infl_modes.add("adverb")
        value = regex.sub(r".*\{\{en-adv[a-z]*\|?([^\}]*)\}\}.*", r"\1", line).strip()
        value = regex.sub(r"\[\[:en:#[^\]]*?\|(.*?)\]\]", r"\1", value)
        values = value.split("|") if value else []
        values = self.TrimInflections(values)
        stop = False
        if values and values[0].startswith("head="):
          if values[0][5:] != title:
            stop = True
          values.pop(0)
        if title.find(" ") >= 0 and len(values) != 2:
          stop = True
        for value in values:
          if value.startswith("head="):
            stop = True
          if value in ("+", "-", "~", "?", "!"):
            stop = True
        if not stop:
          if len(values) == 1 and values[0] == "further":
            values = []
          if len(values) >= 2 and values[-1] == "further":
            values = values[:-1]
          adverb_comparative = None
          adverb_superlative = None
          stem = title
          stem = regex.sub(r"e$", "", stem)
          stem = regex.sub(r"([^aeiou])y$", r"\1i", stem)
          if len(values) == 1 and values[0] == "er":
            adverb_comparative = stem + "er"
            adverb_superlative = stem + "est"
          elif len(values) == 1 and values[0].endswith("er"):
            adverb_comparative = values[0]
            adverb_superlative = values[0][:-2] + "est"
          elif len(values) == 2 and values[1] == "er":
            if values[0] in ("-", "more"):
              adverb_comparative = stem + "er"
              adverb_superlative = stem + "est"
            else:
              adverb_comparative = values[0] + "er"
              adverb_superlative = values[0] + "est"
          elif len(values) == 2 and values[0] in ("-", "~") and values[1] == "more":
            pass
          elif len(values) == 2 and values[0] == "more" and values[1] in ("-", "~"):
            pass
          elif len(values) == 2 and values[0] == "r" and values[1] == "more":
            adverb_comparative = title + "r"
            adverb_superlative = ""
          elif len(values) == 2 and values[0] == "er" and values[1] == "more":
            adverb_comparative = stem + "er"
            adverb_superlative = stem + "est"
          elif len(values) == 2 and values[0] == "more" and values[1] != "most":
            adverb_comparative = values[1]
            adverb_superlative = regex.sub("er$", "est", values[1])
          elif len(values) == 2:
            adverb_comparative = values[0]
            adverb_superlative = values[1]
          if adverb_comparative == "-":
            adverb_comparative = ""
          if adverb_superlative == "-":
            adverb_superlative = ""
          if adverb_superlative == "more":
            adverb_superlative = regex.sub("er$", "est", adverb_comparative)
          if adverb_comparative and adverb_comparative.startswith("more "):
            adverb_comparative = ""
          if adverb_superlative and adverb_superlative.startswith("most "):
            adverb_superlative = ""
    ipa = ipa_us or ipa_misc
    if ipa and ipa not in ("...", "?"):
      output.append("pronunciation_ipa={}".format(ipa))
    if self.IsGoodInflection(noun_plural):
      output.append("inflection_noun_plural={}".format(noun_plural))
    if self.IsGoodInflection(verb_singular):
      output.append("inflection_verb_singular={}".format(verb_singular))
    if self.IsGoodInflection(verb_present_participle):
      output.append("inflection_verb_present_participle={}".format(verb_present_participle))
    if self.IsGoodInflection(verb_past):
      output.append("inflection_verb_past={}".format(verb_past))
    if self.IsGoodInflection(verb_past_participle):
      output.append("inflection_verb_past_participle={}".format(verb_past_participle))
    if self.IsGoodInflection(adjective_comparative):
      output.append("inflection_adjective_comparative={}".format(adjective_comparative))
    if self.IsGoodInflection(adjective_superlative):
      output.append("inflection_adjective_superlative={}".format(adjective_superlative))
    if self.IsGoodInflection(adverb_comparative):
      output.append("inflection_adverb_comparative={}".format(adverb_comparative))
    if self.IsGoodInflection(adverb_superlative):
      output.append("inflection_adverb_superlative={}".format(adverb_superlative))
    if etym_prefix:
      output.append("etymology_prefix={}".format(etym_prefix))
    if etym_core:
      output.append("etymology_core={}".format(etym_core))
    if etym_suffix:
      output.append("etymology_suffix={}".format(etym_suffix))
    alternatives = []
    for mode, lines in sections:
      translation = translations.get(mode)
      if translation:
        del translations[mode]
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
      if translation:
        self.OutputTranslation(mode, translation, output)
      last_mode = mode
      last_tran = translation
      cat_lines = []
      for line in lines:
        if cat_lines and line.startswith("|"):
          cat_lines[:-1] += line
        else:
          cat_lines.append(line)
      current_text = ""
      last_level = 0
      for line in cat_lines:
        if line.find("{{lb|en|obsolete}}") >= 0: continue
        if mode == "alternative":
          for alt in regex.findall(r"\{\{l\|en\|([- \p{Latin}]+?)\}\}", line):
            alternatives.append(alt)
          for alt in regex.findall(r"\[\[([- \p{Latin}]+?)\]\]", line):
            alternatives.append(alt)
          continue
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
        text = self.MakePlainText(text)
        if text.startswith("cf."): continue
        if tran_mode:
          if not regex.search(r"[\p{Hiragana}\p{Katakana}\p{Han}]", text): continue
          text = regex.sub(r"Japanese:", "", text).strip()
        eff_text = regex.sub(r"\(.*?\)", "", text).strip()
        if not regex.search(r"(\p{Latin}{2,})|([\p{Han}\p{Hiragana}|\p{Katakana}ー])", eff_text):
          continue
        if level <= 1:
          if current_text:
            output.append("{}={}".format(mode, current_text))
          current_text = text
        elif current_text:
          if level == 2:
            sep = "[-]"
          elif level == 3:
            sep = "[--]"
          else:
            sep = "[---]"
          current_text += " " + sep + " " + text
      if not regex.search(
          r"([\p{Latin}0-9]{2,}|[\p{Han}\p{Hiragana}\p{Katakana}])", current_text):
        continue
      output.append("{}={}".format(mode, current_text))
    cram_title = regex.sub(r"[-_ ]", "", title)
    for also in alsos:
      if (also != title and regex.sub(r"[-_ ]", "", also) == cram_title and
          regex.fullmatch("[\p{Latin}\d][- \p{Latin}\d']*[\p{Latin}\d]", also)):
        alternatives.append(also)
    if alternatives:
      uniq_alts = set()
      out_alts = []
      for alt in alternatives:
        if alt in uniq_alts: continue
        uniq_alts.add(alt)
        out_alts.append(alt)
      output.append("alternative={}".format(", ".join(out_alts)))
    for rel in ((synonyms, "synonym"), (hypernyms, "hypernym"), (hyponyms, "hyponym"),
                (antonyms, "antonym"), (derivatives, "derivative"), (relations, "relation")):
      if rel[0]:
        output.append("{}={}".format(rel[1], ", ".join(rel[0])))
    if output:
      if tran_mode:
        output.append("mode=translation")
      print("word={}\t{}".format(title, "\t".join(output)))

  def ConcatNestLines(self, text):
    segments = []
    level = 0
    while True:
      beg_pos = text.find("{{")
      end_pos = text.find("}}")
      if end_pos >= 0 and (beg_pos < 0 or end_pos < beg_pos):
        segments.append((level, text[:end_pos+2]))
        text = text[end_pos+2:]
        level -= 1
      elif beg_pos >= 0:
        segments.append((level, text[:beg_pos+2]))
        text = text[beg_pos+2:]
        level += 1
      else:
        segments.append((level, text))
        break
    new_segments = []
    for level, segment in segments:
      if level > 0:
        segment = segment.replace("\n", " ")
      new_segments.append(segment)
    return "".join(new_segments)

  def IsGoodInflection(self, text):
    if not text: return False
    if text in ("-" or "~"): return False
    if regex.search("[\?\!=/\(\)]", text): return False
    return True

  def OutputTranslation(self, mode, translation, output):
    tran_map = {}
    for source, target in translation:
      values = tran_map.get(source) or []
      values.append(target)
      tran_map[source] = values
    for source, targets in tran_map.items():
      source = self.MakePlainText(source)
      source = regex.sub(r"\(.*?\)", "", source)
      source = regex.sub(r"[\s+\(\)\[\]\{\}]", " ", source)
      source = regex.sub(r"[\s+]", " ", source).strip()
      trans = []
      for target in targets:
        for tr, expr in regex.findall(r"\{\{(t|t\+|t-simple)\|ja\|(.*?)\}\}", target):
          fields = expr.split("|")
          tran = self.MakePlainText(fields[0])
          if tran:
            trans.append(tran)
          for field in fields[1:]:
            if field.startswith("alt="):
              tran = self.MakePlainText(regex.sub(r"[a-z]+=", "", field))
              if tran:
                trans.append(tran)
      uniq_trans = set()
      out_trans = []
      for tran in trans:
        if not regex.search(r"[\p{Han}\p{Hiragana}\p{Katakana}]", tran):
          continue
        norm_tran = tran.lower()
        if norm_tran in uniq_trans:
          continue
        uniq_trans.add(norm_tran)
        out_trans.append(tran)
      if out_trans:
        if source:
          output.append("{}=[translation]: ({}) {}".format(mode, source, ", ".join(out_trans)))
        else:
          output.append("{}=[translation]: {}".format(mode, ", ".join(out_trans)))

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

  def TrimInflections(self, values, attrs=None):
    trimmed_values = []
    for value in values:
      value = regex.sub(r"\[\[([^\]]+)\]\]", r"\1", value)
      value = value.replace(r"'''", "")
      value = value.replace(r"''", "")
      if regex.search(" or ", value):
        value = regex.sub(" or .*", "", value)
      value = regex.sub(r"^sup=", "", value)
      value = regex.sub(r",.*", "", value)
      match = regex.search(r"^(past[0-9])=(.*)", value)
      if match:
        attr_value = match.group(2).strip()
        if attrs != None and attr_value:
          attrs[match.group(1)] = attr_value
        continue
      if regex.search(r"^[a-z_]+[234]([a-z0-9_]+)?=", value):
        continue
      if regex.search(r"^(past|pres)[a-z0-9_]*=", value):
        continue
      trimmed_values.append(value.strip())
    return trimmed_values


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
