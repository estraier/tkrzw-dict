#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to parse Japanese Wiktionary XML stream and export word information
#
# Usage:
#   parse_wiktionary_ja.py [--sampling num] [--max num] [--quiet]
#   (It reads the standard input and prints the result on the standard output.)
#
# Example:
#   $ bzcat jawiktionary-latest-pages-articles.xml.bz2 |
#     ./parse_wikipedia_ja.py > wiktionary-ja.tsv
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
    if not regex.search(r"^[-\p{Latin}0-9 ]+$", title): return
    fulltext = html.unescape(self.text)
    fulltext = regex.sub(r"<!--.*?-->", "", fulltext)
    fulltext = regex.sub(r"(\n==+[^=]+==+)", "\\1\n", fulltext)
    output = []
    is_eng_head = False
    is_eng_cat = False
    eng_head_level = 0
    mode = ""
    submode = ""
    infl_modes = set()
    sections = []
    synonyms = []
    hypernyms = []
    hyponyms = []
    antonyms = []
    derivatives = []
    relations = []
    for line in fulltext.split("\n"):
      line = line.strip()
      if regex.search(r"^==([^=]+)==$", line):
        lang = regex.sub(r"^==([^=]+)==$", r"\1", line).strip()
        lang = lang.lower()
        if lang in ("{{en}}", "{{eng}}", "{{english}}", "英語", "english", "{{l|en}}"):
          is_eng_head = True
        elif lang.startswith("{{") or lang.endswith("語"):
          is_eng_head = False
          is_eng_cat = False
        mode = ""
        submode = ""
      elif regex.search(r"^===([^=]+)===$", line):
        mode = regex.sub(r"^===([^=]+)===$", r"\1", line).strip()
        mode = regex.sub(r":.*", "", mode).strip()
        mode = mode.lower()
        sections.append((mode,[]))
        submode = ""
      elif regex.search(r"^====+([^=]+)=+===$", line):
        submode = regex.sub(r"^====+([^=]+)=+===$", r"\1", line).strip()
        submode = regex.sub(r":.*", "", submode).strip()
        submode = submode.lower()
        if submode in ("{{noun}}", "{{name}}", "noun", "名詞", "固有名詞", "人名", "地名",
                       "{{verb}}", "verb", "動詞", "自動詞", "他動詞",
                       "{{adj}}", "{{adjective}}", "adjective", "形容詞",
                       "{{adv}}", "{{adverb}}", "adverb", "副詞",
                       "{{pronoun}}", "{{auxverb}}", "{{prep}}", "{{article}}, {{interj}}",
                       "{{pron}}", "{{pron|en}}", "{{pron|eng}}", "発音"):
          mode = submode
          sections.append((mode,[]))
          submode = ""
      elif regex.search(r"^\[\[category:(.*)\]\]$", line, regex.IGNORECASE):
        lang = regex.sub(r"^\[\[category:(.*)\]\]$", r"\1", line, flags=regex.IGNORECASE)
        if lang in ("{{en}}", "{{eng}}") or lang.find("英語") >= 0:
          is_eng_cat = True
        elif regex.search(r"^\{\{[a-z]{2,3}\}\}$", lang) or lang.find("語") >= 0:
          is_eng_cat = False
      elif (is_eng_head or is_eng_cat):
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
        if CheckMode(("{{syn}}", "synonym", "類義語")):
          rel_words = synonyms
        elif CheckMode(("{{hyper}}", "hypernym", "上位語")):
          rel_words = hypernyms
        elif CheckMode(("{{hypo}}", "hyponym", "下位語")):
          rel_words = hyponyms
        elif CheckMode(("{{ant}}", "antonym", "対義語")):
          rel_words = antonyms
        elif CheckMode(("{{derived}}", "{{drv}}", "derived terms", "derived term", "派生語")):
          rel_words = derivatives
        elif CheckMode(("{{rel}}", "related terms", "related term", "関連語")):
          rel_words = relations
        if rel_words != None:
          for rel_word in regex.findall(r"\{\{l\|en\|([- \p{Latin}]+?)\}\}", line):
            rel_words.append(rel_word)
          for rel_word in regex.findall(r"\[\[([- \p{Latin}]+?)\]\]", line):
            rel_words.append(rel_word)
    pronunciation_ipa_us = ""
    pronunciation_ipa_misc = ""
    pronunciation_sampa_us = ""
    pronunciation_sampa_misc = ""
    alternatives = []
    for mode, lines in sections:
      mode = regex.sub(r":.*", "", mode).strip()
      mode = regex.sub(r"[0-9]+$", "", mode).strip()
      if regex.search(r"^\{\{(pron|発音)(\|(en|eng))?[0-9]?\}\}[0-9]?$", mode) or mode == "発音":
        mode = "pronunciation"
      elif mode in ("{{noun}}", "{{name}}", "noun", "名詞", "固有名詞", "人名", "地名"):
        mode = "noun"
      elif mode in ("{{verb}}", "verb", "動詞", "自動詞", "他動詞"):
        mode = "verb"
      elif mode in ("{{adj}}", "{{adjective}}", "adjective", "形容詞"):
        mode = "adjective"
      elif mode in ("{{adv}}", "{{adverb}}", "adverb", "副詞"):
        mode = "adverb"
      elif mode in ("{{pronoun}}", "pronoun", "代名詞", "人称代名詞", "指示代名詞",
                    "疑問代名詞", "関係代名詞"):
        mode = "pronoun"
      elif mode in ("{{aux}}", "{{auxverb}}", "auxiliary verb", "助動詞"):
        mode = "auxverb"
      elif mode in ("{{prep}}", "{{preposition}}", "preposition", "前置詞"):
        mode = "preposition"
      elif mode in ("{{det}}", "{{determiner}}", "determiner", "限定詞"):
        mode = "determiner"
      elif mode in ("{{article}}", "冠詞"):
        mode = "article"
      elif mode in ("{{interj}}", "{{interjection}}", "interjection", "間投詞", "感動詞"):
        mode = "interjection"
      elif mode in ("{{conj}}", "{{conjunction}}", "conjunction", "接続詞"):
        mode = "conjunction"
      elif mode in ("{{pref}}", "{{prefix}}", "prefix", "接頭辞"):
        mode = "prefix"
      elif mode in ("{{suf}}", "{{suffix}}", "suffix", "接尾辞"):
        mode = "suffix"
      elif mode in ("{{abbr}}", "{{abbreviation}}", "abbreviation", "略語"):
        mode = "abbreviation"
      elif mode in ("{{alter}}", "alternative", "alternative forms", "alternative form",
                    "代替", "代替語", "別表記", "異表記", "異綴", "異体"):
        mode = "alternative"
      else:
        mode = ""
      if mode == "pronunciation":
        for line in lines:
          if regex.search(r"\{\{ipa[0-9]?\|([^}|]+)(\|[^}|]+)*\}\}", line, regex.IGNORECASE):
            value = regex.sub(r".*\{\{ipa[0-9]?\|([^}|]+)(\|[^}|]+)*\}\}.*", r"\1",
                              line, flags=regex.IGNORECASE)
            value = self.TrimPronunciation(value, True)
            if value:
              if regex.search(r"(アメリカ|米)", line):
                pronunciation_ipa_us = value
              else:
                pronunciation_ipa_misc = value
          if regex.search(r"\{\{sampa\|([^}]+)\}\}", line, regex.IGNORECASE):
            value = regex.sub(r".*\{\{sampa\|([^}]+)\}\}.*", r"\1", line, flags=regex.IGNORECASE)
            value = self.TrimPronunciation(value, False)
            if value:
              if regex.search(r"(アメリカ|米)", line):
                pronunciation_sampa_us = value
              else:
                pronunciation_sampa_misc = value
          if regex.search(r"\{\{pron-en1\|([^\}]+)\}\}", line, regex.IGNORECASE):
            values = regex.sub(r".*\{\{pron-en1\|([^\}]+)\}\}.*", r"\1", line).split("|")
            if len(values) == 3:
              output.append("pronunciation_ahd={}".format(values[0]))
              output.append("pronunciation_ipa={}".format(values[1]))
              output.append("pronunciation_sampa={}".format(values[2]))
      elif mode:
        cat_lines = []
        for line in lines:
          if cat_lines and line.startswith("|"):
            cat_lines[:-1] += line
          else:
            cat_lines.append(line)
        current_text = ""
        last_level = 0
        for line in cat_lines:
          if line.startswith("--"): continue
          if line.find("{{lb|en|obsolete}}") >= 0: continue
          if ((regex.search("[^は]廃(語|用)", line) or line.find("{{label|en|archaic}}") >= 0) and
              not regex.search("(または|又は)", line)):
            continue
          if mode == "alternative":
            for alt in regex.findall(r"\{\{l\|en\|([- \p{Latin}]+?)\}\}", line):
              alternatives.append(alt)
            for alt in regex.findall(r"\[\[([- \p{Latin}]+?)\]\]", line):
              alternatives.append(alt)
            continue
          if regex.search(r"\{\{en-noun\|?([^\}]*)\}\}", line):
            if "noun" in infl_modes: continue
            infl_modes.add("noun")
            value = regex.sub(r".*\{\{en-noun\|?([^\}]*)\}\}.*", r"\1", line).strip()
            values = value.split("|") if value else []
            values = self.TrimInflections(values)
            stop = False
            for value in values:
              if value.startswith("head="):
                stop = True
            if not stop:
              plural = title + "s"
              if len(values) == 1 and values[0] == "es":
                plural = title + "es"
              elif len(values) == 1 and values[0] == "~":
                pass
              elif len(values) == 1 and values[0] == "-":
                plural = None
              elif len(values) == 1:
                plural = values[0]
              elif (len(values) == 2 and values[0] in ("-", "~") and
                    values[1] != "s" and values[1] != "es" and values[1] != "?"):
                plural = values[1]
              elif len(values) == 2 and values[1] == "es":
                stem = title if values[0] in ("-", "~") else values[0]
                plural = stem + "es"
              elif len(values) == 2 and values[1] == "ies":
                stem = title if values[0] in ("-", "~") else values[0]
                plural = stem + "ies"
              elif len(values) == 1 and values[0].startswith("pl="):
                plural = regex.sub(".*=", "", values[0])
              elif len(values) == 2 and values[0].startswith("sg=") and values[1] == "es":
                plural = title + "es"
              elif (len(values) == 2 and
                    values[0].startswith("sg=") and values[1].startswith("pl=")):
                plural = regex.sub(".*=", "", values[1])
              if self.IsGoodInflection(plural):
                output.append("inflection_noun_plural={}".format(plural))
          if regex.search(r"\{\{en-verb\|?([^\}]*)\}\}", line):
            if "verb" in infl_modes: continue
            infl_modes.add("verb")
            value = regex.sub(r".*\{\{en-verb\|?([^\}]*)\}\}.*", r"\1", line).strip()
            values = value.split("|") if value else []
            values = self.TrimInflections(values)
            stop = False
            if values and values[0].startswith("head="):
              if values[0][5:] != title:
                stop = True
              values.pop(0)
            for value in values:
              if value.startswith("head="):
                stop = True
            if not stop:
              singular = title + "s"
              present_participle = title + "ing"
              past = title + "ed"
              past_participle = title + "ed"
              if len(values) == 1 and values[0] == "es":
                singular = title + "es"
              elif len(values) == 1 and values[0] == "d":
                past = title + "d"
                past_participle = title + "d"
              elif len(values) == 1 and values[0] == "ing":
                present_participle = title + "ing"
              elif len(values) == 1:
                present_participle = values[0] + "ing"
                past = values[0] + "ed"
                past_participle = values[0] + "ed"
              elif len(values) == 2 and values[1] == "es":
                singular = values[0] + "es"
                present_participle = values[0] + "ing"
                past = values[0] + "ed"
                past_participle = values[0] + "ed"
              elif len(values) == 2 and values[1] == "ies":
                singular = values[0] + "ies"
                present_participle = values[0] + "ying"
                past = values[0] + "ied"
                past_participle = values[0] + "ied"
              elif len(values) == 2 and values[1] == "d":
                singular = values[0] + "s"
                present_participle = values[0] + "ing"
                past = values[0] + "d"
                past_participle = values[0] + "d"
              elif len(values) == 2 and values[1] == "ing":
                singular = values[0] + "es"
                present_participle = values[0] + "ing"
                past = values[0] + "ed"
                past_participle = values[0] + "ed"
              elif len(values) == 2:
                singular = values[0]
                present_participle = values[1]
                stem = regex.sub(r"e$", "", title)
                past = stem + "ed"
                past_participle = stem + "ed"
              elif len(values) == 3 and values[2] == "es":
                singular = values[0] + values[1] + "es"
                present_participle = values[0] + values[1] + "ing"
                past = values[0] + values[1] + "ed"
                past_participle = values[0] + values[1] + "ed"
              elif len(values) == 3 and values[1] == "i" and values[2] == "ed":
                singular = values[0] + "ies"
                present_participle = values[0] + "ying"
                past = values[0] + "ied"
                past_participle = values[0] + "ied"
              elif len(values) == 3 and values[2] == "ed":
                present_participle = values[0] + values[1] + "ing"
                past = values[0] + values[1] + "ed"
                past_participle = values[0] + values[1] + "ed"
              elif len(values) == 3 and values[1] == "k" and values[2] == "ing":
                present_participle = values[0] + "king"
              elif len(values) == 3 and values[1] == "n" and values[2] == "ing":
                present_participle = values[0] + "ning"
              elif len(values) == 3 and values[1] == "y" and values[2] == "ing":
                singular = values[0] + "ies"
                present_participle = values[0] + "ying"
                past = values[0] + "ied"
                past_participle = values[0] + "ied"
              elif len(values) == 3:
                singular = values[0]
                present_participle = values[1]
                past = values[2]
                past_participle = values[2]
              elif len(values) == 4:
                singular = values[0]
                present_participle = values[1]
                past = values[2]
                past_participle = values[3]
              if self.IsGoodInflection(singular):
                output.append("inflection_verb_singular={}".format(singular))
              if self.IsGoodInflection(present_participle):
                output.append("inflection_verb_present_participle={}".format(present_participle))
              if self.IsGoodInflection(past):
                output.append("inflection_verb_past={}".format(past))
              if self.IsGoodInflection(past_participle):
                output.append("inflection_verb_past_participle={}".format(past_participle))
          if regex.search(r"\{\{en-adj\|?([^\}]*)\}\}", line):
            if "adjective" in infl_modes: continue
            infl_modes.add("adjective")
            value = regex.sub(r".*\{\{en-adj\|?([^\}]*)\}\}.*", r"\1", line).strip()
            values = value.split("|") if value else []
            values = self.TrimInflections(values)
            stop = False
            if values and values[0].startswith("head="):
              if values[0][5:] != title:
                stop = True
              values.pop(0)
            for value in values:
              if value.startswith("head="):
                stop = True
            if not stop:
              comparative = None
              superlative = None
              if len(values) == 1 and values[0] == "er":
                stem = title
                stem = regex.sub(r"e$", "", stem)
                stem = regex.sub(r"([^aeiou])y$", r"\1i", stem)
                comparative = stem + "er"
                superlative = stem + "est"
              elif len(values) == 1 and values[0].endswith("er"):
                comparative = values[0]
                superlative = values[0][:-2] + "est"
              elif len(values) == 2 and values[1] == "er":
                comparative = values[0] + "er"
                superlative = values[0] + "est"
              elif len(values) == 2 and values[0] == "r" and values[1] == "more":
                comparative = title + "r"
                superlative = ""
              elif len(values) == 2 and values[0] == "er" and values[1] == "more":
                comparative = title + "er"
                superlative = ""
              elif len(values) == 2:
                comparative = values[0]
                superlative = values[1]
              if title not in ["many", "much"]:
                if comparative in ["more", "most"]:
                  comparative = ""
                if superlative in ["more", "most"]:
                  superlative = ""
              if self.IsGoodInflection(comparative):
                output.append("inflection_adjective_comparative={}".format(comparative))
              if self.IsGoodInflection(superlative):
                output.append("inflection_adjective_superlative={}".format(superlative))
          if regex.search(r"\{\{en-adv\|?([^\}]*)\}\}", line):
            if "adverb" in infl_modes: continue
            infl_modes.add("adverb")
            value = regex.sub(r".*\{\{en-adv\|?([^\}]*)\}\}.*", r"\1", line).strip()
            values = value.split("|") if value else []
            values = self.TrimInflections(values)
            stop = False
            if values and values[0].startswith("head="):
              if values[0][5:] != title:
                stop = True
              values.pop(0)
            for value in values:
              if value.startswith("head="):
                stop = True
            if not stop:
              comparative = None
              superlative = None
              if len(values) == 1 and values[0] == "er":
                stem = title
                stem = regex.sub(r"e$", "", stem)
                stem = regex.sub(r"([^aeiou])y]$", r"\1i", stem)
                comparative = stem + "er"
                superlative = stem + "est"
              elif len(values) == 2 and values[1] == "er":
                comparative = values[0] + "er"
                superlative = values[0] + "est"
              elif len(values) == 1 and values[0].endswith("er"):
                comparative = values[0]
                superlative = values[0][:-2] + "est"
              elif len(values) == 2 and values[0] == "r" and values[1] == "more":
                comparative = title + "r"
                superlative = ""
              elif len(values) == 2 and values[0] == "er" and values[1] == "more":
                comparative = title + "er"
                superlative = ""
              elif len(values) == 2:
                comparative = values[0]
                superlative = values[1]
              if self.IsGoodInflection(comparative):
                output.append("inflection_adverb_comparative={}".format(comparative))
              if self.IsGoodInflection(superlative):
                output.append("inflection_adverb_superlative={}".format(superlative))
          if mode == "noun":
            if regex.search(r"\{\{p\}\} *:.*\[\[([a-zA-Z ]+)\]\]", line):
              value = regex.sub(r".*\{\{p\}\} *:.*\[\[([a-zA-Z ]+)\]\].*", r"\1", line)
              if value:
                output.append("inflection_noun_plural={}".format(value))
          if mode in ("adjective", "adverb"):
            if regex.search(
                r"比較級 *:.*\[\[([a-zA-Z ]+)\]\].*[,、].*最上級 *: *\[\[([a-zA-Z ]+)\]\]", line):
              values = regex.sub(
                r".*比較級 *:.*\[\[([a-zA-Z ]+)\]\].*[,、].*最上級 *: *\[\[([a-zA-Z ]+)\]\].*",
                "\\1\t\\2", line).split("\t")
              if (len(values) == 2 and
                  self.IsGoodInflection(values[0]) and self.IsGoodInflection(values[1])):
                output.append("inflection_{}_comparative={}".format(mode, values[0]))
                output.append("inflection_{}_superlative={}".format(mode, values[1]))
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
          eff_text = regex.sub(r"[\(（].*?[\)）]", "", text).strip()
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
        eff_text = regex.sub(r"[\(（].*?[\)）]", "", current_text).strip()
        if regex.search(r"([\p{Latin}0-9]{2,}|[\p{Han}\p{Hiragana}\p{Katakana}])", eff_text):
          output.append("{}={}".format(mode, current_text))
    pronunciation_ipa = pronunciation_ipa_us or pronunciation_ipa_misc
    if pronunciation_ipa:
      output.append("pronunciation_ipa={}".format(pronunciation_ipa))
    pronunciation_sampa = pronunciation_sampa_us or pronunciation_sampa_misc
    if pronunciation_sampa:
      output.append("pronunciation_sampa={}".format(pronunciation_sampa))
    num_effective_records = 0;
    for record in output:
      name, value = record.split("=", 1)
      if name not in (
          "noun", "verb", "adjective", "adverb",
          "pronoun", "auxverb", "preposition", "determiner", "article",
          "interjection", "conjunction",
          "prefix", "suffix", "abbreviation"):
        continue
      if regex.search(
          r"の(直接法|直説法|仮定法)?(現在|過去)?(第?[一二三]人称)?[ ・、]?" +
          r"(単数|複数|現在|過去|比較|最上|進行|完了|動名詞|単純)+[ ・、]?" +
          r"(形|型|分詞|級|動名詞)+", value):
        continue
      if regex.search(r"の(直接法|直説法|仮定法)(現在|過去)", value):
        continue
      if regex.search(r"の(動名詞|異綴|旧綴)", value):
        continue
      num_effective_records += 1
    if num_effective_records:
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
      print("word={}\t{}".format(title, "\t".join(output)))

  def IsGoodInflection(self, text):
    if not text: return False
    if text in ("-" or "~"): return False
    if regex.search("[\?\!=,/\(\)]", text): return False
    return True

  def MakePlainText(self, text):
    text = regex.sub(r"^[#\*]+", "", text)
    text = regex.sub(r"^--+", "", text)
    text = regex.sub(r"\{\{w\|(lang=[a-z]+\|)?([^\}\|]*)(\|[^\}]*)?\}\}", r"\2", text)
    text = regex.sub(r"\{\{ふりがな\|([^\}\|]+)(\|[^\}]+)?\}\}", r"\1", text)
    text = regex.sub(r"\{\{おくりがな\|(.*?)\|(.*?)\|(.*?)}\}", r"\1\2", text)
    text = regex.sub(r"\{\{おくりがな2\|(.*?)\|(.*?)\|(.*?)\|(.*?)}\}", r"\1\3", text)
    text = regex.sub(r"\{\{おくりがな3\|(.*?)\|(.*?)\|(.*?)\|(.*?)\|(.*?)\|(.*?)\|(.*?)}\}",
                     r"\1\3\4\6", text)
    text = regex.sub(r"\{\{(en-)?(noun)\}\}", r"名詞", text)
    text = regex.sub(r"\{\{(en-)?(verb)\}\}", r"動詞", text)
    text = regex.sub(r"\{\{(en-)?(adj|adjective)\}\}", r"形容詞", text)
    text = regex.sub(r"\{\{(en-)?(adv|adverb)\}\}", r"副詞", text)
    text = regex.sub(r"\{\{(en-)?(pronoun)\}\}", r"代名詞", text)
    text = regex.sub(r"\{\{(en-)?(auxverb)\}\}", r"助動詞", text)
    text = regex.sub(r"\{\{(en-)?(prep|preposition)\}\}", r"前置詞", text)
    text = regex.sub(r"\{\{(en-)?(det)\}\}", r"限定詞", text)
    text = regex.sub(r"\{\{(en-)?(article)\}\}", r"冠詞", text)
    text = regex.sub(r"\{\{(en-)?(interj|interjection)\}\}", r"間投詞", text)
    text = regex.sub(r"\{\{(en-)?(conj|conjunction)\}\}", r"接続詞", text)
    text = regex.sub(r"\{\{(en-)?(prefix)\}\}", r"接頭辞", text)
    text = regex.sub(r"\{\{(en-)?(suffix)\}\}", r"接尾辞", text)
    text = regex.sub(r"\{\{(en-)?(abbr|abbreviation)\}\}", r"略語", text)
    text = regex.sub(r"\{\{(en-)?(drv|derivative)\}\}", r"派生語", text)
    text = regex.sub(r"\{\{(en-)?(alter)\}\}", r"代替語", text)
    text = regex.sub(r"\{\{(en-)?(syn)\}\}", r"類義語", text)
    text = regex.sub(r"\{\{(en-)?(ant)\}\}", r"対義語", text)
    text = regex.sub(r"\{\{(en-)?(rel)\}\}", r"関連語", text)
    text = regex.sub(r"\{\{countable\}\}", r"可算", text)
    text = regex.sub(r"\{\{uncountable\}\}", r"不可算", text)
    text = regex.sub(r"\{\{countable(\|[^\}]+)*\}\}", r"（可算）", text)
    text = regex.sub(r"\{\{uncountable(\|[^\}]+)*\}\}", r"（不可算）", text)
    text = regex.sub(r"\{\{lb\|\en(\|\w+)*(\|countable\+?)(\|\w+)*\}\}", r"（可算）", text)
    text = regex.sub(r"\{\{lb\|\en(\|\w+)*(\|uncountable\+?)(\|\w+)*\}\}", r"（不可算）", text)
    text = regex.sub(r"\{\{intransitive\}\}", r"自動詞", text)
    text = regex.sub(r"\{\{transitive\}\}", r"他動詞", text)
    text = regex.sub(r"\{\{v\.i\.\}\}", r"自動詞", text)
    text = regex.sub(r"\{\{v\.t\.\}\}", r"他動詞", text)
    text = regex.sub(r"\{\{intransitive(\|[^\}]+)*\}\}", r"（自動詞）", text)
    text = regex.sub(r"\{\{context\|transitive(\|[^\}]+)*\}\}", r"（他動詞）", text)
    text = regex.sub(r"\{\{lb\|\en(\|\w+)*(\|transitive\+?)(\|\w+)*\}\}", r"（自動詞）", text)
    text = regex.sub(r"\{\{lb\|\en(\|\w+)*(\|intransitive\+?)(\|\w+)*\}\}", r"（他動詞）", text)
    text = regex.sub(r"\{\{タグ\|en\|自動詞\}\}", r"（自動詞）", text)
    text = regex.sub(r"\{\{タグ\|en\|他動詞\}\}", r"（他動詞）", text)
    text = regex.sub(r"\{\{\.\.\.\}\}", "...", text)
    text = regex.sub(r"(\{\{[^{}]+)\{\{[^{}]+\}\}([^}]*\}\})", r"\1\2", text)
    text = regex.sub(r"\{\{l\|[^\}\|]+\|([^\}]+)?\}\}", r"\1", text)
    text = regex.sub(r"\{\{(context|lb|タグ|tag|label|infl)\|[^\}]*\}\}", "", text)
    text = regex.sub(r"\{\{cat:[^\}]*\}\}", "", text)
    text = regex.sub(r"\{\{abbreviation of(\|en)?\|([^|}]+)\}\}", r"\2", text)
    text = regex.sub(r"\{\{(en-)?plural of(\|en)?\|([^|}]+)\}\}", r"\3の複数形", text)
    text = regex.sub(r"\{\{(en-)?third-person singular of(\|en)?\|([^|}]+)\}\}",
                     r"\3の三人称単数現在形", text)
    text = regex.sub(r"\{\{(en-)?past of(\|en)?\|([^|}]+)\}\}", r"\3の過去形", text)
    text = regex.sub(r"\{\{(en-)?present participle of(\|en)?\|([^|}]+)\}\}",
                     r"\3の現在分詞", text)
    text = regex.sub(r"\{\{(en-)?past participle of(\|en)?\|([^|}]+)\}\}", r"\3の過去分詞", text)
    text = regex.sub(r"\{\{(en-)?comparative of(\|en)?\|([^|}]+)\}\}", r"\3の複数形", text)
    text = regex.sub(r"\{\{(en-)?comparative of(\|en)?\|([^|}]+)\}\}", r"\3の比較級", text)
    text = regex.sub(r"\{\{(en-)?superlative of(\|en)?\|([^|}]+)\}\}", r"\3の最上級", text)
    text = regex.sub(r"\{\{(m|ux|l)\|[a-z]+\|([^\|\}]+)(\|[^\}\|]+)*\}\}", r"\2", text)
    text = regex.sub(r"\{\{(n-g|non-gloss definition)\|([^\|\}]+)(\|[^\}\|]+)*\}\}", r"\2", text)
    text = regex.sub(r"\{\{&lit\|en\|(.*?)\|(.*?)\|(.*?)(\|.*?)*?\}\}", r"cf. \1, \2, \3 ", text)
    text = regex.sub(r"\{\{&lit\|en\|(.*?)\|(.*?)(\|.*?)*?\}\}", r"cf. \1, \2 ", text)
    text = regex.sub(r"\{\{&lit\|en\|(.*?)(\|.*?)*?\}\}", r"cf. \1", text)
    text = regex.sub(r"\{\{(vern|taxlink)\|(.*?)(\|.*?)*\}\}", r"\2", text)
    text = regex.sub(r"\{\{syn of\|en\|(.*?)(\|.*?)*\}\}", r"Synonym of \1", text)
    text = regex.sub(r"\{\{syn\|en\|(.*?)\|(.*?)\|(.*?)(\|.*?)*?\}\}",
                     r"Synonyms: \1, \2, \3 ", text)
    text = regex.sub(r"\{\{syn\|en\|(.*?)\|(.*?)(\|.*?)*?\}\}", r"Synonyms: \1, \2 ", text)
    text = regex.sub(r"\{\{syn\|en\|(.*?)(\|.*?)*?\}\}", r"Synonym: \1 ", text)
    text = regex.sub(r"\{\{rfdate[a-z]+\|[a-z]+\|([^\|\}]+)(\|[^\}\|]+)*\}\}", r"\1", text)
    text = regex.sub(r"\{\{(RQ|Q):([^\|\}]+)(\|[^\|\}]+)*\|passage=([^\|\}]+)(\|[^\|\}]+)*\}\}",
                     r"\2 -- \4", text)
    text = regex.sub(r"\{\{(RQ|R):([^\|\}]+)(\|[^\}\|]+)*\}\}", r"\2", text)
    text = regex.sub(r"\{\{([^\}\|]+\|)([^\}\|]+)(\|[^\}]+)?\}\}", r"\2", text)
    text = regex.sub(r"\{\{([^}]*)\}\}", r"", text)
    text = regex.sub(r"\{\}", r"", text)
    text = regex.sub(r"\}\}", r"", text)
    text = regex.sub(r"\[\[w:[a-z]+:([^\]\|]+)(\|[^\]\|]+)?\]\]", r"\1", text)
    text = regex.sub(r"\[\[(category|カテゴリ):[^\]]*\]\]", "", text, regex.IGNORECASE)
    text = regex.sub(r"\[\[([^\]\|]+\|)?([^\]]*)\]\]", r"\2", text)
    text = regex.sub(r"\[(https?://[^ ]+ +)([^\]]+)\]", r"\2", text)
    text = regex.sub(r"\[https?://.*?\]", r"", text)
    text = regex.sub(r"\[\[", r"", text)
    text = regex.sub(r"\]\]", r"", text)
    text = regex.sub(r"'''", "", text)
    text = regex.sub(r"''", "", text)
    text = regex.sub(r"\( *\)", "", text)
    text = regex.sub(r"（ *）", "", text)
    text = regex.sub(r"「 *」", "", text)
    text = regex.sub(r"<ref>.*?</ref>", "", text)
    text = regex.sub(r"</?[a-z]+[^>]*>", "", text)
    text = regex.sub(r"<!-- *", "(", text)
    text = regex.sub(r" *-->", ")", text)
    text = regex.sub(r"^ *[,:;] *", "", text)
    return regex.sub(r"\s+", " ", text).strip()

  def TrimPronunciation(self, value, is_ipa):
    value = regex.sub(r"</?[a-z]+[^>]*>", "", value)
    value = regex.sub(r"^/(.*)/$", r"\1", value)
    value = regex.sub(r"lang=[a-z]*\|", "", value)
    value = regex.sub(r"[,\|].*", "", value)
    if is_ipa:
      value = regex.sub(r"^/(.*)/$", r"\1", value)
      value = regex.sub(r"/ ?\(.*", "", value)
      value = regex.sub(r"/", "", value)
    if value in ("...", "?"):
      return ""
    return value

  def TrimInflections(self, values):
    trimmed_values = []
    for value in values:
      value = regex.sub(r"\[\[([^\]]+)\]\]", r"\1", value)
      value = value.replace(r"'''", "")
      value = value.replace(r"''", "")
      value = regex.sub(r"(又|また).*", "", value)
      value = regex.sub(r",.*", "", value)
      if regex.search("^[a-z_]+[234](_[a-z_]+)=", value):
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
