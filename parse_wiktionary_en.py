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
    if regex.match(r"^[-\p{Latin}0-9 ]+/translations$", title):
      title = regex.sub(r"/.*", "", title)
      tran_mode = True
    if not regex.match(r"^[-\p{Latin}0-9 ]+$", title): return
    fulltext = html.unescape(self.text)
    fulltext = regex.sub(r"<!--.*?-->", "", fulltext)
    fulltext = regex.sub(r"(\n==+[^=]+==+)", "\\1\n", fulltext)
    output = []
    ipa_us = ""
    ipa_misc = ""
    noun_plural = ""
    verb_singular = ""
    verb_present_participle = ""
    verb_past = ""
    verb_past_participle = ""
    adjective_comparative = ""
    adjective_superative = ""
    adverb_comparative = ""
    adverb_superative = ""
    infl_modes = set()
    is_eng = False
    mode = ""
    submode = ""
    sections = []
    synonyms = []
    hypernyms = []
    hyponyms = []
    antonyms = []
    derivations = []
    relations = []
    translations = {}
    for line in fulltext.split("\n"):
      line = line.strip()
      if regex.search(r"^==([^=]+)==$", line):
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
                       "{{adv}}", "{{adverb}}", "adverb",
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
          rel_words = derivations
        elif CheckMode(("{{rel}}", "related terms", "related term", "関連語")):
          rel_words = relations
        if rel_words != None:
          for rel_word in regex.findall(r"\{\{l\|en\|([- \p{Latin}]+?)\}\}", line):
            rel_words.append(rel_word)
          for rel_word in regex.findall(r"\[\[([- \p{Latin}]+?)\]\]", line):
            rel_words.append(rel_word)
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
      if regex.search(r"\{\{ipa\|en\|([^}]+)\}\}", line, regex.IGNORECASE):
        value = regex.sub(r".*\{\{ipa\|en\|([^}]+)\}\}.*", r"\1",
                          line, flags=regex.IGNORECASE)
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
        value = regex.sub(r".*\{\{en-noun\|?([^\}]*)\}\}.*", r"\1", line).strip()
        values = value.split("|") if value else []
        values = self.TrimInflections(values)        
        stop = False
        for value in values:
          if value.startswith("head="):
            stop = True
        if not stop:
          noun_plural = title + "s"
          if len(values) == 1 and values[0] == "es":
            noun_plural = title + "es"
          elif len(values) == 1 and values[0] == "~":
            pass
          elif len(values) == 1 and values[0] == "-":
            noun_plural = None
          elif len(values) == 1:
            noun_plural = values[0]
          elif (len(values) == 2 and values[0] in ("-", "~") and
                values[1] != "s" and values[1] != "es" and values[1] != "?"):
            noun_plural = values[1]
          elif len(values) == 2 and values[0] == "es":
            noun_plural = title + "es"
          elif len(values) == 2 and values[1] == "es":
            stem = title if values[0] in ("-", "~") else values[0]
            noun_plural = stem + "es"
          elif len(values) == 2 and values[1] == "ies":
            stem = title if values[0] in ("-", "~") else values[0]
            noun_plural = stem + "ies"
          elif len(values) == 1 and values[0].startswith("pl="):
            noun_plural = regex.sub(".*=", "", values[0])
          elif len(values) == 2 and values[0].startswith("sg=") and values[1] == "es":
            noun_plural = title + "es"
          elif len(values) == 2 and values[0].startswith("sg=") and values[1].startswith("pl="):
            noun_plural = regex.sub(".*=", "", values[1])
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
          verb_singular = title + "s"
          verb_present_participle = title + "ing"
          verb_past = title + "ed"
          verb_past_participle = title + "ed"
          if len(values) == 1 and values[0] == "es":
            verb_singular = title + "es"
          elif len(values) == 1 and values[0] == "d":
            verb_past = title + "d"
            verb_past_participle = title + "d"
          elif len(values) == 1 and values[0] == "ing":
            verb_present_participle = title + "ing"
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
          elif len(values) == 3 and values[1] == "k" and values[2] == "ing":
            verb_present_participle = values[0] + "king"
          elif len(values) == 3 and values[1] == "n" and values[2] == "ing":
            verb_present_participle = values[0] + "ning"
          elif len(values) == 3 and values[1] == "y" and values[2] == "ing":
            verb_singular = values[0] + "ies"
            verb_present_participle = values[0] + "ying"
            verb_past = values[0] + "ied"
            verb_past_participle = values[0] + "ied"
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
          adjective_comparative = None
          adjective_superative = None
          if len(values) == 1 and values[0] == "er":
            stem = title
            stem = regex.sub(r"e$", "", stem)

            print("BEF", stem)
            
            stem = regex.sub(r"([^aeiou])y$", r"\1i", stem)

            print("AFT", stem)
            
            adjective_comparative = stem + "er"
            adjective_superative = stem + "est"
          elif len(values) == 1 and values[0].endswith("er"):
            adjective_comparative = values[0]
            adjective_superative = values[0][:-2] + "est"
          elif len(values) == 2 and values[1] == "er":
            adjective_comparative = values[0] + "er"
            adjective_superative = values[0] + "est"
          elif len(values) == 2 and values[0] == "r" and values[1] == "more":
            adjective_comparative = title + "r"
            adjective_superative = ""
          elif len(values) == 2 and values[0] == "er" and values[1] == "more":
            adjective_comparative = title + "er"
            adjective_superative = ""
          elif len(values) == 2:
            adjective_comparative = values[0]
            adjective_superative = values[1]
          if adjective_comparative == "-":
            adjective_comparative = ""
          if adjective_superative == "-":
            adjective_superative = ""
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
          adverb_comparative = None
          adverb_superative = None
          if len(values) == 1 and values[0] == "er":
            stem = title
            stem = regex.sub(r"e$", "", stem)
            stem = regex.sub(r"([^aeiou])y]$", r"\1i", stem)
            adverb_comparative = stem + "er"
            adverb_superative = stem + "est"
          elif len(values) == 1 and values[0].endswith("er"):
            adverb_comparative = values[0]
            adverb_superative = values[0][:-2] + "est"
          elif len(values) == 2 and values[1] == "er":
            adverb_comparative = values[0] + "er"
            adverb_superative = values[0] + "est"
          elif len(values) == 2 and values[0] == "r" and values[1] == "more":
            adverb_comparative = title + "r"
            adverb_superative = ""
          elif len(values) == 2 and values[0] == "er" and values[1] == "more":
            adverb_comparative = title + "er"
            adverb_superative = ""
          elif len(values) == 2:
            adverb_comparative = values[0]
            adverb_superative = values[1]
          if adverb_comparative == "-":
            adverb_comparative = ""
          if adverb_superative == "-":
            adverb_superative = ""
    ipa = ipa_us or ipa_misc  
    if ipa:
      output.append("pronunciation_ipa={}".format(ipa))
    if noun_plural and not regex.match("[\?\!]", noun_plural):
      output.append("inflection_noun_plural={}".format(noun_plural))
    if verb_singular:
      output.append("inflection_verb_singular={}".format(verb_singular))
    if verb_present_participle:
      output.append("inflection_verb_present_participle={}".format(verb_present_participle))
    if verb_past:
      output.append("inflection_verb_past={}".format(verb_past))
    if verb_past_participle:
      output.append("inflection_verb_past_participle={}".format(verb_past_participle))
    if adjective_comparative:
      output.append("inflection_adjective_comparative={}".format(adjective_comparative))
    if adjective_superative:
      output.append("inflection_adjective_superative={}".format(adjective_superative))
    if adverb_comparative:
      output.append("inflection_adverb_comparative={}".format(adverb_comparative))
    if adverb_superative:
      output.append("inflection_adverb_superative={}".format(adverb_superative))
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
      elif mode in ("{{adv}}", "{{adverb}}", "adverb"):
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
    if output:
      if alternatives:
        uniq_alts = set()
        out_alts = []
        for alt in alternatives:
          if alt in uniq_alts: continue
          uniq_alts.add(alt)
          out_alts.append(alt)
        output.append("alternative={}".format(", ".join(out_alts)))
      for rel in ((synonyms, "synonym"), (hypernyms, "hypernym"), (hyponyms, "hyponym"),
                  (antonyms, "antonym"), (derivations, "derivation"), (relations, "relation")):
        if rel[0]:
          output.append("{}={}".format(rel[1], ", ".join(rel[0])))
      if tran_mode:
        output.append("mode=translation")
      print("word={}\t{}".format(title, "\t".join(output)))

  def OutputTranslation(self, mode, translation, output):
    tran_map = {}
    for source, target in translation:
      values = tran_map.get(source) or []
      values.append(target)
      tran_map[source] = values
    for source, targets in tran_map.items():
      source = self.MakePlainText(source)
      source = regex.sub(r"\(.*?\)", "", source)
      source = regex.sub(r"[\s+\(\)\[\]\{\}]", " ", source).strip()
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
    text = regex.sub(r"\{\{syn\|en\|(.*?)\|(.*?)\|(.*?)(\|.*?)*?\}\}",
                     r"Synonyms: \1, \2, \3 ", text)
    text = regex.sub(r"\{\{syn\|en\|(.*?)\|(.*?)(\|.*?)*?\}\}", r"Synonyms: \1, \2 ", text)
    text = regex.sub(r"\{\{syn\|en\|(.*?)(\|.*?)*?\}\}", r"Synonym: \1 ", text)
    text = regex.sub(r"\{\{rfdate[a-z]+\|[a-z]+\|([^\|\}]+)(\|[^\}\|]+)*\}\}", r"\1", text)
    text = regex.sub(r"\{\{(RQ|Q):([^\|\}]+)(\|[^\|\}]+)*\|passage=([^\|\}]+)(\|[^\|\}]+)*\}\}",
                     r"\2 -- \4", text)
    text = regex.sub(r"\{\{(RQ|R):([^\|\}]+)(\|[^\}\|]+)*\}\}", r"\2", text)
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
    text = regex.sub(r"</?[a-z]+[^>]*>", "", text)
    text = regex.sub(r"<!-- *", "(", text)
    text = regex.sub(r" *-->", ")", text)
    text = regex.sub(r"^ *[,:;] *", "", text)
    return regex.sub(r"\s+", " ", text).strip()

  def TrimInflections(self, values):
    trimmed_values = []
    for value in values:
      value = regex.sub(r"\[\[([^\]]+)\]\]", r"\1", value)
      value = value.replace(r"'''", "")
      value = value.replace(r"''", "")
      if regex.search(" or ", value):
        value = regex.sub(" or .*", "", value)
      value = regex.sub(r"^sup=", "", value)
      value = regex.sub(r",.*", "", value)
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
