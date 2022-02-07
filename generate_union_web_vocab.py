#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to generate files to make a vocaburary book for Kindle
#
# Usage:
#   generate_union_kindle_vocab.py [--vocab str] [--body str] [--phrase str] [--output str]
#
# Example:
#   ./generate_union_kindle_vocab.py --vocab union-vocab.tsv --body union-body.tkh
#     --phrase enuunion-phrase.tkh --output union-dict-kindle-vocab
#     
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
import urllib
import uuid


logger = tkrzw_dict.GetLogger()
INTRO_TEXT = """* 概要
{TITLE}は、重要英単語を効率よく覚えるためのWebサイトです。日常でよく使われる重要な英単語{NUM_MAIN_WORDS}語をWebから自動抽出し、意味や用法が似た単語をまとめて覚えることができます。無作為に並べられた英単語を暗記するよりも、周辺の語から連想して記憶する方が、明らかに効率よく学習を進められます。派生語や熟語も含めて総数{NUM_UNIQ_WORDS}語を学習できます。
* 典型的な使い方
英単語は章毎にまとめられています。全部で{NUM_SECTIONS}章あります。1日に1章から3章程度に取り組むと良いでしょう。まず、索引ページから各章の「STUDY」リンクをクリックして、その章の単語の意味を学習しましょう。一通りの語の意味を覚えたら、「CHECK」リンクをクリックして、きちんと覚えられているかを確認します。
語彙の学習では反復が重要です。1日の学習の最初には、前回の範囲を確認ページで復習すると良いでしょう。おそらく何割かは忘れていることでしょうが、間を置いて再度学習することで、記憶の定着率が高まります。
既に他の方法で語彙学習をしている人や、2周目以降に取り組む人は、腕試しに確認ページだけに取り組むのも良いでしょう。
* 学習ページ
学習ページの各単語には、典型的な訳語のリストがつけられているので、それで大まかな意味を把握できます。詳細な語義が知りたい場合には、その下に並べてある英語の説明文を読んでください。日本語訳と英文の説明が両方読めるので、英和辞書と英英辞書の良いところを合わせた使い勝手になります。訳語だけを見て意味がわかったなら、英文の語義説明は読み飛ばしても構いません。
各単語の欄には、その単語の派生語や、その単語を含む重要熟語も載せられています。それらにも目を通すと意味や用法をより深く理解できるでしょう。とはいえ、派生語の意味は基本語の意味を覚えていれば自然に想起できるので、まず基本語の意味を覚えることを優先するとよいでしょう。
* 確認ページ
確認ページでは、各単語とその訳語が表になっています。初期状態では、訳語は透明になって隠されています。ポインタを訳語の欄に移動させると、その訳語が表示されます。各々の単語について、訳語を表示する前にだいたいの意味を想起して、声に出してみましょう。それから、それが正しいかどうかを訳語を表示して確認します。もし意味を覚えていなかったり記憶が間違っていたりしたら、その単語の欄をクリックしてハイライトをつけます。全ての単語でその作業を行います。全ての単語の確認が終わったら、ハイライトされた部分だけを重点的に復習します。
「CHANGE VIEW」をクリックすると、英単語が隠されて、訳語が表示されます。この状態で、訳語を見て英単語を想起できるかを確認します。この逆方向の確認も全ての単語で行います。英日方向、日英方向の両方で全ての単語が思い出せるようになれば、一連の学習は完了です。ダメ押しに、全ての英単語と訳語を一読するのもよいでしょう。
* ライセンス
収録された英単語とその語義は、WordNet、日本語WordNet、Wiktionary、EDictのデータとWeb上のコンテンツを組み合わせて生成されています。それぞれのライセンスについては以下のサイトを参照してください。
> WordNet Copyright 2021 The Trustees of Princeton University. -- https://wordnet.princeton.edu/
> Japanese Wordnet Copyright 2009-2011 NICT, 2012-2015 Francis Bond and 2016-2017 Francis Bond, Takayuki Kuribayashi. -- http://compling.hss.ntu.edu.sg/wnja/index.en.html
> Wiktionary data is copyrighted by each contributers and licensed under CC BY-SA and GFDL. -- https://en.wiktionary.org/ https://ja.wiktionary.org/
> EDict2 Copyright 2017 The Electronic Dictionary Research and Development Group. -- http://www.edrdg.org/jmdict/edict.html
統合辞書プロジェクト（https://dbmx.net/dict/）では、同じデータで作成した辞書検索システムやKindle用辞書も公開しています。
"""
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
  ("noun_plural", "複数"),
  ("verb_singular", "三単"),
  ("verb_present_participle", "現分"),
  ("verb_past", "過去"),
  ("verb_past_participle", "過分"),
  ("adjective_comparative", "形比"),
  ("adjective_superlative", "形最"),
  ("adverb_comparative", "副比"),
  ("adverb_superlative", "副最"),
]
STYLE_TEXT = """html,body,article,p,pre,code,li,dt,dd,td,th,div { font-size: 12pt; }
html { margin: 0; padding: 0; background: #eee; }
body { width: 100%; margin: 0; padding: 0; background: #eee; text-align: center; color: #111; }
article { display: inline-block; width: 100ex; overflow: hidden; text-align: left; }
a { text-decoration: none; color: #000; }
div.navi { margin: 2ex 0; padding: 0; width: 100%; text-align: right; }
div.navi a, div.navi span { display: inline-block;
  text-align: center; margin: 0 0.5ex; padding: 0; width: 10ex;
  border: 1px solid #ddd; border-radius: 0.5ex; color: #333; background: #ddd; }
div.navi a:hover { background: #def; }
div.navi span { color: #999; background: #eee; }
h1 { font-size: 135%; margin: 1ex; padding: 0; }
section { position: relative; border: solid 1px #ddd; border-radius: 0.5ex;
   background: #fff; margin: 3ex 0; padding: 1.5ex 2ex; }
div.num { position: absolute; top: 2ex; right: 2ex; font-size: 80%; color: #888; }
div.head { margin-bottom: 0.5ex; }
span.pron { display: inline-block; margin-left: 2ex; font-size: 90%; color: #333; }
span.pron:before, span.pron:after { content: "/"; font-size: 90%; color: #888; }
a.word { font-size: 120%; font-weight: bold; text-decoration: none; color: #000; }
div.trans { margin-bottom: 0.5ex; }
span.attr { display: inline-block; margin: 0.5ex 0; padding: 0;
   min-width: 3.5ex; text-align: center;
   border: 1px solid #ddd; border-radius: 0.5ex; background: #eee;
   font-size: 60%; color: #555; text-decoration: none; }
span.subattr { margin-left: -0.5ex; }
div.aux { margin-left: 3.5ex; margin-top: -0.2ex; font-size: 90%; color: #333; }
span.auxattr { color: #999; }
span.metavalue { display: inline-block; margin-right: 0.5ex }
span.childtrans { font-size: 90%; color: #333; }
span.subword { font-weight: bold; color: #000; }
div.control { text-align: right; mergin}
div.control span { display: inline-block;
  text-align: center; margin: 0 0.5ex; padding: 0; width: 20ex;
  border: 1px solid #ddd; border-radius: 0.5ex; color: #333; background: #ddd; }
div.control span:hover { background: #def; }
table.check_table { border-collapse: collapse; }
table.check_table td { border: solid 1px #ddd; padding: 0.5ex 1ex; }
tr.check_line_even { background: #f8f8f8; }
tr.check_line:hover { background: #eef8ff; }
tr.check_line_active, tr.check_line_active:hover { background: #ffffee; }
td.check_num { width: 2ex; text-align: right; font-size: 80%; color: #555; }
td.check_title { width: 20ex; overflow: hidden; white-space: nowrap; font-weight: bold; }
td.check_text { overflow: hidden; white-space: nowrap; font-size: 90%; }
a.check_word { display: inline-block; font-size: 105%; color: #000; }
span.check_trans { display: inline-block; width: 82ex; }
table.check_mode_0 span.check_trans { opacity: 0; }
table.check_mode_0 tr.check_line_active span.check_trans { opacity: 1; }
table.check_mode_1 a.check_word { opacity: 0; }
table.check_mode_1 tr.check_line_active a.check_word { opacity: 1; }
table.check_mode_1 span.kk { opacity: 0; }
table.check_mode_1 tr.check_line_active span.kk { opacity: 1; }
tr.check_line:hover a.check_word { opacity: 1; }
tr.check_line:hover span.check_trans { opacity: 1; }
tr.check_line:hover span.kk { opacity: 1; }
div.toc_line { margin: 1ex 0; }
span.toc_label { display: inline-block; width: 14ex; text-align: center; }
a.toc_link { display: inline-block; width: 14ex; text-align: center;
  border: 1px solid #ddd; border-radius: 0.5ex; color: #111; background: #eee; }
a.toc_link:hover { background: #def; }
span.toc_text { padding-left: 1ex; }
div.index_head { font-size: 110%; font-weight: bold; margin: 1.5ex 0 0.6ex 0; }
div.intro_head { font-size: 110%; font-weight: bold; margin: 1.5ex 0 0.6ex 0; }
div.intro_quote { font-size: 90%; line-height: 110%; margin-left: 1ex; }
div.intro_text { text-indent: 2ex; margin: 0.5ex 0; }
@media only screen and (max-width: 600px) {
html,body,article,p,pre,code,li,dt,dd,td,th,div { font-size: 10pt; }
article { width: 49ex; }
section { margin: 2ex 0; padding: 1ex 1ex; }
div.num { top: 1ex; right: 1ex; }
div.aux { margin-left: 1ex; }
}
"""
CHECKSCRIPT_TEXT = """'use strict';
let check_mode = 0;
function change_view() {
  check_mode = (check_mode + 1) % 3;
  for (let table of document.getElementsByClassName("check_table")) {
    table.classList.remove("check_mode_0");
    table.classList.remove("check_mode_1");
    table.classList.remove("check_mode_2");
    table.classList.add("check_mode_" + check_mode)
  }
}
function change_item(item) {
  if (item.active) {
    item.classList.remove("check_line_active");
    item.active = 0;
  } else {
    item.classList.add("check_line_active");
    item.active = 1;
  }
}
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


def EscapeTranslations(values):
  fields = []
  for value in values:
    mod_value = regex.sub(
      r"([\p{Katakana}][\p{Katakana}ー]*)", r'<span class="kk">\1</span>', esc(value))
    fields.append(mod_value)
  return ", ".join(fields)


class GenerateUnionVocabBatch:
  def __init__(self, vocab_path, body_path, phrase_path, output_path,
               num_extra_items, num_section_clusters, child_min_prob, title):
    self.vocab_path = vocab_path
    self.body_path = body_path
    self.phrase_path = phrase_path
    self.output_path = output_path
    self.num_extra_items = num_extra_items
    self.num_section_clusters = num_section_clusters
    self.child_min_prob = child_min_prob
    self.title = title

  def Run(self):
    clusters = self.ReadClusters()
    body_dbm = tkrzw.DBM()
    body_dbm.Open(self.body_path, False, dbm="HashDBM").OrDie()
    phrase_dbm = tkrzw.DBM()
    phrase_dbm.Open(self.phrase_path, False, dbm="HashDBM").OrDie()
    os.makedirs(self.output_path, exist_ok=True)
    vetted_words = self.VetWords(clusters, body_dbm)
    num_sections = 0
    uniq_words = set()
    index_items = []
    num_main_words = 0
    while clusters:
      num_sections += 1
      section_clusters = clusters[:self.num_section_clusters]
      clusters = clusters[self.num_section_clusters:]
      has_next = bool(clusters)
      out_words = []
      self.PrepareSection(section_clusters, num_sections, has_next, uniq_words,
                          body_dbm, phrase_dbm, vetted_words, out_words)
      index_items.append(out_words)
      num_main_words += len(out_words)

      # hoge
      # if num_sections >= 5: break
    
    self.OutputTOC(index_items)
    self.OutputIndex(index_items)
    self.OutputIntro(num_sections, num_main_words, len(uniq_words))
    self.OutputMiscFiles()
    phrase_dbm.Close().OrDie()
    body_dbm.Close().OrDie()
    
  def ReadClusters(self):
    clusters = []
    with open(self.vocab_path) as input_file:
      for line in input_file:
        fields = line.strip().split("\t")
        if not fields: continue
        main_words = []
        extra_words = []
        is_extra = False
        for field in fields:
          if field == "|":
            is_extra = True
            continue
          if is_extra:
            extra_words.append(field)
          else:
            main_words.append(field)
        clusters.append((main_words, extra_words))
    return clusters

  def VetWords(self, clusters, body_dbm):
    vetted_words = set()
    for cluster in clusters:
      for word in cluster[0]:
        vetted_words.add(word)
        data = body_dbm.GetStr(word)
        if not data: continue
        entries = json.loads(data)
        for entry in entries:
          if entry["word"] != word: continue
          for label in ("parent", "child"):
            rel_words = entry.get(label)
            if rel_words:
              for rel_word in rel_words:
                vetted_words.add(rel_word)
          phrases = entry.get("phrase")
          if phrases:
            for phrase in phrases:
              vetted_words.add(phrase["w"])
    return vetted_words

  def PrepareSection(self, clusters, num_sections, has_next,
                     uniq_words, body_dbm, phrase_dbm, vetted_words, out_words):
    section_main_words = []
    section_extra_word_lists = []
    for cluster in clusters:
      main_words = []
      dedup_words = []
      aliases = collections.defaultdict(set)
      num_words = len(cluster[0])
      for word in cluster[0]:
        if word in uniq_words: continue
        data = body_dbm.GetStr(word)
        if not data: continue
        entries = json.loads(data)
        for entry in entries:
          if entry["word"] != word: continue
          count_synonyms = collections.defaultdict(int)
          num_items = 0
          for item in entry["item"]:
            if item["label"] == "wn":
              num_items += 1
            for part in item["text"].split("[-]"):
              part = part.strip()
              match = regex.search(r"\[synonym\]: (.*)", part)
              if match:
                for synonym in match.group(1).split(","):
                  synonym = synonym.strip()
                  if synonym:
                    count_synonyms[synonym] += 1
          synonyms = set()
          for synonym, count in count_synonyms.items():
            if count >= num_items:
              synonyms.add(synonym)
          if synonyms:
            dedup_words.append((word, synonyms))
          duplicated = False
          for dedup_word, dedup_synonyms in dedup_words:
            if word[0] == dedup_word[0]:
              dist = tkrzw.Utility.EditDistanceLev(word, dedup_word)
              if dist <= 1 and (word in dedup_synonyms or dedup_word in synonyms):
                aliases[dedup_word].add(word)
                duplicated = True
          if duplicated:
            continue
          main_words.append(word)
          break
      extra_words = []
      for extra_word in cluster[1]:
        if extra_word not in vetted_words:
          extra_words.append(extra_word)
      while len(main_words) < num_words and extra_words:
        main_words.append(extra_words[0])
        extra_words = extra_words[1:]
      for word in main_words:
        surfaces = [word]
        surfaces.extend(aliases.get(word) or [])
        main_surface = ""
        other_surfaces = []
        if len(surfaces) == 1:
          main_surface = surfaces[0]
        else:
          prob_surfaces = []
          for surface in surfaces:
            prob = float(phrase_dbm.GetStr(surface) or "0")
            prob_surfaces.append((surface, prob))
          prob_surfaces = sorted(prob_surfaces, key=lambda x: x[1], reverse=True)
          main_surface = prob_surfaces[0][0]
          other_surfaces = [x[0] for x in prob_surfaces[1:]]
        section_main_words.append((main_surface, other_surfaces))
      section_extra_word_lists.append(extra_words)
    for main_word in section_main_words:
      out_words.append(main_word[0])
    out_path = os.path.join(self.output_path, "study-{:03d}.xhtml".format(num_sections))
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      self.OutputStudy(out_file, num_sections, has_next,
                       section_main_words, section_extra_word_lists,
                       body_dbm, uniq_words)
    out_path = os.path.join(self.output_path, "check-{:03d}.xhtml".format(num_sections))
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      self.OutputCheck(out_file, num_sections, has_next, out_words, body_dbm)

  def OutputStudy(self, out_file, num_sections, has_next, main_words, extra_word_lists,
                  body_dbm, uniq_words):
    def P(*args, end="\n"):
      esc_args = []
      for arg in args[1:]:
        if isinstance(arg, str):
          arg = esc(arg)
        esc_args.append(arg)
      print(args[0].format(*esc_args), end=end, file=out_file)
    def PrintNavi():
      P('<div class="navi">')
      P('<a href="index.xhtml">TOP</a>')
      check_url = "check-{:03d}.xhtml".format(num_sections)
      P('<a href="{}">CHECK</a>', check_url)
      if num_sections == 1:
        P('<span class="void">PREV</span>')
      else:
        prev_url = "study-{:03d}.xhtml".format(num_sections - 1)
        P('<a href="{}">PREV</a>', prev_url)
      if has_next:
        next_url = "study-{:03d}.xhtml".format(num_sections + 1)
        P('<a href="{}">NEXT</a>', next_url)
      else:
        P('<span class="void">NEXT</span>')
      P('</div>')
    P('<?xml version="1.0" encoding="UTF-8"?>')
    P('<!DOCTYPE html>')
    P('<html xmlns="http://www.w3.org/1999/xhtml">')
    P('<head>')
    P('<meta charset="UTF-8"/>')
    P('<meta name="viewport" content="width=device-width"/>')
    P('<title>{}: Chapter {} Study</title>', self.title, num_sections)
    P('<link rel="stylesheet" href="style.css"/>')
    P('</head>')
    P('<body>')
    P('<article>')
    PrintNavi()
    P('<h1><a href="">Chapter {} Study</a></h1>', num_sections)
    num_words = 0
    for surface, aliases in main_words:
      data = body_dbm.GetStr(surface) or ""
      entries = json.loads(data)
      entry = None
      for word_entry in entries:
        if word_entry["word"] == surface:
          entry = word_entry
          break
      if not entry:
        P('<p>Warning: no data for {}</p>', surface)
        continue
      uniq_words.add(surface)
      num_words += 1
      word_id = ConvertWordToID(surface)
      P('<section id="{}" class="entry">', word_id)
      P('<div class="num">{:02d}</div>', num_words)
      P('<div class="head">')
      P('<a href="#{}" class="word">{}</a>', word_id, surface)
      pron = entry.get("pronunciation")
      if pron:
        P('<span class="pron">{}</span>', pron)
      P('</div>', surface)
      trans = entry.get("translation")
      if trans:
        P('<div class="trans">{}</div>', ", ".join(trans[:8]))
      first_label = None
      num_items = 0
      poses = set()
      for item in entry["item"]:
        label = item["label"]
        pos = item["pos"]
        text = item["text"]
        if regex.search(r"^\[translation\]", text): continue
        if num_items >= 10: break
        if first_label and label != first_label:
          break
        first_label = label
        parts = []
        for part in text.split("[-]"):
          part = part.strip()
          parts.append(part)
        if not parts: continue
        num_items += 1
        main_text = CutTextByWidth(parts[0], 128)
        poses.add(pos)
        P('<div class="text">')
        P('<span class="attr">{}</span>', POSES.get(pos) or pos)
        P('<span>{}</span>', main_text)
        P('</div>')
        synonyms = []
        examples = []
        for part in parts[1:]:
          match = regex.search(r"\[synonym\]: (.*)", part)
          if match:
            synonyms.append(match.group(1).strip())
          match = regex.search(r"\e.g.: (.*)", part)
          if match:
            examples.append(match.group(1).strip())
        for text in synonyms:
          text = CutTextByWidth(text, 128)
          P('<div class="aux">')
          P('<span class="auxattr">≒</span>')
          P('<span>{}</span>', text)
          P('</div>')
        for text in examples[:1]:
          text = CutTextByWidth(text, 128)
          P('<div class="aux">')
          P('<span class="auxattr">・</span>')
          P('<span>{}</span>', text)
          P('</div>')
      parents = entry.get("parent")
      children = entry.get("child")
      sibling_alts = set((parents or []) + (children or []))
      phrases = entry.get("phrase") or []
      for label, delivatives in (("語幹", parents), ("派生", children)):
        if not delivatives: continue
        for child in delivatives:
          if child in uniq_words: continue
          uniq_words.add(child)
          child_trans = None
          child_poses = None
          child_data = body_dbm.GetStr(child)
          if child_data:
            child_entries = json.loads(child_data)
            child_prob = 0
            for child_entry in child_entries:
              if child_entry["word"] != child: continue
              child_prob = float(child_entry.get("probability") or 0.0)
              us_hit = False
              child_alts = child_entry.get("alternative") or []
              suffix_pairs = [("se", "ze"), ("sing", "zing"), ("sed", "zed"),
                              ("ser", "zer"), ("sation", "zation"), ("ence", "ense"),
                              ("our", "or"), ("og", "ogue"), ("re", "er"), ("l", "ll")]
              for gb_suffix, us_suffix in suffix_pairs:
                if child.endswith(gb_suffix):
                  us_word = child[:-len(gb_suffix)] + us_suffix
                  if ((us_word in child_alts or us_word in sibling_alts)
                      and body_dbm.GetStr(us_word)):
                    us_hit = True
                    break
              if us_hit:
                continue
              child_trans = child_entry.get("translation")
              child_poses = self.GetEntryPOSList(child_entry)
              child_phrases = child_entry.get("phrase") or []
              if child_phrases:
                phrases.extend(child_phrases)
              break
          if not child_trans: continue
          if self.child_min_prob > 0 and child_prob < self.child_min_prob:
            continue
          P('<div class="child">')
          P('<span class="attr">{}</span>', label)
          for child_pos in child_poses[:2]:
            P('<span class="attr subattr">{}</span>', POSES.get(child_pos) or child_pos)
          child_id = ConvertWordToID(child)
          P('<span id="{}" class="subword">{}</span>', child_id, child_id, child)
          P('<span class="childtrans">: {}</span>', ", ".join(child_trans[:4]))
          P('</div>')
      if phrases:
        for phrase in phrases:
          if not phrase.get("i"): continue
          phrase_word = phrase.get("w")
          if not phrase: continue
          if phrase_word in uniq_words: continue
          uniq_words.add(phrase_word)
          phrase_data = body_dbm.GetStr(phrase_word)
          if not phrase_data: continue
          phrase_entries = json.loads(phrase_data)
          phrase_trans = None
          phrase_poses = None
          phrase_prob = 0
          for phrase_entry in phrase_entries:
            if phrase_entry["word"] != phrase_word: continue
            phrase_prob = float(phrase_entry.get("probability") or 0.0)
            phrase_trans = phrase_entry.get("translation")
            phrase_poses = self.GetEntryPOSList(phrase_entry)
            break
          if not phrase_trans: continue
          if self.child_min_prob > 0 and phrase_prob < self.child_min_prob: continue
          P('<div class="child">')
          P('<span class="attr">句</span>')
          for phrase_pos in phrase_poses[:2]:
            P('<span class="attr subattr">{}</span>', POSES.get(phrase_pos) or phrase_pos)
          phrase_id = ConvertWordToID(phrase_word)
          P('<span href="#{}" id="{}" class="subword">{}</span>', phrase_id, phrase_id, phrase_word)
          P('<span class="childtrans">: {}</span>', ", ".join(phrase_trans[:4]))
          P('</div>')
      infls = []
      for name, label in INFLECTIONS:
        prefix = name[:name.find("_")]
        if prefix not in poses: continue
        value = entry.get(name)
        if not value: continue
        infls.append((label, value))
      uniq_infls = set()
      for alias in aliases:
        if alias in uniq_infls: continue
        uniq_infls.add(alias)
        infls.append(("代替", alias))
      alternatives = entry.get("alternative")
      if alternatives:
        for alt in alternatives:
          if alt in uniq_infls: continue
          uniq_infls.add(alt)
          infls.append(("代替", alt))
      if infls:
        P('<div class="meta">')
        for label, value in infls:
          P('<span class="attr">{}</span>', label)
          P('<span class="metavalue">{}</span>', value)
        P('</div>')
      P('</section>')
    extra_words = []
    for extra_word_list in extra_word_lists:
      num_extra_words = 0
      for extra_word in extra_word_list:
        if num_extra_words >= self.num_extra_items: break
        if extra_word in uniq_words: continue
        extra_trans = []
        extra_poses = []
        extra_data = body_dbm.GetStr(extra_word)
        if extra_data:
          extra_entries = json.loads(extra_data)
          for extra_entry in extra_entries:
            if extra_entry["word"] != extra_word: continue
            extra_trans.extend(extra_entry.get("translation") or [])
            extra_poses.extend(self.GetEntryPOSList(extra_entry))
        if not extra_trans: continue
        extra_trans = extra_trans[:5]
        has_kanji = False
        for extra_tran in extra_trans:
          if regex.search(r"\p{Han}", extra_tran):
            has_kanji = True
        if not has_kanji: continue
        extra_words.append((extra_word, extra_trans, extra_poses))
        uniq_words.add(extra_word)
        num_extra_words += 1
    if extra_words:
      P('<section class="entry">')
      P('<div class="num">Bonus Words</div>')
      for extra_word, extra_trans, extra_poses in extra_words:
        P('<div class="extra">')
        word_id = ConvertWordToID(extra_word)
        P('<span id="{}" class="subword">{}</span> :', word_id, word_id, extra_word)
        for extra_pos in extra_poses[:2]:
          P('<span class="attr subattr">{}</span>', POSES.get(extra_pos) or extra_pos)
        P('<span class="childtrans">{}</span>', ", ".join(extra_trans))
        P('</div>')
      P('</section>')
    PrintNavi()
    P('</article>')
    P('</body>')
    P('</html>')
          
  def OutputCheck(self, out_file, num_sections, has_next, out_words, body_dbm):
    def P(*args, end="\n"):
      esc_args = []
      for arg in args[1:]:
        if isinstance(arg, str):
          arg = esc(arg)
        esc_args.append(arg)
      print(args[0].format(*esc_args), end=end, file=out_file)
    def PrintNavi():
      P('<div class="navi">')
      P('<a href="index.xhtml">TOP</a>')
      study_url = "study-{:03d}.xhtml".format(num_sections)
      P('<a href="{}">STUDY</a>', study_url)
      if num_sections == 1:
        P('<span class="void">PREV</span>')
      else:
        prev_url = "check-{:03d}.xhtml".format(num_sections - 1)
        P('<a href="{}">PREV</a>', prev_url)
      if has_next:
        next_url = "check-{:03d}.xhtml".format(num_sections + 1)
        P('<a href="{}">NEXT</a>', next_url)
      else:
        P('<span class="void">NEXT</span>')
      P('</div>')
    def PrintControl():
      P('<div class="control">')
      P('<span class="button" onclick="change_view();">CHANGE VIEW</span>')
      P('</div>')
    P('<?xml version="1.0" encoding="UTF-8"?>')
    P('<!DOCTYPE html>')
    P('<html xmlns="http://www.w3.org/1999/xhtml">')
    P('<head>')
    P('<meta charset="UTF-8"/>')
    P('<meta name="viewport" content="width=device-width"/>')
    P('<title>{}: Chapter {} Check</title>', self.title, num_sections)
    P('<link rel="stylesheet" href="style.css"/>')
    P('<script src="checkscript.js"></script>')
    P('</head>')
    P('<body>')
    P('<article>')
    PrintNavi()
    P('<h1><a href="">Chapter {} Check</a></h1>', num_sections)
    PrintControl()
    P('<section>')
    P('<table class="check_table check_mode_0">')
    num_line = 0
    for word in out_words:
      data = body_dbm.GetStr(word)
      if not data: continue
      poses = []
      tran_html = None
      entries = json.loads(data)
      for entry in entries:
        if entry["word"] != word: continue
        poses = self.GetEntryPOSList(entry)
        translations = entry.get("translation")
        if translations:
          tran_html = EscapeTranslations(translations[:6])
        else:
          for item in entry["item"]:
            if not tran_html:
              for part in item["text"].split("[-]"):
                part = part.strip()
                if part:
                  tran_html = esc(part)
                  break
      if not tran_html: continue
      word_url = "study-{:03d}.xhtml#{}".format(num_sections, ConvertWordToID(word))
      line_class_suffix = "even" if num_line % 2 == 0 else "odd"
      P('<tr class="check_line check_line_{}" onclick="change_item(this);">', line_class_suffix)
      P('<td class="check_num">{:02d}</td>', num_line + 1)
      P('<td class="check_title">')
      P('<a href="{}" class="check_word">{}</a>', word_url, word)
      P('</td>')
      P('<td class="check_text">')
      P('<span class="check_trans">')
      print(tran_html, file=out_file)
      P('</span>')
      P('</td>')
      P('</tr>')
      num_line += 1
    P('</table>')
    P('</section>')
    PrintControl()
    PrintNavi()
    P('</article>')
    P('</body>')
    P('</html>')
    
  def GetEntryPOSList(self, entry):
    poses = []
    first_label = None
    for item in entry["item"]:
      label = item["label"]
      pos = item["pos"]
      if first_label and label != first_label: break
      first_label = label
      if pos not in poses:
        poses.append(pos)
    return poses

  def OutputTOC(self, index_items):
    out_path = os.path.join(self.output_path, "index.xhtml")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      def P(*args, end="\n"):
        esc_args = []
        for arg in args[1:]:
          if isinstance(arg, str):
            arg = esc(arg)
          esc_args.append(arg)
        print(args[0].format(*esc_args), end=end, file=out_file)
      P('<?xml version="1.0" encoding="UTF-8"?>')
      P('<!DOCTYPE html>')
      P('<html xmlns="http://www.w3.org/1999/xhtml">')
      P('<head>')
      P('<meta charset="UTF-8"/>')
      P('<meta name="viewport" content="width=device-width"/>')
      P('<title>{}: TOC</title>', self.title)
      P('<link rel="stylesheet" href="style.css"/>')
      P('</head>')
      P('<body>')
      P('<article>')
      P('<h1><a href="">{}</a></h1>', self.title)
      P('<section id="toc">')
      P('<div class="toc_line">')
      intro_url = "intro.xhtml"
      P('<span class="toc_label">Chapter 000 :</span>')
      P('<a href="{}" class="toc_link">INTRO</a>', intro_url)
      index_url = "list.xhtml"
      P('<a href="{}" class="toc_link">INDEX</a>', index_url)
      P('</div>')
      for i, out_words in enumerate(index_items):
        num_sections = i + 1
        P('<div class="toc_line">')
        P('<span class="toc_label">Chapter {:03} :</span>', num_sections)
        study_url = "study-{:03}.xhtml".format(num_sections)
        P('<a href="{}" class="toc_link">STUDY</a>', study_url)
        check_url = "check-{:03}.xhtml".format(num_sections)
        P('<a href="{}" class="toc_link">CHECK</a>', check_url)
        P('<span class="toc_text">{} ...</span>', ", ".join(out_words[:4]))
        P('</div>')
      P('</section>')
      P('</article>')
      P('</body>')
      P('</html>')

  def OutputIndex(self, index_items):
    out_path = os.path.join(self.output_path, "list.xhtml")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      def P(*args, end="\n"):
        esc_args = []
        for arg in args[1:]:
          if isinstance(arg, str):
            arg = esc(arg)
          esc_args.append(arg)
        print(args[0].format(*esc_args), end=end, file=out_file)
      def PrintNavi():
        P('<div class="navi">')
        P('<a href="index.xhtml">TOP</a>')
        P('</div>')
      P('<?xml version="1.0" encoding="UTF-8"?>')
      P('<!DOCTYPE html>')
      P('<html xmlns="http://www.w3.org/1999/xhtml">')
      P('<head>')
      P('<meta charset="UTF-8"/>')
      P('<meta name="viewport" content="width=device-width"/>')
      P('<title>{}: Intro</title>', self.title)
      P('<link rel="stylesheet" href="style.css"/>')
      P('</head>')
      P('<body>')
      P('<article>')
      PrintNavi()
      P('<h1><a href="">{}の索引</a></h1>', self.title)
      P('<section id="index">')
      section_words = []
      for i, out_words in enumerate(index_items):
        for word in out_words:
          section_words.append((i, word))
      section_words = sorted(section_words, key=lambda x: x[1])
      first_letter = ""
      first_word = False
      for i, word in section_words:
        if first_letter != word[0]:
          if first_letter:
            P('</div>')
          first_letter = word[0]
          P('<div class="index_head">{}</div>', first_letter.upper())
          P('<div class="index_list">', end="")
          first_word = True
        if not first_word:
          P(', ')
        word_url = "study-{:03d}.xhtml#{}".format(i + 1, ConvertWordToID(word))
        P('<a href="{}">{}</a>', word_url, word, end="")
        first_word = False
      if first_letter:
        P('</div>')
      P('</section>')
      PrintNavi()
      P('</article>')
      P('</body>')
      P('</html>')
    
  def OutputIntro(self, num_sections, num_main_words, num_uniq_words):
    out_path = os.path.join(self.output_path, "intro.xhtml")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      def P(*args, end="\n"):
        esc_args = []
        for arg in args[1:]:
          if isinstance(arg, str):
            arg = esc(arg)
          esc_args.append(arg)
        print(args[0].format(*esc_args), end=end, file=out_file)
      def PrintNavi():
        P('<div class="navi">')
        P('<a href="index.xhtml">TOP</a>')
        P('</div>')
      P('<?xml version="1.0" encoding="UTF-8"?>')
      P('<!DOCTYPE html>')
      P('<html xmlns="http://www.w3.org/1999/xhtml">')
      P('<head>')
      P('<meta charset="UTF-8"/>')
      P('<meta name="viewport" content="width=device-width"/>')
      P('<title>{}: Intro</title>', self.title)
      P('<link rel="stylesheet" href="style.css"/>')
      P('</head>')
      P('<body>')
      P('<article>')
      PrintNavi()
      P('<h1><a href="">{}の手引き</a></h1>', self.title)
      P('<section id="intro">')
      for line in INTRO_TEXT.split("\n"):
        line = line.strip()
        if not line: continue
        mode = "intro_text"
        if line.startswith("*"):
          mode = "intro_head"
          line = line[1:].strip()
        elif line.startswith(">"):
          mode = "intro_quote"
          line = line[1:].strip()
        line = line.replace("{TITLE}", self.title)
        line = line.replace("{NUM_SECTIONS}", str(num_sections))
        line = line.replace("{NUM_MAIN_WORDS}", str(num_main_words))
        line = line.replace("{NUM_UNIQ_WORDS}", str(num_uniq_words))
        if not line: continue
        P('<div class="{}">{}</div>', mode, line)
      P('</section>')
      PrintNavi()
      P('</article>')
      P('</body>')
      P('</html>')

  def OutputMiscFiles(self):
    out_path = os.path.join(self.output_path, "style.css")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(STYLE_TEXT, file=out_file, end="")
    out_path = os.path.join(self.output_path, "checkscript.js")
    logger.info("Creating: {}".format(out_path))
    with open(out_path, "w") as out_file:
      print(CHECKSCRIPT_TEXT, file=out_file, end="")


def main():
  args = sys.argv[1:]
  vocab_path = tkrzw_dict.GetCommandFlag(args, "--vocab", 1) or ""
  body_path = tkrzw_dict.GetCommandFlag(args, "--body", 1) or ""
  phrase_path = tkrzw_dict.GetCommandFlag(args, "--phrase", 1) or ""
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or ""
  num_extra_items = int(tkrzw_dict.GetCommandFlag(args, "--extra_items", 1) or 0)
  num_section_clusters = int(tkrzw_dict.GetCommandFlag(args, "--section_clusters", 1) or 1)
  child_min_prob = float(tkrzw_dict.GetCommandFlag(args, "--child_min_prob", 1) or 0)
  title = tkrzw_dict.GetCommandFlag(args, "--title", 1) or "連想英単語帳"
  if not vocab_path:
    raise RuntimeError("the vocab path is required")
  if not body_path:
    raise RuntimeError("the body path is required")
  if not phrase_path:
    raise RuntimeError("the phrase path is required")
  if not output_path:
    raise RuntimeError("the output path is required")
  GenerateUnionVocabBatch(vocab_path, body_path, phrase_path, output_path,
                          num_extra_items, num_section_clusters, child_min_prob, title).Run()


if __name__=="__main__":
  main()
