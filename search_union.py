#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to search a union dictionary
#
# Usage:
#   search_union.py [--data_prefix str] [--index str] [--search str] [--view str]
#     [--capacity] [--query_file str] [--output_prefix str] [words...]
#
#   Index modes: auto (default), normal, reverse, inflection, grade, annot
#   Search modes: auto (default), expact, prefix, suffix, contain, word, edit, related
#   View mode: auto (default), full, simple, list, annot
#
# Example:
#   ./search_union.py --data_prefix union --search full --view full  united states
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


import cgi
import html
import os
import math
import regex
import sys
import tkrzw_dict
import tkrzw_union_searcher
import urllib
import urllib.request


PAGE_WIDTH = 100
CGI_DATA_PREFIX = "union"
CGI_CAPACITY = 100
CGI_MAX_HTTP_CONTENT_LENGTH = 512 * 1024
CGI_MAX_QUERY_LENGTH = 256 * 1024
CGI_MAX_QUERY_LINE_LENGTH = 16 * 1024
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
  "translation": "訳語",
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


def Atoi(text):
  try:
    return int(text)
  except ValueError:
    return 0

  
def CutTextByWidth(text, width):
  result = ""
  for c in text:
    if width < 0:
      result += "..."
      break
    result += c
    width -= 2 if ord(c) > 256 else 1
  return result


def GetEntryPoses(entry):
  poses = []
  uniq_poses = set()
  first_label = None
  for item in entry["item"]:
    label = item["label"]
    if first_label and label != first_label: break
    first_label = label
    text = item["text"]
    if regex.search(
        r"の(直接法|直説法|仮定法)?(現在|過去)?(第?[一二三]人称)?[ ・、]?" +
        r"(単数|複数|現在|過去|比較|最上|進行|完了|動名詞|単純)+[ ・、]?" +
        r"(形|型|分詞|級|動名詞|名詞|動詞|形容詞|副詞)+", text):
      continue
    if regex.search(r"の(直接法|直説法|仮定法)(現在|過去)", text):
      continue
    if regex.search(r"の(動名詞|異綴|異体|古語|略|省略|短縮|頭字語)", text):
      continue
    pos = item["pos"]
    if pos in uniq_poses: continue
    uniq_poses.add(pos)
    poses.append(pos)
  return poses


def PrintWrappedText(text, indent):
  sys.stdout.write(" " * indent)
  width = indent
  foldable = True
  for c in text:
    if (foldable and width >= PAGE_WIDTH - 1) or width >= PAGE_WIDTH + 20:
      sys.stdout.write("\n")
      sys.stdout.write(" " * indent)
      width = indent
    sys.stdout.write(c)
    width += 2 if ord(c) > 256 else 1
    foldable = c == " "
  print("")


def PrintResult(entries, mode, query):
  for entry in entries:
    if mode != "list":
      print()
    title = entry.get("word")
    if mode == "list":
      poses = []
      for pos in GetEntryPoses(entry):
        pos = POSES.get(pos) or pos[:1]
        poses.append(pos)
      if poses:
        title += "  [{}]".format(",".join(poses))
    translations = entry.get("translation")
    if translations:
      if tkrzw_dict.PredictLanguage(query) != "en":
        translations = tkrzw_dict.TwiddleWords(translations, query)
      title += "  \"{}\"".format(", ".join(translations[:8]))
    elif mode == "list":
      for item in entry["item"]:
        text = item["text"].split(" [-] ")[0]
        text = CutTextByWidth(text, 70)
        title +=  "  \"{}\"".format(text)
        break
    if mode != "list":
      pron = entry.get("pronunciation")
      if pron:
        title += "  /{}/".format(pron)
    PrintWrappedText(title, 2)
    if mode == "full":
      for attr_list in INFLECTIONS:
        fields = []
        for name, label in attr_list:
          value = entry.get(name)
          if value:
            fields.append("{}: {}".format(label, value))
        if fields:
          PrintWrappedText("  ".join(fields), 4)
    if mode != "list":
      print()
    if mode in ("simple", "full"):
      num_items = 0
      for item in entry["item"]:
        if mode == "simple" and num_items >= 8:
          break
        label = item.get("label")
        pos = item.get("pos")
        sections = item["text"].split(" [-] ")
        text = ""
        if label:
          text += "({}) ".format(label)
        if pos:
          pos = POSES.get(pos) or pos
          text += "[{}] ".format(pos)
        text += sections[0]
        PrintWrappedText(text, 4)
        if mode == "full":
          for section in sections[1:]:
            attr_match = regex.search(r"^\[([a-z]+)\]: ", section)
            eg_match = regex.search(r"^e\.g\.: ", section)
            if attr_match:
              if attr_match.group(1) == "synset": continue
              attr_label = WORDNET_ATTRS.get(attr_match.group(1))
              if attr_label:
                section = "{}: {}".format(attr_label, section[len(attr_match.group(0)):].strip())
            elif eg_match:
              section = "例: {}".format(section[len(eg_match.group(0)):].strip())
            subsections = section.split(" [--] ")
            PrintWrappedText(subsections[0], 6)
            for subsection in subsections[1:]:
              subsubsections = subsection.split(" [---] ")
              PrintWrappedText(subsubsections[0], 8)
              for subsubsubsection in subsubsections[1:]:
                PrintWrappedText(subsubsubsection, 10)
        num_items += 1
      if mode == "full":
        parents = entry.get("parent")
        if parents:
          text = "[語幹] {}".format(", ".join(parents[:8]))
          PrintWrappedText(text, 4)
        children = entry.get("child")
        if children:
          text = "[派生] {}".format(", ".join(children[:8]))
          PrintWrappedText(text, 4)
        idioms = entry.get("idiom")
        if idioms:
          text = "[熟語] {}".format(", ".join(idioms[:8]))
          PrintWrappedText(text, 4)
        related = entry.get("related")
        if related:
          text = "[関連] {}".format(", ".join(related[:8]))
          PrintWrappedText(text, 4)
        coocs = entry.get("cooccurrence")
        if coocs:
          text = "[共起] {}".format(", ".join(coocs[:8]))
          PrintWrappedText(text, 4)          
        etym_parts = []
        etym_prefix = entry.get("etymology_prefix")
        if etym_prefix: etym_parts.append(etym_prefix + "-")
        etym_core = entry.get("etymology_core")
        if etym_core: etym_parts.append(etym_core)
        etym_suffix = entry.get("etymology_suffix")
        if etym_suffix: etym_parts.append("-" + etym_suffix)
        if etym_parts:
          text = "[語源] {}".format(" + ".join(etym_parts))
          PrintWrappedText(text, 4)
        prob = entry.get("probability")
        if prob:
          prob = float(prob)
          if prob > 0:
            fmt = "{{:.{}f}}".format(min(max(int(math.log10(prob) * -1 + 1), 3), 6))
            prob_expr = regex.sub(r"\.(\d{3})(\d*?)0+$", r".\1\2", fmt.format(prob * 100))
            text = "[確率] {}%".format(prob_expr)
            PrintWrappedText(text, 4)
        aoa = entry.get("aoa") or entry.get("aoa_concept") or entry.get("aoa_base")
        if aoa:
          text = "[年齢] {:.2f}".format(float(aoa))
          PrintWrappedText(text, 4)
  if mode != "list":
    print()


def PrintResultAnnot(spans, head_level):
  ruby_trans = None
  ruby_word = None
  ruby_annots = None
  ruby_aoa = 0
  ruby_life = 0
  def StartRuby():
    print("[", end="")
  def EndRuby(ruby_trans):
    word_width = 0
    for c in ruby_word:
      word_width += 2 if ord(c) > 256 else 1
    tran_width = max(word_width * 2, 20)
    ruby_text = ""
    for tran in ruby_trans:
      if tran_width < 6: break
      if ruby_text:
        ruby_text += ","
        tran_width -= 1
      for c in tran:
        tran_width -= 2 if ord(c) > 256 else 1
        if tran_width <= 0:
          ruby_text += ".."
          break
        ruby_text += c
    print("] (" + ruby_text + ")", end="")
    ruby_trans.clear()
  if head_level:
    print("< ", end="")
  for i in range(0, len(spans)):
    text, is_word, annots = spans[i]
    if regex.search(r"[^\s]", text) or ruby_life == 1:
      ruby_life -= 1
    if ruby_trans and ruby_life == 0:
      EndRuby(ruby_trans)
    if annots:
      if ruby_life <= 0:
        for entry in annots:
          word = entry["word"]
          trans = entry.get("translation")
          if not trans: continue
          ruby_trans = trans
          ruby_word = word
          ruby_life = word.count(" ") + 1
          break
        if ruby_trans:
          ruby_annots = annots
          StartRuby()
      else:
        ruby_annots.extend(annots)
    print(text, end="")
  if ruby_trans:
    EndRuby(ruby_trans)
  if head_level:
    print(" >")
  print()


def main():
  args = sys.argv[1:]
  data_prefix = tkrzw_dict.GetCommandFlag(args, "--data_prefix", 1) or "union"
  index_mode = tkrzw_dict.GetCommandFlag(args, "--index", 1) or "auto"
  search_mode = tkrzw_dict.GetCommandFlag(args, "--search", 1) or "auto"
  view_mode = tkrzw_dict.GetCommandFlag(args, "--view", 1) or "auto"
  capacity = int(tkrzw_dict.GetCommandFlag(args, "--capacity", 1) or "100")
  query_file = tkrzw_dict.GetCommandFlag(args, "--query_file", 1) or ""
  output_prefix = tkrzw_dict.GetCommandFlag(args, "--output_prefix", 1) or ""
  if query_file:
    if query_file == "-":
      query = sys.stdin.read()
    else:
      with open(query_file) as input_file:
        query = input_file.read()
  else:
    query = " ".join(args)
  if not query:
    raise RuntimeError("words are not specified")
  query = query.strip()
  if index_mode == "auto" and len(query) > 48:
    index_mode = "annot"
  searcher = tkrzw_union_searcher.UnionSearcher(data_prefix)
  is_reverse = False
  if index_mode == "auto":
    if tkrzw_dict.PredictLanguage(query) != "en":
      is_reverse = True
  elif index_mode == "normal":
    pass
  elif index_mode == "reverse":
    is_reverse = True
  elif index_mode == "inflection":
    lemmas = searcher.SearchInflections(query)
    if lemmas:
      if search_mode in ("auto", "exact"):
        lemmas.append(query)
        query = ",".join(lemmas)
      else:
        query = lemmas[0]
  elif index_mode == "grade":
    search_mode = "grade"
  elif index_mode == "annot":
    search_mode = "annot"
  else:
    raise RuntimeError("unknown index mode: " + index_mode)
  if search_mode in ("auto", "exact"):
    if is_reverse:
      result = searcher.SearchExactReverse(query, capacity)
      if not result and search_mode == "auto":
        result = searcher.SearchPatternMatchReverse("edit", query, capacity)
    else:
      result = searcher.SearchExact(query, capacity)
      if not result and search_mode == "auto":
        result = searcher.SearchPatternMatch("edit", query, capacity)
  elif search_mode == "prefix":
    if is_reverse:
      result = searcher.SearchPatternMatchReverse("begin", query, capacity)
    else:
      result = searcher.SearchPatternMatch("begin", query, capacity)
  elif search_mode == "suffix":
    if is_reverse:
      result = searcher.SearchPatternMatchReverse("end", query, capacity)
    else:
      result = searcher.SearchPatternMatch("end", query, capacity)
  elif search_mode == "contain":
    if is_reverse:
      result = searcher.SearchPatternMatchReverse("contain", query, capacity)
    else:
      result = searcher.SearchPatternMatch("contain", query, capacity)
  elif search_mode == "word":
    pattern = r"(^| ){}( |$)".format(regex.escape(query))
    if is_reverse:
      result = searcher.SearchPatternMatchReverse("regex", pattern, capacity)
    else:
      result = searcher.SearchPatternMatch("regex", pattern, capacity)
  elif search_mode == "edit":
    if is_reverse:
      result = searcher.SearchPatternMatchReverse("edit", query, capacity)
    else:
      result = searcher.SearchPatternMatch("edit", query, capacity)
  elif search_mode == "related":
    if is_reverse:
      result = searcher.SearchRelatedReverse(query, capacity)
    else:
      result = searcher.SearchRelated(query, capacity)
  elif search_mode == "grade":
    page = max(Atoi(query), 1)
    result = searcher.SearchByGrade(capacity, page, True)
  elif search_mode == "annot":
    if view_mode in ("auto", "annot"):
      result = True
      view_mode = "annot"
    else:
      result = []
      for span, is_word, annots in searcher.AnnotateText(query):
        if annots:
          for entry in annots:
            result.append(entry)
  else:
    raise RuntimeError("unknown search mode: " + search_mode)
  if result:
    if view_mode == "annot":
      if (query.startswith("<html") or query.startswith("<?xml") or
          query.startswith("<!DOCTYPE")):
        query = tkrzw_union_searcher.ConvertHTMLToText(query)
      elif not query.startswith("====[META]====") and not query.startswith("====[PAGE]===="):
        query = tkrzw_union_searcher.CramText(query)
      meta, pages = tkrzw_union_searcher.DivideTextToPages(query)
      doc_title = ""
      meta_lines = []
      for line in meta:
        line = line.strip()
        if line.startswith("[title]:"):
          line = line[line.find(":") + 1:].strip()
          if line:
            doc_title = line
        elif line:
          meta_lines.append(line)
      if output_prefix:
        OutputAnnotHTML(searcher, output_prefix, doc_title, meta_lines, pages)
      else:
        print()
        if doc_title:
          print("<< " + doc_title + " >>")
        for line in meta_lines:
          print(" " + line)
        num_sections = 0
        num_words = 0
        num_words_with_annots = 0
        num_annots = 0
        for page in pages:
          print()
          for line in page:
            line = line.strip()
            head_level = 0
            match = regex.search(r"^\[head([1-3])\]:", line)
            if match:
              head_level = Atoi(match.group(1))
              line = line[match.end():].strip()
            if not line: continue
            result = searcher.AnnotateText(line)
            PrintResultAnnot(result, head_level)
            num_sections += 1
            for span, is_word, annots in result:
              if is_word and not regex.search(r"\d", span):
                num_words += 1
                if annots:
                  num_words_with_annots += 1
                  num_annots += len(annots)
        print()
        coverage = num_words_with_annots / num_words if num_words else 0
        P('頁数: {}  段落数: {}  単語数: {}  注釈数: {}  カバー率: {:.1f}%',
          len(pages), num_sections, num_words, num_annots, coverage * 100)
        print()
    elif view_mode == "auto":
      keys = searcher.GetResultKeys(result)
      if len(keys) < 2:
        PrintResult(result, "full", query)
      elif len(keys) < 6:
        PrintResult(result, "simple", query)
      else:
        PrintResult(result, "list", query)
    else:
      PrintResult(result, view_mode, query)
  else:
    print("No result.")


def OutputAnnotHTML(searcher, output_prefix, doc_title, meta_lines, pages):
  script_name = "https://dbmx.net/dict/search_union.cgi"
  prefix_is_dir = os.path.isdir(output_prefix)
  page_paths = []
  num_sections = 0
  num_words = 0
  num_words_with_annots = 0
  num_annots = 0
  page_titles = {}
  for page in pages:
    page_id = len(page_paths) + 1
    if prefix_is_dir:
      page_path = os.path.join(output_prefix, "{:04d}.xhtml".format(page_id))      
    else:
      page_path = "{}-{:04d}.xhtml".format(output_prefix, page_id)
    print("Outputting {}".format(page_path))
    page_paths.append(page_path)
    page_title = (doc_title or "Page") + "-" + str(page_id)
    with open(page_path, "w") as page_file:
      def P(*args, end="\n"):
        global P
        P(*args, end, file=page_file)
      PrintCGIHeader(page_title, file=page_file)
      P('<div class="message_view">')
      P('<form name="annot_navi_form">')
      P('<div id="annot_navi_line">')
      P('注釈想定年齢:')
      P('<select name="min_aoa" onchange="toggle_rubies(parseInt(this.value))">')
      for min_aoa in range(3, 21):
        P('<option value="{}"', min_aoa, end="")
        if min_aoa == 12:
          P(' selected="selected"')
        P('>{}歳</option>', min_aoa)
      P('</select>')
      P('<select name="init_only" onchange="toggle_rubies()">')
      P('<option value="1" selected="selected">初出のみ</option>')
      P('<option value="0">全て</option>')
      P('</select>')
      P('</div>')
      P('</form>')
      P('</div>')
      for line in page:
        line = line.strip()
        head_level = 0
        match = regex.search(r"^\[head([1-3])\]:", line)
        if match:
          head_level = Atoi(match.group(1))
          line = line[match.end():].strip()
        if not line: continue
        if head_level == 1 and page_id not in page_titles:
          page_titles[page_id] = line
        result = searcher.AnnotateText(line)
        PrintResultCGIAnnot(script_name, result, head_level, file=page_file)
        num_sections += 1
        for span, is_word, annots in result:
          if is_word and not regex.search(r"\d", span):
            num_words += 1
            if annots:
              num_words_with_annots += 1
              num_annots += len(annots)
      PrintCGIFooter(file=page_file)
  page_title = doc_title or "Index"
  if prefix_is_dir:
    index_path = os.path.join(output_prefix, "index.xhtml".format(page_id))      
  else:
    index_path = "{}-index.xhtml".format(output_prefix, page_id)
  print("Outputting {}".format(index_path))
  with open(index_path, "w") as index_file:
    def P(*args, end="\n"):
      global P
      P(*args, end, file=index_file)
    PrintCGIHeader(page_title, file=index_file)
    P('<div class="message_view">')
    P('<h1>{}</h1>', page_title)
    for meta_line in meta_lines:
      P('<p>{}</p>', meta_line)
    P('<ul>')
    for i, page_path in enumerate(page_paths):
      page_id = i + 1
      page_name = os.path.basename(page_path)
      page_title = page_titles.get(page_id) or "ページ{}".format(page_id)
      P('<li><a href="{}">{}</a></li>', urllib.parse.quote(page_name), page_title)
    P('</ul>')
    coverage = num_words_with_annots / num_words if num_words else 0
    P('<p>頁数: {} 。段落数: {} 。単語数: {} 。注釈数: {} 。カバー率: {:.1f}% 。</p>',
      len(pages), num_sections, num_words, num_annots, coverage * 100)
    P('</div>')
    PrintCGIFooter(file=index_file)


def esc(expr):
  if expr is None:
    return ""
  return html.escape(str(expr), True)


def P(*args, end="\n", file=sys.stdout):
  esc_args = []
  for arg in args[1:]:
    if isinstance(arg, str):
      arg = esc(arg)
    esc_args.append(arg)
  print(args[0].format(*esc_args), end=end, file=file)


def PrintResultCGI(script_name, entries, query, details):
  for entry in entries:
    P('<div class="entry_view">')
    word = entry["word"]
    pron = entry.get("pronunciation")
    word_url = "{}?q={}".format(script_name, urllib.parse.quote(word))
    P('<h2 class="entry_word">', end="")
    P('<a href="{}">{}</a>', word_url, word, end="")
    if not details and pron:
      P(' <span class="title_pron">{}</span>', pron, end="")
    P('</h2>')
    translations = entry.get("translation")
    if translations:
      if tkrzw_dict.PredictLanguage(query) != "en":
        translations = tkrzw_dict.TwiddleWords(translations, query)
      fields = []
      for tran in translations[:8]:
        tran_url = "{}?q={}".format(script_name, urllib.parse.quote(tran))
        value = '<a href="{}" class="tran">{}</a>'.format(esc(tran_url), esc(tran))
        fields.append(value)
      if fields:
        P('<div class="attr attr_tran">', end="")
        print(", ".join(fields), end="")
        P('</div>')
    if details:
      if pron:
        P('<div class="attr attr_pron"><span class="attr_label">発音</span>' +
          ' <span class="attr_value">{}</span></div>', pron)
      for attr_list in INFLECTIONS:
        fields = []
        for name, label in attr_list:
          value = entry.get(name)
          if value:
            value = ('<span class="attr_label">{}</span>'
                     ' <span class="attr_value">{}</span>').format(esc(label), esc(value))
            fields.append(value)
        if fields:
          P('<div class="attr attr_infl">', end="")
          print(", ".join(fields), end="")
          P('</div>')
    for num_items, item in enumerate(entry["item"]):
      if not details and num_items >= 8:
        P('<div class="item item_omit">', label)
        P('<a href="{}">... ...</a>', word_url)
        P('</div>')
        break
      label = item.get("label") or "misc"
      pos = item.get("pos") or "misc"
      pos = POSES.get(pos) or pos
      sections = item["text"].split(" [-] ")
      section = sections[0]
      attr_label = None
      attr_match = regex.search(r"^\[([a-z]+)\]: ", section)
      if attr_match:
        attr_label = WORDNET_ATTRS.get(attr_match.group(1))
        if attr_label:
          section = section[len(attr_match.group(0)):].strip()
      P('<div class="item item_{}">', label)
      P('<div class="item_text item_text1">')
      P('<span class="label">{}</span>', label.upper())
      P('<span class="pos">{}</span>', pos)
      if attr_label:
        fields = []
        annot = ""
        annot_match = regex.search(r"^\(.*?\)", section)
        if annot_match:
          annot = annot_match.group(0)
          section = section[len(annot):].strip()
        for subword in section.split(","):
          subword = subword.strip()
          if subword:
            subword_url = "{}?q={}".format(script_name, urllib.parse.quote(subword))
            fields.append('<a href="{}" class="subword">{}</a>'.format(
              esc(subword_url), esc(subword)))
        if fields:
          P('<span class="subattr_label">{}</span>', attr_label)
          P('<span class="text">', end="")
          if annot:
            P('<span class="annot">{}</span> ', annot)
          print(", ".join(fields))
          P('</span>')
      else:
        while True:
          attr_label = None
          attr_match = regex.search(r"^ *[,、]*[\(（〔]([^\)）〕]+)[\)）〕]", section)
          if not attr_match: break
          for name in regex.split(r"[ ,、]", attr_match.group(1)):
            attr_label = TEXT_ATTRS.get(name)
            if attr_label: break
          if not attr_label: break
          section = section[len(attr_match.group(0)):].strip()
          P('<span class="subattr_label">{}</span>', attr_label)
        P('<span class="text">', end="")
        PrintItemTextCGI(section)
        P('</span>')
      P('</div>')
      if details:
        for section in sections[1:]:
          subattr_label = None
          subattr_link = False
          attr_match = regex.search(r"^\[([a-z]+)\]: ", section)
          eg_match = regex.search(r"^e\.g\.: ", section)
          if attr_match:
            if attr_match.group(1) == "synset": continue
            subattr_label = WORDNET_ATTRS.get(attr_match.group(1))
            if subattr_label:
              section = section[len(attr_match.group(0)):].strip()
              subattr_link = True
          elif eg_match:
            subattr_label = "例"
            section = section[len(eg_match.group(0)):].strip()
          subsections = section.split(" [--] ")
          P('<div class="item_text item_text2 item_text_n">')
          if subattr_label:
            P('<span class="subattr_label">{}</span>', subattr_label)
          if subattr_link:
            fields = []
            for subword in subsections[0].split(","):
              subword = subword.strip()
              if subword:
                subword_url = "{}?q={}".format(script_name, urllib.parse.quote(subword))
                fields.append('<a href="{}" class="subword">{}</a>'.format(
                  esc(subword_url), esc(subword)))
            if fields:
              P('<span class="text">', end="")
              print(", ".join(fields), end="")
              P('</span>')
          else:
            P('<span class="text">')
            PrintItemTextCGI(subsections[0])
            P('</span>')
          P('</div>')
          for subsection in subsections[1:]:
            subsubsections = subsection.split(" [---] ")
            P('<div class="item_text item_text3 item_text_n">')
            PrintItemTextCGI(subsubsections[0])
            P('</div>')
            for subsubsubsection in subsubsections[1:]:
              P('<div class="item_text item_text4 item_text_n">')
              PrintItemTextCGI(subsubsubsection)
              P('</div>')
      P('</div>')
    if details:
      for rel_name, rel_label in (
          ("parent", "語幹"), ("child", "派生"), ("idiom", "熟語"),
          ("related", "関連"), ("cooccurrence", "共起")):
        related = entry.get(rel_name)
        if related:
          P('<div class="attr attr_{}">', rel_name)
          P('<span class="attr_label">{}</span>', rel_label)
          P('<span class="text">')
          fields = []
          for subword in related[:8]:
            subword_url = "{}?q={}".format(script_name, urllib.parse.quote(subword))
            fields.append('<a href="{}" class="subword">{}</a>'.format(
              esc(subword_url), esc(subword)))
          print(", ".join(fields), end="")
          P('</span>')
          P('</div>')
      etym_fields = []
      etym_prefix = entry.get("etymology_prefix")
      if etym_prefix:
        etym_fields.append('<span class="attr_value">{}+</span>'.format(
          esc(etym_prefix)))
      etym_core = entry.get("etymology_core")
      if etym_core:
        etym_core_url = "{}?q={}".format(script_name, urllib.parse.quote(etym_core))
        etym_fields.append('<a href="{}" class="subword">{}</a>'.format(
            esc(etym_core_url), esc(etym_core)))
      etym_suffix = entry.get("etymology_suffix")
      if etym_suffix:
        etym_fields.append('<span class="attr_value">+{}</span>'.format(
          esc(etym_suffix)))
      if etym_fields:
        P('<div class="attr attr_etym">')
        P('<span class="attr_label">語源</span>')
        P('<span class="text">')
        print(" ".join(etym_fields))
        P('</span>')
        P('</div>')
      prob = entry.get("probability")
      aoa = entry.get("aoa") or entry.get("aoa_concept") or entry.get("aoa_base")
      if aoa or prob:
        P('<div class="attr attr_prob">')
        if prob:
          prob = float(prob)
          if prob > 0:
            fmt = "{{:.{}f}}".format(min(max(int(math.log10(prob) * -1 + 1), 3), 6))
            prob_expr = regex.sub(r"\.(\d{3})(\d*?)0+$", r".\1\2", fmt.format(prob * 100))
            P('<span class="attr_label">頻度</span>' +
              ' <span class="attr_value">{}%</span>', prob_expr)
        if aoa:
          aoa = float(aoa)
          P('<span class="attr_label">年齢</span>' +
            ' <span class="attr_value">{:.2f}</span>', aoa)
        P('</div>')
    P('</div>')


def PrintItemTextCGI(text):
  P('<span class="text">', end="")
  while text:
    match = regex.search("(^|.*?[。、])([\(（〔].+?[\)）〕])", text)
    if match:
      print(esc(match.group(1)), end="")
      P('<span class="annot">{}</span>', match.group(2), end="")
      text = text[len(match.group(0)):]
    else:
      print(esc(text), end="")
      break
  P('</span>', end="")


def PrintResultCGIList(script_name, entries, query):
  P('<div class="list_view">')
  for entry in entries:
    word = entry["word"]
    word_url = "{}?q={}".format(script_name, urllib.parse.quote(word))
    P('<div class="list_item">')
    P('<a href="{}" class="list_head">{}</a> :', word_url, word)
    poses = []
    for pos in GetEntryPoses(entry):
      pos = POSES.get(pos) or pos[:1]
      P('<span class="list_label">{}</span>', pos, end="")
    P('<span class="list_text">', end="")
    translations = entry.get("translation")
    if translations:
      if tkrzw_dict.PredictLanguage(query) != "en":
        translations = tkrzw_dict.TwiddleWords(translations, query)
      fields = []
      for tran in translations[:8]:
        tran_url = "{}?q={}".format(script_name, urllib.parse.quote(tran))
        value = '<a href="{}" class="list_tran">{}</a>'.format(esc(tran_url), esc(tran))
        fields.append(value)
      print(", ".join(fields), end="")
    else:
      text = ""
      for item in entry["item"]:
        text = item["text"].split(" [-] ")[0]
        break
      if text:
        text = CutTextByWidth(text, 70)
        P('<span class="list_gross">{}</span>', text)
    P('</span>')
    P('</div>')
  P('</div>')


def PrintResultCGIAnnot(script_name, spans, head_level, file=sys.stdout):
  def P(*args, end="\n"):
    global P
    P(*args, end, file=file)
  class_tags = ["annot_view"]
  if head_level > 0:
    class_tags.append("annot_head_{}".format(head_level))
  P('<div class="{}">', " ".join(class_tags))
  ruby_trans = None
  ruby_word = None
  ruby_annots = None
  ruby_aoa = 0
  ruby_life = 0
  def StartRuby():
    word_url = "{}?q={}".format(script_name, urllib.parse.quote(ruby_word))
    P('<ruby><span class="word" onmouseover="show_tip(this)" onclick="fix_tip(this)">',
      word_url, end="")
  def EndRuby(ruby_trans):
    word_width = 0
    for c in ruby_word:
      word_width += 2 if ord(c) > 256 else 1
    tran_width = max(word_width * 2, 20)
    ruby_text = ""
    for tran in ruby_trans:
      if tran_width < 6: break
      if ruby_text:
        ruby_text += ","
        tran_width -= 1
      for c in tran:
        tran_width -= 2 if ord(c) > 256 else 1
        if tran_width <= 0:
          ruby_text += ".."
          break
        ruby_text += c
    P('<span class="tip" onmouseleave="hide_tip(this)">')
    max_items = 8 if len(ruby_annots) < 2 else 4
    for entry in ruby_annots:
      P('<div class="annot_entry">')
      word = entry["word"]
      word_url = "{}?q={}".format(script_name, urllib.parse.quote(word))
      P('<div class="annot_title">')
      P('<a href="{}" class="annot_title_word">{}</a>', word_url, word)
      pron = entry.get("pronunciation")
      if pron:
        P('<span class="annot_title_pron">{}</span>', pron)
      aoa = entry.get("aoa_syn")
      if aoa:
        P('<span class="annot_title_aoa">{}</span>', aoa)
      P('</div>')
      trans = entry.get("translation")
      if trans:
        P('<div class="annot_tran">{}</div>', ", ".join(trans[:6]))
      items = entry.get("item")
      for item in items[:max_items]:
        pos = item["pos"]
        pos_label = POSES.get(pos) or pos
        text = item["text"]
        text = regex.sub(r" \[-.*", "", text).strip()
        P('<div class="annot_item"><span class="annot_pos">{}</span> {}</div>', pos_label, text)
      if len(items) > max_items:
        P('<a href="{}" class="annot_item_more">... ...</a>', word_url)
      P('</div>')
    P('</span>', end="")
    P('</span>', end="")
    P('<rt data-aoa="{}" data-word="{}">{}</rt>', ruby_aoa, ruby_word, ruby_text, end="")
    P('</ruby>', end="")
    ruby_trans.clear()
  P('<p class="text">', end="")
  for i in range(0, len(spans)):
    text, is_word, annots = spans[i]
    if regex.search(r"[^\s]", text) or ruby_life == 1:
      ruby_life -= 1
    if ruby_trans and ruby_life == 0:
      EndRuby(ruby_trans)
    if annots:
      if ruby_life <= 0:
        for entry in annots:
          word = entry["word"]
          trans = entry.get("translation")
          if not trans: continue
          ruby_trans = trans
          ruby_word = word
          ruby_aoa = entry.get("aoa_syn") or 100
          ruby_life = word.count(" ") + 1
          break
        if ruby_trans:
          ruby_annots = annots
          StartRuby()
      else:
        ruby_annots.extend(annots)
    P('{}', text, end="")
  if ruby_trans:
    EndRuby(ruby_trans)
  P('</p>')
  P('</div>')


def ReadHTTPQuery(url, error_notes):
  try:
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as response:
      info = response.info()
      clen = int(info.get("Content-Length") or 0)
      if clen > CGI_MAX_HTTP_CONTENT_LENGTH:
        error_notes.append("HTTPのデータが長すぎる。")
        return None, None
      ctype = info.get("Content-Type") or "text/plain"
      charset = ""
      match = regex.search("charset=([-_a-zA-Z0-9]+)", ctype)
      if match:
        charset = match.group(1)
      if not charset:
        charset = "utf-8"
      ctype = regex.sub(r";.*", "", ctype).strip()
      if ctype in ("text/html", "application/xhtml+xml", "application/xml"):
        text = response.read(clen).decode(charset)
        return text, "html"
      elif ctype in ("text/plain", "text/tab-separated-values", "text/csv"):
        text = response.read(clen).decode(charset)
        return text, "text"
      error_notes.append("未知のメディアタイプ。")
      return None, None
  except urllib.error.URLError:
    error_notes.append("不正なURL。")
  except urllib.error.HTTPError:
    error_notes.append("HTTP通信障害。")
  except UnicodeError:
    error_notes.append("エンコーディングエラー。")
  return None, None


def PrintCGIHeader(page_title, file=sys.stdout):
  print("""<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" lang="ja">
<head>
<title>{}</title>
<style type="text/css">/*<![CDATA[*/
html {{ margin: 0ex; padding: 0ex; background: #eeeeee; font-size: 12pt; }}
body {{ margin: 0ex; padding: 0ex; text-align: center; -webkit-text-size-adjust: 100%; }}
article {{ display: inline-block; width: 100ex; text-align: left; padding-bottom: 3ex; }}
a,a:visited {{ text-decoration: none; }}
a:hover {{ color: #0011ee; text-decoration: underline; }}
h1 a,h2 a {{ color: #000000; text-decoration: none; }}
h1 {{ font-size: 110%; margin: 1ex 0ex 0ex 0ex; }}
h2 {{ font-size: 105%; margin: 0.7ex 0ex 0.3ex 0.8ex; }}
.search_form,.entry_view,.list_view,.annot_view,.message_view,.license {{
  border: 1px solid #dddddd; border-radius: 0.5ex;
  margin: 1ex 0ex; padding: 0.8ex 1ex 1.3ex 1ex; background: #ffffff; position: relative; }}
#query_line,#annot_navi_line {{ color: #333333; }}
#query_input {{ zoom: 110%; color: #111111; width: 32ex; }}
#query_input_annot {{ color: #111111; width: 99%; height: 30ex; }}
#index_mode_box,#search_mode_box,#view_mode_box {{ color: #111111; width: 14ex; }}
#submit_button {{ color: #111111; width: 10ex; }}
#submit_button_annot {{ color: #111111; width: 20ex; }}
#clear_button_annot {{ color: #111111; width: 8ex; }}
.license {{ opacity: 0.7; font-size: 90%; padding: 2ex 3ex; }}
.license a {{ color: #001166; }}
.license ul {{ font-size: 90%; }}
.message_view {{ position: relative; opacity: 0.9; font-size: 90%; padding: 1ex 2ex; }}
.message_view p {{ margin: 0; padding: 0; }}
.pagenavi {{ float: right; }}
.pagenavi a {{ min-width: 3ex; color: #002244; padding-left: 0.5ex; }}
.title_pron {{ margin-left: 1.5ex; font-size: 85%; font-weight: normal; color: #444444; }}
.title_pron:before,.title_pron:after {{ content: "/"; font-size: 90%; color: #999999; }}
.attr,.item {{ color: #999999; }}
.attr a,.item a {{ color: #111111; }}
.attr a:hover,.item a:hover {{ color: #0011ee; }}
.attr {{ margin-left: 3ex; }}
.item_text1 {{ margin-left: 3ex; }}
.item_text2 {{ margin-left: 7ex; }}
.item_text3 {{ margin-left: 10ex; }}
.item_text4 {{ margin-left: 13ex; }}
.item_text_n {{ font-size: 90%; }}
.item_omit {{ margin-left: 4ex; opacity: 0.6; font-size: 90%; }}
.attr_prob {{ margin-left: 3ex; font-size: 95%; }}
.attr_label,.label,.pos,.subattr_label {{
  display: inline-block; border: solid 1px #999999; border-radius: 0.5ex;
  font-size: 65%; min-width: 3.5ex; text-align: center; margin-right: -0.5ex;
  color: #111111; background: #eeeeee; opacity: 0.85; }}
.item_xa .label {{ background: #eeeeee; opacity: 0.7; }}
.item_xz .label {{ background: #eeeeee; opacity: 0.7; }}
.item_wn .label {{ background: #eeffdd; opacity: 0.7; }}
.item_we .label {{ background: #ffddee; opacity: 0.7; }}
.item_wj .label {{ background: #ddeeff; opacity: 0.7; }}
.tran {{ color: #000000; }}
.attr_value {{ margin-left: 0.3ex; color: #111111; }}
.text {{ margin-left: 0.3ex; color: #111111; }}
.annot {{ font-size: 80%; color: #555555; }}
.item_text_n .text {{ color: #333333; }}
.list_view {{ padding: 1.2ex 1ex 1.5ex 1.8ex; }}
.list_item {{ margin: 0.3ex 0.3ex; color: #999999; }}
.list_head {{ font-weight: bold; color: #000000; }}
.list_head:hover {{ color: #0011ee; }}
.list_label {{
  display: inline-block; border: solid 1px #999999; border-radius: 0.5ex;
  font-size: 60%; min-width: 2.5ex; text-align: center; margin-right: 0.2ex;
  color: #111111; background: #eeeeee; opacity: 0.8; }}
.list_text {{ font-size: 95%; margin-left: 0.4ex; }}
.list_tran {{ font-size: 95%; color: #333333; }}
.list_tran:hover {{ color: #0011ee; }}
.list_gross {{ color: #444444; font-size: 95%; }}
.annot_meta {{ background: #eeeeee; }}
.annot_meta h2 {{ font-size: 100%; }}
.annot_meta p {{ padding-left: 0.5ex; }}
.annot_view {{ padding: 0ex 1.5ex; }}
.annot_head_1 {{ font-weight: bold; font-size: 110%; }}
.annot_head_2 {{ font-weight: bold; font-size: 105%; }}
.annot_head_3 {{ font-weight: bold; }}
.annot_view a {{ color: #000000; }}
.annot_view .text {{ line-height: 190%; margin: 1ex 1ex; color: #000000; }}
.word {{ position: relative; display: inline-block; line-height: 110%; }}
.annot_view rt {{ color: #333333; }}
.word .tip {{
  visibility: hidden;
  position: absolute;
  top: 3.2ex;
  left: -1.2ex;
  right: auto;
  width: 50ex;
  height: 40ex;
  overflow: scroll;
  line-height: initial;
  background: #ffff99;
  opacity: 0.95;
  border-radius: 0.5ex;
  padding: 0.5ex 1ex;
  z-index: 1;
  font-size: 0.9rem;
  font-weight: normal;
  box-shadow: 2px 2px 4px #aaaaaa;
}}
.word .fixedtip {{
  visibility: visible;
  background: #fff8aa;
  opacity: 1.0;
}}
.word:hover {{ text-decoration: underline; }}
.word:hover .tip {{ visibility: visible; }}
.annot_entry {{ margin: 0.3ex 0.3ex; }}
.annot_title_word {{ font-weight: bold; }}
.annot_title_pron {{ font-size: 95%; color: #444444; margin-left: 1ex; }}
.annot_title_pron:before,.annot_title_pron:after {{ content: "/"; font-size: 90%; color: #999999; }}
.annot_title_aoa {{ font-size: 80%; color: #666666; margin-left: 1.5ex; opacity: 0.6; }}
.annot_item {{ font-size: 95%; }}
.annot_pos {{
  display: inline-block; border: solid 1px #999999; border-radius: 0.5ex;
  font-size: 65%; min-width: 3.5ex; text-align: center;
  margin-right: -0.3ex; color: #333333; }}
@media (max-device-width:720px) {{
  html {{ background: #eeeeee; font-size: 32pt; }}
  body {{ padding: 0; }}
  h1 {{ padding: 5ex 0 0 8ex; }}
  article {{ width: 100%; overflow-x: hidden; }}
  #query_line,#annot_navi_line {{ font-size: 12pt; zoom: 250%; }}
  .search_form,.entry_view,.list_view,.annot_view,.message_view,.license {{
    padding: 0.8ex 0.8ex; }}
  .attr {{ margin-left: 1ex; }}
  .item_text1 {{ margin-left: 1ex; }}
  .item_text2 {{ margin-left: 3ex; }}
  .item_text3 {{ margin-left: 5ex; }}
  .item_text4 {{ margin-left: 7ex; }}
  .item_text_n {{ font-size: 90%; }}
  .list_view {{ padding: 0.6ex 0.5ex 0.8ex 0.8ex; }}
  .annot_view .text {{ margin: 0.3ex 0.2ex; font-size: 95%; }}
  .word .tip {{
    font-size: 85%;
    width: 35ex;
    height: 30ex;
  }}
}}
/*]]>*/</style>
<script>/*<![CDATA[*/
function startup() {{
  let search_form = document.forms['search_form'];
  if (search_form) {{
    let query_input = search_form.elements['q'];
    if (query_input) {{
      query_input.focus();
    }}
  }}
  let annot_navi_form = document.forms["annot_navi_form"];
  if (annot_navi_form) {{
    toggle_rubies();
  }}
}}
function check_search_form() {{
  let search_form = document.forms["search_form"];
  if (!search_form) return;
  let query = search_form.q.value.trim();
  let re_url = new RegExp("^https?://");
  if (re_url.test(query) || query.length > 2000) {{
    search_form.method = "post";
  }} 
}}
function clear_query() {{
  let search_form = document.forms["search_form"];
  if (!search_form) return;
  search_form.q.value = "";
}}
function toggle_rubies() {{
  let annot_navi_form = document.forms["annot_navi_form"];
  let min_aoa = parseInt(annot_navi_form.min_aoa.value);
  let init_only = annot_navi_form.init_only.value == "1"
  let uniq_words = new Set();
  let elems = document.getElementsByTagName("rt");
  for (let elem of elems) {{
    if (elem.dataset.aoa) {{
      let aoa = parseInt(elem.dataset.aoa);
      let word = elem.dataset.word;
      if (init_only) {{
        if (uniq_words.has(word)) {{
          aoa = 0;
        }} else {{
          uniq_words.add(word)
        }}
      }}
      if (aoa <= min_aoa) {{
        elem.style.display = "none";
      }} else {{
        elem.style.display = null;
      }}
    }}
  }}
}}
function show_tip(parent) {{
  let ww = window.innerWidth - 8;
  let elems = parent.getElementsByClassName("tip");
  for (let elem of elems) {{    
    let list = elem.classList;
    if (!list) continue;
    let rect = elem.getBoundingClientRect();
    let right = rect.left + rect.width;
    if (right > ww && !elem.right_overflow) {{
      elem.right_overflow = right - ww;
    }}
    if (elem.right_overflow) {{
      elem.style.transform = "translateX(" + (- elem.right_overflow) + "px)";
    }}
  }}
}}
function fix_tip(parent) {{
  if (is_touchable()) return;
  let elems = parent.getElementsByClassName("tip");
  for (let elem of elems) {{
    let list = elem.classList;
    if (!list) continue;
    elem.classList.toggle("fixedtip");
  }}
}}
function hide_tip(elem) {{
  let list = elem.classList;
  if (!list) return;
  elem.classList.remove("fixedtip");
}}
function is_touchable() {{
  let ua = navigator.userAgent;
  if (ua.indexOf('iPhone') >= 0 || ua.indexOf('iPad') >= 0 || ua.indexOf('Android') >= 0) {{
    return true;
  }}
  return false;
}}
/*]]>*/</script>
</head>
<body onload="startup()">
<article>
""".format(esc(page_title)), end="", file=file)


def PrintCGIFooter(file=sys.stdout):
  print("""</article>
</body>
</html>
""", end="", file=file)
  

def main_cgi():
  script_name = os.environ.get("SCRIPT_NAME", sys.argv[0])
  request_method = os.environ.get("REQUEST_METHOD", sys.argv[0])
  params = {}
  form = cgi.FieldStorage()
  for key in form.keys():
    value = form[key]
    if isinstance(value, list):
      params[key] = value[0].value
    else:
      params[key] = value.value
  query = (params.get("q") or "").strip()
  error_notes = []
  is_http_query = False
  is_html_query = False
  if regex.search("^https?://", query):
    is_http_query = True
    if CGI_MAX_HTTP_CONTENT_LENGTH <= 0:
      error_notes.append("URL query is not supported.")
    elif request_method != "POST":
      error_notes.append("Non-POST URL query is forbidden.")
    else:
      query, ctype = ReadHTTPQuery(query, error_notes)
      if query == None:
        query = ""
      elif not query:
        error_notes.append("空のデータ。")
      if ctype == "html":
        is_html_query = True
  if (is_html_query or
      (query.startswith("<html") or query.startswith("<?xml") or
       query.startswith("<!DOCTYPE"))):
    query = tkrzw_union_searcher.ConvertHTMLToText(query)
    is_html_query = True
  if len(query) > CGI_MAX_QUERY_LENGTH:
    query = query[:CGI_MAX_QUERY_LENGTH]
  query = "\n".join([regex.sub(r"[\p{C}]+", " ", x).strip() for x in query.split("\n")])
  index_mode = params.get("i") or "auto"
  if index_mode == "auto" and len(query) > 48:
    index_mode = "annot"
  search_mode = params.get("s") or "auto"
  view_mode = params.get("v") or "auto"
  if index_mode == "grade" and not query:
    query = "1"
  page_title = "統合英和辞書検索"
  if query:
    page_title += ": " + regex.sub(r"\s+", " ", query).strip()[:24]
  print("""Content-Type: application/xhtml+xml

""", end="")
  PrintCGIHeader(page_title)
  P('<h1><a href="{}">統合英和辞書検索</a></h1>', script_name)
  if index_mode == "annot":
    if not is_http_query:
      P('<div class="search_form">')
      P('<form method="post" name="search_form" action="{}">', script_name)
      P('<div id="query_line">')
      P('<textarea name="q" id="query_input_annot" cols="80" rows="10">{}</textarea>', query)
      P('</div>')
      P('<div id="query_line">')
      P('<input type="hidden" name="i" value="annot"/>')
      P('<input type="submit" value="注釈" id="submit_button_annot"/>')
      P('<input type="button" value="消去" id="clear_button_annot" onclick="clear_query()"/>')
      P('</div>')
      P('</form>')
      P('</div>')
  else:
    P('<div class="search_form">')
    P('<form method="get" name="search_form" onsubmit="check_search_form()">')
    P('<div id="query_line">')
    P('<input type="text" name="q" value="{}" id="query_input"/>', query)
    P('<input type="submit" value="検索" id="submit_button"/>')
    P('</div>')
    P('<div id="query_line">')
    P('<select name="i" id="index_mode_box">')
    for value, label in (("auto", "索引"), ("normal", "英和"),
                         ("reverse", "和英"), ("inflection", "英和屈折"),
                         ("grade", "等級"), ("annot", "注釈")):
      P('<option value="{}"', esc(value), end="")
      if value == index_mode:
        P(' selected="selected"', end="")
      P('>{}</option>', label)
    P('</select>')
    P('<select name="s" id="search_mode_box">')
    for value, label in (
        ("auto", "検索条件"), ("exact", "完全一致"),
        ("prefix", "前方一致"), ("suffix", "後方一致"), ("contain", "中間一致"),
        ("word", "単語一致"), ("edit", "曖昧一致"), ("related", "類語展開")):
      P('<option value="{}"', esc(value), end="")
      if value == search_mode:
        P(' selected="selected"', end="")
      P('>{}</option>', label)
    P('</select>')
    P('<select name="v" id="view_mode_box">')
    for value, label in (("auto", "表示形式"), ("full", "詳細表示"),
                         ("simple", "簡易表示"), ("list", "リスト表示")):
      P('<option value="{}"', esc(value), end="")
      if value == view_mode:
        P(' selected="selected"', end="")
      P('>{}</option>', label)
    P('</select>')
    P('</div>')
    P('</form>')
    P('</div>')
  if error_notes:
    P('<div class="message_view">')
    for note in error_notes:
      P('<p>{}</p>', note)
    P('</div>')
  elif query:
    searcher = tkrzw_union_searcher.UnionSearcher(CGI_DATA_PREFIX)
    is_reverse = False
    if index_mode == "auto":
      if tkrzw_dict.PredictLanguage(query) != "en":
        is_reverse = True
    elif index_mode == "normal":
      pass
    elif index_mode == "reverse":
      is_reverse = True
    elif index_mode == "inflection":
      lemmas = searcher.SearchInflections(query)
      if lemmas:
        if search_mode in ("auto", "exact"):
          lemmas.append(query)
          query = ",".join(lemmas)
        else:
          query = lemmas[0]
    elif index_mode == "grade":
      search_mode = "grade"
    elif index_mode == "annot":
      search_mode = "annot"
    else:
      raise RuntimeError("unknown index mode: " + index_mode)
    if search_mode in ("auto", "exact"):
      if is_reverse:
        result = searcher.SearchExactReverse(query, CGI_CAPACITY)
      else:
        result = searcher.SearchExact(query, CGI_CAPACITY)
    elif search_mode == "prefix":
      if is_reverse:
        result = searcher.SearchPatternMatchReverse("begin", query, CGI_CAPACITY)
      else:
        result = searcher.SearchPatternMatch("begin", query, CGI_CAPACITY)
    elif search_mode == "suffix":
      if is_reverse:
        result = searcher.SearchPatternMatchReverse("end", query, CGI_CAPACITY)
      else:
        result = searcher.SearchPatternMatch("end", query, CGI_CAPACITY)
    elif search_mode == "contain":
      if is_reverse:
        result = searcher.SearchPatternMatchReverse("contain", query, CGI_CAPACITY)
      else:
        result = searcher.SearchPatternMatch("contain", query, CGI_CAPACITY)
    elif search_mode == "word":
      pattern = r"(^| ){}( |$)".format(regex.escape(query))
      if is_reverse:
        result = searcher.SearchPatternMatchReverse("regex", pattern, CGI_CAPACITY)
      else:
        result = searcher.SearchPatternMatch("regex", pattern, CGI_CAPACITY)
    elif search_mode == "edit":
      if is_reverse:
        result = searcher.SearchPatternMatchReverse("edit", query, CGI_CAPACITY)
      else:
        result = searcher.SearchPatternMatch("edit", query, CGI_CAPACITY)
    elif search_mode == "related":
      if is_reverse:
        result = searcher.SearchRelatedReverse(query, CGI_CAPACITY)
      else:
        result = searcher.SearchRelated(query, CGI_CAPACITY)
    elif search_mode == "grade":
      page = max(Atoi(query), 1)
      result = searcher.SearchByGrade(CGI_CAPACITY, page, True)
      P('<div class="message_view">')
      P('<div class="pagenavi">')
      if page > 1:
        prev_url = "{}?i=grade&q={}".format(script_name, page - 1)
        P('<a href="{}">&#x2B05;</a>', prev_url)
      next_url = "{}?i=grade&q={}".format(script_name, page + 1)
      P('<a href="{}">&#x2B95;</a>', next_url)
      P('</div>')
      P('<p>等級順: <strong>{}</strong></p>', page)
      P('</div>')
    elif search_mode == "annot":
      if view_mode == "auto":
        result = True
        view_mode = "annot"
      else:
        result = []
        for span, is_word, annot in searcher.AnnotateText(query):
          if annot:
            for entry in annot:
              result.append(entry)
    else:
      raise RuntimeError("unknown search mode: " + search_mode)
    if result:
      if view_mode == "auto":
        keys = searcher.GetResultKeys(result)
        if len(keys) < 2:
          PrintResultCGI(script_name, result, query, True)
        elif len(keys) < 6:
          PrintResultCGI(script_name, result, query, False)
        else:
          PrintResultCGIList(script_name, result, query)
        if not is_reverse and index_mode == "auto":
          lemmas =set()
          for lemma in searcher.SearchInflections(query):
            if lemma in keys: continue
            lemmas.add(lemma)
          if lemmas:
            words = set([x["word"] for x in result])
            infl_result = []
            for lemma in lemmas:
              for entry in searcher.SearchExact(lemma, CGI_CAPACITY):
                if entry["word"] in words: continue
                infl_result.append(entry)
            if infl_result:
              PrintResultCGIList(script_name, infl_result, "")
      elif view_mode == "full":
        PrintResultCGI(script_name, result, query, True)
      elif view_mode == "simple":
        PrintResultCGI(script_name, result, query, False)
      elif view_mode == "list":
        PrintResultCGIList(script_name, result, query)
      elif view_mode == "annot":
        P('<div class="message_view">')
        P('<form name="annot_navi_form">')
        P('<div id="annot_navi_line">')
        P('注釈想定年齢:')
        P('<select name="min_aoa" onchange="toggle_rubies()">')
        for min_aoa in range(3, 21):
          P('<option value="{}"', min_aoa, end="")
          if min_aoa == 12:
            P(' selected="selected"')
          P('>{}歳</option>', min_aoa)
        P('</select>')
        P('<select name="init_only" onchange="toggle_rubies()">')
        P('<option value="1" selected="selected">初出のみ</option>')
        P('<option value="0">全て</option>')
        P('</select>')
        P('</div>')
        P('</form>')
        P('</div>')
        if (not is_html_query and not query.startswith("====[META]====") and
            not query.startswith("====[PAGE]====")):
          query = tkrzw_union_searcher.CramText(query)
        meta, pages = tkrzw_union_searcher.DivideTextToPages(query)
        doc_title = ""
        meta_lines = []
        for line in meta:
          line = line.strip()
          if line.startswith("[title]:"):
            line = line[line.find(":") + 1:].strip()
            if line:
              doc_title = line
          elif line:
            meta_lines.append(line)
        if doc_title or meta_lines:
          P('<div class="message_view annot_meta">')
          if doc_title:
            P('<h2>{}</h2>', doc_title)
          for meta_line in meta_lines:
            P('<p>{}</p>', meta_line)
          P('</div>')
        num_sections = 0
        num_words = 0
        num_words_with_annots = 0
        num_annots = 0
        for page in pages:
          for line in page:
            line = line.strip()
            head_level = 0
            match = regex.search(r"^\[head([1-3])\]:", line)
            if match:
              head_level = Atoi(match.group(1))
              line = line[match.end():].strip()
            if not line: continue
            result = searcher.AnnotateText(line)
            PrintResultCGIAnnot(script_name, result, head_level)
            num_sections += 1
            for span, is_word, annots in result:
              if is_word and not regex.search(r"\d", span):
                num_words += 1
                if annots:
                  num_words_with_annots += 1
                  num_annots += len(annots)
        coverage = num_words_with_annots / num_words if num_words else 0
        P('<div class="message_view annot_meta">')
        P('<p>段落数: {} 。単語数: {} 。注釈数: {} 。カバー率: {:.1f}% 。</p>',
          num_sections, num_words, num_annots, coverage * 100)
        P('</div>')
      else:
        raise RuntimeError("unknown view mode: " + view_mode)
    else:
      infl_result = None
      edit_result = None
      if search_mode == "auto":
        if is_reverse:
          edit_result = searcher.SearchPatternMatchReverse("edit", query, CGI_CAPACITY)
        else:
          edit_result = searcher.SearchPatternMatch("edit", query, CGI_CAPACITY)
        if index_mode in ("auto", "normal"):
          lemmas = searcher.SearchInflections(query)
          if lemmas:
            infl_query = ",".join(lemmas)
            infl_result = searcher.SearchExact(infl_query, CGI_CAPACITY)
      subactions = []
      if infl_result:
        subactions.append("屈折検索")
      if edit_result:
        subactions.append("曖昧検索")
      submessage = ""
      if subactions:
        submessage = "{}に移行。".format("、".join(subactions))
      P('<div class="message_view">')
      P('<p>該当なし。{}</p>', submessage)
      P('</div>')
      if infl_result:
        PrintResultCGIList(script_name, infl_result, "")
      if edit_result:
        PrintResultCGIList(script_name, edit_result, "")
  elif index_mode == "annot":
    print("""<div class="license">
<p>これは、英文の自動注釈付与機能です。入力欄に英文を入れて、「注釈」ボタンを押してください。入力された英文の中に現れる全ての語句を英和辞書で調べ、その語義をルビと付箋で表示します。結果に現れた語句が難しそうな場合、その和訳がルビとして振られます。「注釈想定年齢」を変更すると、ルビを振る基準となる難易度が変更されます。</p>
<p>英文中の語句にポインタを合わせると、より詳しい語義が書いてある付箋が表示されます。語句をクリックすると付箋が固定されるので、その付箋の中をスクロールしたり、見出し語をクリックしたりできます。見出し語をクリックすると、その見出し語で英和辞書の検索が行われます。付箋の範囲からポインタを外すと付箋は消えます。</p>
<p>入力欄にURLを指定すると、そのURLのWebページの内容を対象として処理を行います。HTMLとプレーンテキストに対応し、一度に{}KBまでのデータを処理することができます。</p>
</div>""".format(int(CGI_MAX_QUERY_LENGTH / 1024)))
  else:
    print("""<div class="license">
<p>デフォルトでは、英語の検索語が入力されると英和の索引が検索され、日本語の検索語が入力されると和英の索引が検索されます。オプションで索引を明示的に指定できます。英和屈折は、単語の過去形などの屈折形を吸収した検索を行います。等級は、検索語を無視して全ての見出し語を重要度順に表示します。注釈は、英文を和訳の注釈付きの形式に整形します。</p>
<p>検索条件のデフォルトは、完全一致です。つまり、入力語そのものを見出しに含む語が表示されます。ただし、該当がない場合には自動的に曖昧検索が行われて、綴りが似た語が表示されます。オプションで検索条件を以下のものから明示的に選択できます。</p>
<ul>
<li>完全一致 : 見出し語が検索語と完全一致するものが該当する。</li>
<li>前方一致 : 見出し語が検索語で始まるものが該当する。</li>
<li>後方一致 : 見出し語が検索語で終わるものが該当する。</li>
<li>中間一致 : 見出し語が検索語を含むものが該当する。</li>
<li>単語一致 : 見出し語が検索語を単語として含むものが該当する。</li>
<li>曖昧一致 : 見出し語の綴りが検索語の綴りと似ているものが該当する。</li>
<li>類語展開 : 見出し語が検索語と完全一致するものとその類語が該当する。</li>
</ul>
<p>デフォルトでは、表示形式は自動的に設定されます。ヒット件数が1件の場合にはその語の語義が詳細に表示され、ヒット件数が5以下の場合には主要語義のみが表示され、ヒット件数がそれ以上の場合には翻訳語のみがリスト表示されます。結果の見出し語を選択すると詳細表示が見られます。</p>
<p>このサイトはオープンな英和辞書検索のデモです。辞書データは<a href="https://wordnet.princeton.edu/">WordNet</a>と<a href="http://compling.hss.ntu.edu.sg/wnja/index.en.html">日本語WordNet</a>と<a href="https://ja.wiktionary.org/">Wiktionary日本語版</a>と<a href="https://en.wiktionary.org/">Wiktionary英語版</a>を統合したものです。検索システムは高性能データベースライブラリ<a href="https://dbmx.net/tkrzw/">Tkrzw</a>を用いて実装されています。<a href="https://github.com/estraier/tkrzw-dict">コードベース</a>はGitHubにて公開されています。</p>
</div>""")
  PrintCGIFooter()


if __name__=="__main__":
  interface = os.environ.get("GATEWAY_INTERFACE")
  if interface and interface.startswith("CGI/"):
    main_cgi()
  else:
    main()
