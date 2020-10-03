#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to search a union dictionary
#
# Usage:
#   search_union.py [--data_prefix str] [--search str] [--view str] words...
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
import regex
import sys
import tkrzw_dict
import tkrzw_union_searcher
import urllib


PAGE_WIDTH = 100
CGI_DATA_PREFIX = "union"
CGI_CAPACITY = 100
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
        r"(形|型|分詞|級|動名詞)+", text):
      continue
    if regex.search(r"の(直接法|直説法|仮定法)(現在|過去)", text):
      continue
    if regex.search(r"の(動名詞|異綴|異体|古語)", text):
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
            if attr_match:
              if attr_match.group(1) == "synset": continue              
              attr_label = WORDNET_ATTRS.get(attr_match.group(1))
              if attr_label:
                section = "{}: {}".format(attr_label, section[len(attr_match.group(0)):].strip())
            subsections = section.split(" [--] ")
            PrintWrappedText(subsections[0], 6)
            for subsection in subsections[1:]:
              subsubsections = subsection.split(" [---] ")
              PrintWrappedText(subsubsections[0], 8)
              for subsubsubsection in subsubsections[1:]:
                PrintWrappedText(subsubsubsection, 10)
        num_items += 1
      if mode == "full":
        related = entry.get("related")
        if related:
          text = "[関連] {}".format(", ".join(related[:8]))
          PrintWrappedText(text, 4)
        coocs = entry.get("cooccurrence")
        if coocs:
          text = "[共起] {}".format(", ".join(coocs[:8]))
          PrintWrappedText(text, 4)
        prob = entry.get("probability")
        if prob:
          text = "[確率] {:.4f}%".format(float(prob) * 100)
          PrintWrappedText(text, 4)
  if mode != "list":
    print()
  

def main():
  args = sys.argv[1:]
  data_prefix = tkrzw_dict.GetCommandFlag(args, "--data_prefix", 1) or "union"
  index_mode = tkrzw_dict.GetCommandFlag(args, "--index", 1) or "auto"
  search_mode = tkrzw_dict.GetCommandFlag(args, "--search", 1) or "auto"
  view_mode = tkrzw_dict.GetCommandFlag(args, "--view", 1) or "auto"
  capacity = int(tkrzw_dict.GetCommandFlag(args, "--capacity", 1) or "100")
  query = " ".join(args)
  if not query:
    raise RuntimeError("words are not specified")
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
  else:
    raise RuntimeError("unknown search mode: " + search_mode)
  if result:
    if len(result) > capacity:
      result = result[:capacity]
    if view_mode == "auto":
      keys = searcher.GetResultKeys(result)
      if len(keys) < 2:
        view_mode = "full"
      elif len(keys) < 6:
        view_mode = "simple"
      else:
        view_mode = "list"
    if view_mode == "list":
      print()
    PrintResult(result, view_mode, query)    
    if view_mode == "list":
      print()
  else:
    print("No result.")


def esc(expr):
  if expr is None:
    return ""
  return html.escape(str(expr), True)


def P(*args, end="\n"):
  esc_args = []
  for arg in args[1:]:
    if isinstance(arg, str):
      arg = esc(arg)
    esc_args.append(arg)
  print(args[0].format(*esc_args), end=end)


def PrintResultCGI(entries, query, details):
  for entry in entries:
    P('<div class="entry">')
    word = entry["word"]
    word_url = "?q={}".format(urllib.parse.quote(word))
    P('<h2 class="entry_word"><a href="{}">{}</a></h2>', word_url, word)
    translations = entry.get("translation")
    if translations:
      if tkrzw_dict.PredictLanguage(query) != "en":
        translations = tkrzw_dict.TwiddleWords(translations, query)
      fields = []
      for tran in translations[:8]:
        tran_url = "?q={}".format(urllib.parse.quote(tran))
        value = '<a href="{}" class="tran">{}</a>'.format(esc(tran_url), esc(tran))
        fields.append(value)
      if fields:
        P('<div class="attr attr_tran">', end="")
        print(", ".join(fields), end="")
        P('</div>')
    if details:
      pron = entry.get("pronunciation")
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
            subword_url = "?q={}".format(urllib.parse.quote(subword))
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
          attr_match = regex.search(r"^\[([a-z]+)\]: ", section)
          if attr_match:
            if attr_match.group(1) == "synset": continue              
            subattr_label = WORDNET_ATTRS.get(attr_match.group(1))
            if subattr_label:
              section = section[len(attr_match.group(0)):].strip()
          subsections = section.split(" [--] ")
          P('<div class="item_text item_text2 item_text_n">')
          if subattr_label:
            fields = []
            for subword in subsections[0].split(","):
              subword = subword.strip()
              if subword:
                subword_url = "?q={}".format(urllib.parse.quote(subword))
                fields.append('<a href="{}" class="subword">{}</a>'.format(
                  esc(subword_url), esc(subword)))
            if fields:
              P('<span class="subattr_label">{}</span>', subattr_label)
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
      related = entry.get("related")
      if related:
        P('<div class="attr attr_related">')
        P('<span class="attr_label">関連</span>')
        P('<span class="text">')
        fields = []
        for subword in related[:8]:
          subword_url = "?q={}".format(urllib.parse.quote(subword))
          fields.append('<a href="{}" class="subword">{}</a>'.format(
            esc(subword_url), esc(subword)))
        print(", ".join(fields), end="")
        P('</span>')
        P('</div>')

      coocs = entry.get("cooccurrence")
      if coocs:
        P('<div class="attr attr_cooc">')
        P('<span class="attr_label">共起</span>')
        P('<span class="text">')
        fields = []
        for subword in coocs[:8]:
          subword_url = "?q={}".format(urllib.parse.quote(subword))
          fields.append('<a href="{}" class="subword">{}</a>'.format(
            esc(subword_url), esc(subword)))
        print(", ".join(fields), end="")
        P('</span>')
        P('</div>')
      prob = entry.get("probability")
      if prob:
        P('<div class="attr attr_prob"><span class="attr_label">頻度</span>' +
          ' <span class="attr_value">{:.4f}%</span></div>', float(prob) * 100)
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
  

def PrintResultCGIList(entries, query):
  P('<div class="list">')
  for entry in entries:
    word = entry["word"]
    word_url = "?q={}".format(urllib.parse.quote(word))
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
        tran_url = "?q={}".format(urllib.parse.quote(tran))
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


def main_cgi():
  script_name = os.environ.get("SCRIPT_NAME", sys.argv[0])
  params = {}
  form = cgi.FieldStorage()
  for key in form.keys():
    value = form[key]
    params[key] = value.value
  query = params.get("q") or ""
  query = query.strip()
  index_mode = params.get("i") or "auto"
  search_mode = params.get("s") or "auto"
  view_mode = params.get("v") or "auto"
  page_title = "統合辞書検索"
  if query:
    page_title += ": " + query
  print("""Content-Type: application/xhtml+xml

<html xmlns="http://www.w3.org/1999/xhtml" lang="ja">
<head>
<title>{}</title>
<style type="text/css">
html {{ margin: 0ex; padding: 0ex; background: #eeeeee; font-size: 12pt; }}
body {{ margin: 0ex; padding: 0ex; text-align: center; -webkit-text-size-adjust: 100%; }}
article {{ display: inline-block; width: 100ex; text-align: left; padding-bottom: 3ex; }}
a,a:visited {{ text-decoration: none; }}
a:hover {{ color: #0011ee; text-decoration: underline; }}
h1 a,h2 a {{ color: #000000; text-decoration: none; }}
h1 {{ font-size: 110%; margin: 1ex 0ex 0ex 0ex; }}
h2 {{ font-size: 105%; margin: 0.7ex 0ex 0.3ex 0.8ex; }}
.search_form,.entry,.list,.message,.license {{
  border: 1px solid #dddddd; border-radius: 0.5ex;
  margin: 1ex 0ex; padding: 0.8ex 1ex 1.3ex 1ex; background: #ffffff; position: relative; }}
#query_line {{ color: #333333; }}
#query_input {{ zoom: 110%; color: #111111; width: 32ex; }}
#index_mode_box,#search_mode_box,#view_mode_box {{ color: #111111; width: 14ex; }}
#submit_button {{ color: #111111; width: 10ex; }}
.license {{ opacity: 0.7; font-size: 90%; padding: 2ex 3ex; }}
.license a {{ color: #001166; }}
.license ul {{ font-size: 90%; }}
.message {{ opacity: 0.9; font-size: 90%; padding: 1ex 2ex; }}
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
.item_wj .label {{ background: #ddeeff; opacity: 0.7; }}
.item_we .label {{ background: #ffddee; opacity: 0.7; }}
.item_wn .label {{ background: #eeffdd; opacity: 0.7; }}
.tran {{ color: #000000; }}
.attr_value {{ margin-left: 0.3ex; color: #111111; }}
.text {{ margin-left: 0.3ex; color: #111111; }}
.annot {{ font-size: 80%; color: #555555; }}
.item_text_n .text {{ color: #333333; }}
.list {{ padding: 1.2ex 1ex 1.5ex 1.8ex; }}
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
@media (max-device-width:720px) {{
  html {{ background: #eeeeee; font-size: 32pt; }}
  body {{ padding: 0.8ex; }}
  article {{ width: 100%; }}
  #query_line {{ font-size: 12pt; zoom: 250%; }}
  .search_form,.entry,.list,.message,.license {{
    padding: 0.5ex 0.5ex; }}
  .attr {{ margin-left: 1ex; }}
  .item_text1 {{ margin-left: 1ex; }}
  .item_text2 {{ margin-left: 3ex; }}
  .item_text3 {{ margin-left: 5ex; }}
  .item_text4 {{ margin-left: 7ex; }}
  .item_text_n {{ font-size: 90%; }}
  .list {{ padding: 0.6ex 0.5ex 0.8ex 0.8ex; }}
}}
</style>
<script>
function startup() {{
  let search_form = document.forms['search_form']
  if (search_form) {{
    let query_input = search_form.elements['q']
    if (query_input) {{
      query_input.focus()
    }}
  }}
}}
</script>
</head>
<body onload="startup()">
<article>
<h1><a href="{}">統合辞書検索</a></h1>
""".format(esc(page_title), esc(script_name), end=""))
  P('<div class="search_form">')
  P('<form method="get" name="search_form">')
  P('<div id="query_line">')
  P('<div id="query_column">')
  P('<input type="text" name="q" value="{}" id="query_input"/>', query)
  P('<input type="submit" value="検索" id="submit_button"/>')
  P('</div>')
  P('<select name="i" id="index_mode_box">')
  for value, label in (("auto", "索引"), ("normal", "英和"),
                       ("reverse", "和英"), ("inflection", "英和屈折")):
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
  if query:
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
    else:
      raise RuntimeError("unknown search mode: " + search_mode)
    if result:
      if view_mode == "auto":
        keys = searcher.GetResultKeys(result)
        if len(keys) < 2:
          PrintResultCGI(result, query, True)
        elif len(keys) < 6:
          PrintResultCGI(result, query, False)
        else:
          PrintResultCGIList(result, query)
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
              PrintResultCGIList(infl_result, "")
      elif view_mode == "full":
        PrintResultCGI(result, query, True)
      elif view_mode == "simple":
        PrintResultCGI(result, query, False)
      elif view_mode == "list":
        PrintResultCGIList(result, query)
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
      P('<div class="message">該当なし。{}</div>', submessage)
      if infl_result:
        PrintResultCGIList(infl_result, "")
      if edit_result:
        PrintResultCGIList(edit_result, "")
  else:
    print("""<div class="license">
<p>デフォルトでは、英語の検索語が入力されると英和の索引が検索され、日本語の検索語が入力されると和英の索引が検索されます。オプションで索引を明示的に指定できます。</p>
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
<p>このサイトはオープンな英和辞書検索のデモです。辞書データは<a href="https://ja.wiktionary.org/">Wiktionary日本語版</a>と<a href="https://en.wiktionary.org/">Wiktionary英語版</a>と<a href="https://wordnet.princeton.edu/">WordNet</a>と<a href="http://compling.hss.ntu.edu.sg/wnja/index.en.html">日本語WordNet</a>を統合したものです。検索システムは高性能データベースライブラリ<a href="https://dbmx.net/tkrzw/">Tkrzw</a>を用いて実装されています。<a href="https://github.com/estraier/tkrzw-dict">コードベース</a>はGitHubにて公開されています。</p>
</div>""")
  print("""</article>
</body>
</html>""")


if __name__=="__main__":
  interface = os.environ.get("GATEWAY_INTERFACE")
  if interface and interface.startswith("CGI/"):
    main_cgi()
  else:
    main()
