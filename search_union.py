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
      

def PrintResult(key, entries, mode, query):
  if mode != "list":
    print("---- {} ----".format(key))
  for entry in entries:
    if mode != "list":
      print()
    title = entry.get("word") or key
    translations = entry.get("translation")
    if translations:
      if tkrzw_dict.PredictLanguage(query) != "en":
        translations = tkrzw_dict.TwiddleWords(translations, query)
      title += "  \"{}\"".format(", ".join(translations[:6]))
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
      for item in entry["item"]:
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
  if mode != "list":
    print()
  

def main():
  args = sys.argv[1:]
  data_prefix = tkrzw_dict.GetCommandFlag(args, "--data_prefix", 1) or "union"
  search_mode = tkrzw_dict.GetCommandFlag(args, "--search", 1) or "auto"
  view_mode = tkrzw_dict.GetCommandFlag(args, "--view", 1) or "auto"
  query = " ".join(args)
  if not query:
    raise RuntimeError("words are not specified")
  if search_mode == "auto":
    if tkrzw_dict.PredictLanguage(query) == "en":
      search_mode = "exact"
    else:
      search_mode = "reverse"
  if view_mode == "auto":
    if search_mode == "reverse":
      view_mode = "list"
    else:
      view_mode = "simple"
  searcher = tkrzw_union_searcher.UnionSearcher(data_prefix)
  if search_mode == "exact":
    result = searcher.SearchExact(query)
  elif search_mode == "reverse":
    result = searcher.SearchReverse(query)
  else:
    raise RuntimeError("unknown search mode: " + search_mode)
  if result:
    for key, entries in result:
      PrintResult(key, entries, view_mode, query)
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


def PrintResultCGI(result, query, details):
  for key, entries, in result:
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
        for tran in translations[:6]:
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
        subattr_label = None
        attr_match = regex.search(r"^\[([a-z]+)\]: ", section)
        if attr_match:
          subattr_label = WORDNET_ATTRS.get(attr_match.group(1))
          if subattr_label:
            section = section[len(attr_match.group(0)):].strip()
        P('<div class="item item_{}">', label)
        P('<div class="item_text item_text1">')
        P('<span class="label">{}</span>', label.upper())
        P('<span class="pos">{}</span>', pos)
        P('<span class="text">', end="")
        if subattr_label:
          fields = []
          for subword in section.split(","):
            subword = subword.strip()
            if subword:
              subword_url = "?q={}".format(urllib.parse.quote(subword))
              fields.append('<a href="{}" class="subword">{}</a>'.format(
                esc(subword_url), esc(subword)))
          if fields:
            P('<span class="subattr_label">{}</span>', subattr_label)
            P('<span class="text">', end="")
            print(", ".join(fields))
            P('</span>')
        else:
          P('<span class="text">{}</span>', section)
        P('</span>')
        P('</div>')
        if details:
          for section in sections[1:]:
            subattr_label = None
            attr_match = regex.search(r"^\[([a-z]+)\]: ", section)
            if attr_match:
              subattr_label = WORDNET_ATTRS.get(attr_match.group(1))
              if subattr_label:
                section = section[len(attr_match.group(0)):].strip()
            subsections = section.split(" [--] ")
            P('<div class="item_text item_text2">')
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
                print(", ".join(fields))
                P('</span>')
            else:
              P('<span class="text">{}</span>', subsections[0])
            P('</div>')
            for subsection in subsections[1:]:
              subsubsections = subsection.split(" [---] ")
              P('<div class="item_text item_text3">')
              P('<span class="text">{}</span>', subsubsections[0])
              P('</div>')
              for subsubsubsection in subsubsections[1:]:
                P('<div class="item_text item_text4">')
                P('<span class="text">{}</span>', subsubsubsection)
                P('</div>')
        P('</div>')
      if details:
        prob = entry.get("probability")
        if prob:
          P('<div class="attr attr_prob"><span class="attr_label">頻度</span>' +
            ' <span class="attr_value">{:.4f}%</span></div>', float(prob) * 100)
      P('</div>')

def PrintResultCGIList(result, query):
  P('<div class="list">')
  for key, entries, in result:
    for entry in entries:
      word = entry["word"]
      word_url = "?q={}".format(urllib.parse.quote(word))
      P('<div class="list_item">')
      P('<a href="{}" class="list_head">{}</a> :', word_url, word)
      translations = entry.get("translation")
      if translations:
        if tkrzw_dict.PredictLanguage(query) != "en":
          translations = tkrzw_dict.TwiddleWords(translations, query)
        fields = []
        for tran in translations[:6]:
          tran_url = "?q={}".format(urllib.parse.quote(tran))
          value = '<a href="{}" class="list_tran">{}</a>'.format(esc(tran_url), esc(tran))
          fields.append(value)
        P('<span class="list_text">', end="")
        print(", ".join(fields), end="")
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
  search_mode = params.get("s") or "a"
  view_mode = params.get("v") or "a"
  print("""Content-Type: application/xhtml+xml

<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<title>Union Search Search</title>
<style type="text/css">
html {{ margin: 0ex; padding: 0ex; background: #eeeeee; }}
body {{ margin: 0ex; padding: 0ex; text-align: center; }}
article {{ display: inline-block; width: 100ex; text-align: left; padding-bottom: 3ex; }}
a,a:visited {{ text-decoration: none; }}
a:hover {{ color: #0011ee; text-decoration: underline; }}
h1 a,h2 a {{ color: #000000; text-decoration: none; }}
h1 {{ font-size: 110%; }}
h2 {{ font-size: 105%; margin: 0.7ex 0ex 0.3ex 0.8ex; }}
.query_form,.entry,.list,.note,.license {{
  border: 1px solid #dddddd; border-radius: 0.5ex;
  margin: 1ex 0ex; padding: 0.8ex 1ex 1.3ex 1ex; background: #ffffff; position: relative; }}
#query_line {{ color: #333333; }}
#query_input {{ color: #111111; width: 30ex; }}
#search_mode_box,#view_mode_box {{ color: #111111; width: 14ex; }}
#submit_button {{ color: #111111; width: 10ex; }}
.license {{ opacity: 0.7; }}
.license a {{ color: #001166; }}
.attr,.item {{ color: #999999; }}
.attr a,.item a {{ color: #111111; }}
.attr a:hover,.item a:hover {{ color: #0011ee; }}
.attr {{ margin-left: 3ex; }}
.item_text1 {{ margin-left: 3ex; }}
.item_text2 {{ margin-left: 7ex; font-size: 95%; }}
.item_text3 {{ margin-left: 10ex; font-size: 95%; }}
.item_text4 {{ margin-left: 13ex; font-size: 95%; }}
.item_omit {{ margin-left: 4ex; opacity: 0.6; font-size: 90%; }}
.attr_prob {{ margin-left: 3ex; font-size: 95%; }}
.attr_label,.label,.pos,.subattr_label {{
  display: inline-block; border: solid 1px #999999; border-radius: 0.5ex;
  font-size: 65%; min-width: 3.3ex; text-align: center;
  color: #111111; background: #eeeeee; opacity: 0.8; }}
.tran {{ color: #000000; }}
.attr_value {{ color: #111111; }}
.text {{ margin-left: 0.3ex; color: #111111; }}
.list {{ padding: 1.2ex 1ex 1.5ex 1.8ex; }}
.list_item {{ margin: 0.2ex 0.3ex; color: #999999; }}
.list_head {{ font-weight: bold; color: #000000; }}
.list_head:hover {{ color: #0011ee; }}
.list_text {{ font-size: 95%; }}
.list_tran {{ color: #333333; }}
.list_tran:hover {{ color: #0011ee; }}
</style>
</head>
<body>
<article>
<h1><a href="{}">Union Dictionary Search</a></h1>
""".format(esc(script_name), esc(query)), end="")
  P('<div class="query_form">')
  P('<form method="get" name="form">')
  P('<div id="query_line">')
  P('Query: <input type="text" name="q" value="{}" id="query_input"/>', query)
  P('<select name="s" id="search_mode_box">')
  for value, label in (("a", "Auto Mode"), ("e", "En-to-Ja"), ("r", "Ja-to-En")):
    P('<option value="{}"', esc(value), end="")
    if value == search_mode:
      P(' selected="selected"', end="")
    P('>{}</option>', label)
  P('</select>')
  P('<select name="v" id="view_mode_box">')
  for value, label in (("a", "Auto View"), ("f", "Full"), ("s", "Simple"), ("l", "List")):
    P('<option value="{}"', esc(value), end="")
    if value == view_mode:
      P(' selected="selected"', end="")
    P('>{}</option>', label)
  P('</select>')
  P('<input type="submit" value="search" id="submit_button"/>')
  P('</div>')
  P('</form>')
  P('</div>')
  if search_mode == "a":
    if tkrzw_dict.PredictLanguage(query) == "en":
      search_mode = "e"
    else:
      search_mode = "r"
  if query:
    searcher = tkrzw_union_searcher.UnionSearcher(CGI_DATA_PREFIX)
    if search_mode == "e":
      result = searcher.SearchExact(query)
    elif search_mode == "r":
      result = searcher.SearchReverse(query)
    else:
      raise RuntimeError("unknown search mode: " + search_mode)
    if result:
      if view_mode == "a":
        if len(result) < 2:
          PrintResultCGI(result, query, True)
        elif len(result) < 6:
          PrintResultCGI(result, query, False)
        else:
          PrintResultCGIList(result, query)
      elif view_mode == "f":
        PrintResultCGI(result, query, True)
      elif view_mode == "s":
        PrintResultCGI(result, query, False)
      elif view_mode == "l":
        PrintResultCGIList(result, query)
      else:
        raise RuntimeError("unknown view mode: " + view_mode)
    else:
      P('<div class="note">No result.</div>')
  else:
    P('<div class="license">')
    P('<p>This site demonstrats a search system on a English-Japanese dictionary.  If you input an English word, entries whose titles match it are shown.  If you input a Japanese word, entries whose translations match it are shown.</p>')
    P('<p>This service uses data from <a href="https://ja.wiktionary.org/">Japanese Wiktionary</a>, <a href="https://en.wiktionary.org/">English Wiktionary</a>, <a href="https://wordnet.princeton.edu/">WordNet</a>, and <a href="http://compling.hss.ntu.edu.sg/wnja/index.en.html">Japanese WordNet.</a></p>')
    P('<p>This service is implemented with <a href="https://dbmx.net/tkrzw/">Tkrzw</a>, which is a high performance DBM library.  <a href="https://github.com/estraier/tkrzw-dict">The code base</a> is published on GitHub.</p>')
    P('</div>')
  print("""</article>
</body>
</html>""")


if __name__=="__main__":
  interface = os.environ.get("GATEWAY_INTERFACE")
  if interface and interface.startswith("CGI/"):
    main_cgi()
  else:
    main()
