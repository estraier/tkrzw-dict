#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to search a WordNet dictionary
#
# Usage:
#   search_wordnet.py [--data_prefix str] [--direction str] [--details] words...
#
# Example:
#   ./search_wordnet.py --data_prefix wordnet united states
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
import sys
import textwrap
import tkrzw_dict
import tkrzw_wordnet_searcher
import urllib


PAGE_WIDTH = 100
CGI_DATA_PREFIX = "wordnet"


def PrintWrappedText(text, indent,  multi_byte):
  page_width = PAGE_WIDTH
  if multi_byte:
    page_width = int(page_width * 0.6)
  for line in textwrap.wrap(text, page_width - indent):
    print((" " * indent) + line)


def PrintResultWord(key, entry, show_details):
  print("---- {} ----".format(key))
  if show_details:
    score = entry.get("score")
    if score:
      print("score: {:.6f}".format(float(score)))
    search_score = entry.get("search_score")
    if search_score:
      print("search_score: {:.6f}".format(float(search_score)))
  for item in entry["item"]:
    print()
    title = item.get("word") or key
    pos = item.get("pos")
    if pos:
      title += " [{}]".format(pos)
    print("  {}".format(title))
    translations = item.get("translation")
    if translations:
      translations = tkrzw_dict.DeduplicateWords(translations)
      if not show_details:
        translations = translations[:5]
      PrintWrappedText(format(", ".join(translations)), 4, True)
    gross = item.get("gross")
    if gross:
      PrintWrappedText(gross, 4, False)
    if show_details:
      attrs = ("synonym", "hypernym", "hyponym", "antonym", "similar", "derivative")
      for attr in attrs:
        values = item.get(attr)
        if values:
          PrintWrappedText("{}: {}".format(attr, ", ".join(values)), 6, False)
      score = item.get("score")
      if score:
        PrintWrappedText("score: {:.6f}".format(float(score)), 6, False)
    else:
      synonyms = item.get("synonym")
      if synonyms:
        PrintWrappedText("syn: {}".format(", ".join(synonyms)), 6, False)
  print()


def main():
  args = sys.argv[1:]
  data_prefix = tkrzw_dict.GetCommandFlag(args, "--data_prefix", 1) or "wordnet"
  direction = tkrzw_dict.GetCommandFlag(args, "--direction", 1) or "auto"
  show_details = tkrzw_dict.GetCommandFlag(args, "--details", 0)
  text = " ".join(args)
  if not text:
    raise RuntimeError("words are not specified")
  reverse = False
  if direction == "auto":
    reverse = tkrzw_dict.PredictLanguage(text) != "en"
  elif direction == "reverse":
    reverse = True
  searcher = tkrzw_wordnet_searcher.WordNetSearcher(data_prefix)
  if reverse:
    result = searcher.SearchReverse(text)
  else:
    result = searcher.SearchExact(text)
  if result:
    for key, entry in result:
      PrintResultWord(key, entry, show_details)
  else:
    print("No result.")


def esc(expr):
  if expr is None:
    return ""
  return html.escape(str(expr), True)


def PrintResultWordCGI(key, entry, show_details):
  print('<div class="entry">')
  print('<h2>{}</h2>'.format(esc(key)))
  for item in entry["item"]:
    print('<div class="item">')
    word = item.get("word") or key
    print('<h3>', end='')
    print('<a href="?q={}" class="word">{}</a>'.format(
      esc(urllib.parse.quote(word)), esc(word)))
    pos = item.get("pos")
    if pos:
      print('<span class="pos">[{}]</span>'.format(esc(pos)), end='')
    print('</h3>')
    translations = item.get("translation")
    if translations:
      translations = tkrzw_dict.DeduplicateWords(translations)
      translations = translations[:5]
      print('<div class="translation">', end='')
      esc_trans = []
      for tran in translations:
        esc_trans.append('<a href="?q={}">{}</a>'.format(
          esc(urllib.parse.quote(tran)), esc(tran)))
      print(', '.join(esc_trans), end='')
      print('</div>')
    gross = item.get("gross")
    if gross:
      print('<div class="gross">{}</div>'.format(esc(gross)))
    if show_details:
      attrs = ("synonym", "hypernym", "hyponym", "antonym", "similar", "derivative")
    else:
      attrs = ('synonym',)
    for attr in attrs:
      values = item.get(attr)
      if values:
        print('<div class="relword">', end='')
        print('<span class="rellabel">{}:</span> '.format(esc(attr)), end='')
        esc_relwords = []
        for value in values:
          esc_relwords.append('<a href="?q={}">{}</a>'.format(
            esc(urllib.parse.quote(value)), esc(value)))
        print(', '.join(esc_relwords), end='')
        print('</div>')
    print('</div>')
  print('</div>')


def main_cgi():
  script_name = os.environ.get("SCRIPT_NAME", sys.argv[0])
  params = {}
  form = cgi.FieldStorage()
  for key in form.keys():
    value = form[key]
    params[key] = value.value
  query = params.get("q") or ""
  print("""Content-Type: application/xhtml+xml

<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<title>WordNet Search</title>
<style type="text/css">
html {{ margin: 0ex; padding: 0ex; background: #eeeeee; }}
body {{ margin: 0ex; padding: 0ex; text-align: center; }}
article {{ display: inline-block; width: 100ex; text-align: left; padding-bottom: 3ex; }}
a,a:visited {{ text-decoration: none; }}
a {{ color: #000000; }}
a:hover {{ color: #0011ee; text-decoration: underline; }}
h1 a {{ color: #000000; }}
h1 {{ font-size: 110%; }}
h2 {{ position: absolute; right: 1ex; font-size: 90%; color: #aaaaaa;
   margin: 0ex; font-weight: normal; }}
h3 {{ font-size: 105%; margin: 1ex 0ex 0ex 1ex; }}
.query_form,.entry,.note,.license {{ border: 1px solid #dddddd; border-radius: 0.5ex;
  margin: 1ex 0ex; padding: 0.8ex 1ex 1.3ex 1ex; background: #ffffff; position: relative; }}
.pos {{ margin-left: 0.5ex; font-size: 90%; color: #666666; font-weight: normal; }}
.translation,.gross {{ margin-left: 5ex; }}
.relword {{ margin-left: 7ex; font-size: 95%; }}
.relword a {{ color: #444444; }}
.rellabel {{ color: #888888; }}
.license {{ opacity: 0.7; }}
</style>
</head>
<body>
<article>
<h1><a href="{}">WordNet Search</a></h1>
<div class="query_form">
<form method="get" name="form">
<div>
Query: <input type="text" name="q" value="{}"/>
<input type="submit" value="search"/>
</div>
</form>
</div>""".format(esc(script_name), esc(query)))
  if query:
    reverse = tkrzw_dict.PredictLanguage(query) != "en"
    searcher = tkrzw_wordnet_searcher.WordNetSearcher(CGI_DATA_PREFIX)
    if reverse:
      result = searcher.SearchReverse(query)
    else:
      result = searcher.SearchExact(query)
    if result:
      show_details = not reverse
      for key, entry in result:
        PrintResultWordCGI(key, entry, show_details)
    else:
      print('<div class="note">No result.</div>')
  else:
    print("""<div class="license">
<p>This site demonstrats a search system on a English-Japanese dictionary.  If you input an English word, entries whose titles match it are shown.  If you input a Japanese word, entries whose translations match it are shown.</p>
<p>This service uses data from <a href="https://wordnet.princeton.edu/">WordNet</a> and <a href="http://compling.hss.ntu.edu.sg/wnja/index.en.html">Japanese WordNet.</a></p>

</div>""")
  print("""
</article>
</body>
</html>""")


if __name__=="__main__":
  interface = os.environ.get("GATEWAY_INTERFACE")
  if interface and interface.startswith("CGI/"):
    main_cgi()
  else:
    main()
