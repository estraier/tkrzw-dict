#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to check related words
#
# Usage:
#   check_related_words [--data_prefix str] [--language str] words...
#
# Example:
#   $ ./check_related_words.py --data_prefix enwiki --language en barack obama
#   $ ./check_related_words.py --data_prefix jawiki --language ja バラク オバマ
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
import tkrzw_dict
import tkrzw_related_word_predictor
import urllib


CGI_DATA_PREFIX = "result"
CGI_LANGUAGE = "en"


def main():
  args = sys.argv[1:]
  data_prefix = tkrzw_dict.GetCommandFlag(args, "--data_prefix", 1) or "result"
  language = tkrzw_dict.GetCommandFlag(args, "--language", 1) or "en"
  text = " ".join(args)
  if not text:
    raise RuntimeError("words are not specified")
  predictor = tkrzw_related_word_predictor.RelatedWordsPredictor(data_prefix, language)
  rel_words, features = predictor.Predict(text)
  print("==== FEATURES ====")
  for feat_word, feat_score in features[:16]:
    print("{} = {:.2f}".format(feat_word, feat_score))
  print()
  print("==== RELATED WORDS ====")
  for rel_word, rel_score in rel_words[:32]:
    print("{} = {:.4f}".format(rel_word, rel_score))


def esc(expr):
  if expr is None:
    return ""
  return html.escape(str(expr), True)


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
<title>Related Words</title>
<style type="text/css">
html {{ background: #ffffff; }}
body {{ margin: 2ex 2ex; }}
.result_table td {{ min-width: 40ex; vertical-align: top; }}
ul {{ padding-left: 2.5ex; color: #444444; }}
a,a:visited {{ text-decoration: none; }}
a {{ color: #0022aa; }}
a:hover {{ text-decoration: underline; }}
h1 a {{ color: #000000; }}
</style>
</head>
<body>
<h1><a href="{}">Search for Related Words</a></h1>
<div class="query_form">
<form method="get" name="form">
<div>
Query: <input type="text" name="q" value="{}"/>
<input type="submit" value="search"/>
</div>
</form>
</div>""".format(esc(script_name), esc(query)))
  if query:
    predictor = tkrzw_related_word_predictor.RelatedWordsPredictor(CGI_DATA_PREFIX, CGI_LANGUAGE)
    rel_words, features = predictor.Predict(query)
    print('<table class="result_table">')
    print('<tr>')
    print('<td>')
    print('<h2>Features</h2>')
    if features:
      print('<ul>')
      for feat_word, feat_score in features[:64]:
        print('<li><a href="?q={}">{}</a> = {:.2f}</li>'.format(
          esc(urllib.parse.quote(feat_word)), esc(feat_word), feat_score))
      print('</ul>')
    else:
      print('<div>No features.</div>')
    print('</td>')
    print('<td>')
    print('<h2>Related Words</h2>')
    if rel_words:
      print('<ul>')
      for rel_word, rel_score in rel_words[:64]:
        print('<li><a href="?q={}">{}</a> = {:.4f}</li>'.format(
          esc(urllib.parse.quote(rel_word)), esc(rel_word), rel_score))
      print('</ul>')
    else:
      print('<div>No related words.</div>')
    print('</td>')
    print('</tr>')
    print('</table>')
  print("""</body>
</html>""")


if __name__=="__main__":
  interface = os.environ.get("GATEWAY_INTERFACE")
  if interface and interface.startswith("CGI/"):
    main_cgi()
  else:
    main()
