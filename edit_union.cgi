#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# CGI Script to edit a dictonary
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
import json
import os
import math
import regex
import sys
import tkrzw
import tkrzw_dict
import tkrzw_union_searcher
import urllib
import urllib.request


DICT_PATH = "union-body.tkh"
HEADER = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" lang="ja">
<head>
<title>Edit Dictionary</title>
<meta name="robots" content="noindex,nofollow,noarchive"/>
<style>/*<![CDATA[*/
html { margin: 0ex; padding: 0ex; background: #eeeeee; font-size: 12pt; }
body { margin: 0ex; padding: 0ex; text-align: center; -webkit-text-size-adjust: 100%; }
article { display: inline-block; width: 140ex; text-align: left; padding-bottom: 3ex; }
a { text-decoration: none; color: #000000; }
form div { margin: 0.5ex; }
.submit { width: 20ex; }
/*]]>*/</style>
</head>
<body>
<article>
"""
FOOTER = """</article>
</body>
</html>
"""
POSES = set([
  "noun", "verb", "adjective", "adverb",
  "pronoun", "auxverb", "preposition", "determiner",
  "article", "interjection", "conjunction", "prefix",
  "suffix", "abbreviation", "misc",
])


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


def PrintBody(script_name, request_method, params):
  dbm = tkrzw.DBM()
  op = (params.get("o") or "").strip()
  word = (params.get("w") or "").strip()
  data = (params.get("d") or "").strip()
  P('<h1><a href="{}">Edit Dictionary</a></h1>', script_name)
  messages = []
  posting = False
  if op == "edit" and word and request_method == "POST":
    posting = True
    ok = False
    if data:
      try:
        entries = json.loads(data)
        if not isinstance(entries, list):
          raise ValueError("the top level is not a list")
        for entry in entries:
          if not entry.get("word"):
            raise ValueError("an entry doesn't have word")
          if not entry.get("probability"):
            raise ValueError("an entry doesn't have probability")
          items = entry.get("item")
          if not items:
            raise ValueError("an entry doesn't have item")
          if not isinstance(items, list):
            raise ValueError("an entry has a non-list item")
          for item in items:
            if not item.get("label"):
              raise ValueError("an item doesn't have label")
            if item.get("pos") not in POSES:
              raise ValueError("an item doesn't have pos")
            if not item.get("text"):
              raise ValueError("an item doesn't have text")
          translations = entry.get("translation")
          if translations != None:
            if not isinstance(translations, list):
              raise ValueError("an entry has a non-list translation")
            if not translations:
              raise ValueError("an entry has an empty translation")
            for translation in translations:
              if not translation:
                raise ValueError("a translation is empty")
        data = json.dumps(entries, separators=(",", ":"), ensure_ascii=False)
        ok = True
      except json.JSONDecodeError as e:
        messages.append("JSON Error: " + str(e))
      except ValueError as e:
        messages.append("Format Error: " + str(e))
    else:
      ok = True
    if ok:
      dbm.Open(DICT_PATH, True, dbm="HashDBM").OrDie()
      if data:
        dbm.Set(word, data).OrDie()
        messages.append("The entry has been modified.")
        data = json.dumps(json.loads(data), ensure_ascii=False, indent=2)
      else:
        dbm.Remove(word).OrDie()
        messages.append("The entry has been removed.")
      dbm.Close().OrDie()
  dbm.Open(DICT_PATH, False, dbm="HashDBM").OrDie()
  P('<form method="POST">')
  if word:
    if not posting:
      data = dbm.GetStr(word) or ""
      if data:
        data = json.dumps(json.loads(data), ensure_ascii=False, indent=2)
      else:
        entry = {
          "word": word,
          "item": [{"label": "xa", "pos": "noun", "text": ""}],
          "probability": ".000000",
          "translation": [""],
        }
        data = json.dumps([entry], ensure_ascii=False, indent=2)
        messages.append("the word is missing so a template is shown.")
    P('<div>Key: <strong>{}</strong></div>', word)
    P('<div>')
    P('<textarea name="d" cols="120" rows="30">{}</textarea>', data)
    P('</div>')
    P('<div>')
    P('<input type="hidden" name="w" value="{}"/>', word)
    P('<input type="hidden" name="o" value="edit"/>')
    P('<input type="submit" value="Edit" class="submit"/>')
    P('</div>')
  else:
    P('<div>Key:')
    P('<input type="text" name="w" value="" size="20"/>')
    P('</div>')
    P('<div>')
    P('<input type="submit" value="View" class="submit"/>')
    P('</div>')
  P('</form>')
  messages.append("name={}, count={}, size={}".format(
    dbm.GetFilePath(), dbm.Count(), dbm.GetFileSize()))
  for message in messages:
    P('<p>{}</p>', message)

  dbm.Close().OrDie()


def main():
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
  print("""Content-Type: application/xhtml+xml

""", end="")
  print(HEADER, end="")
  PrintBody(script_name, request_method, params)
  print(FOOTER, end="")


if __name__=="__main__":
  main()
