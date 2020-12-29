#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to dump translation pairs of Wiktionary data ino TSV
#
# Usage:
#   dump_wiktionary_trans.py wikitionary_en_tsv wiktionary_ja_tsv
#   (It prints the result on the standard output.)
#
# Example:
#   ./dump_wiktionary_trans.py wiktionary-en.tsv wiktionary-ja.tsv
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
import os
import regex
import sys
import tkrzw
import tkrzw_dict
import tkrzw_tokenizer
import unicodedata


poses = ("noun", "verb", "adjective", "adverb",
         "pronoun", "auxverb", "preposition", "determiner", "article",
         "interjection", "conjunction",
         "prefix", "suffix", "abbreviation")


def Run(en_path, ja_path):
  records = collections.defaultdict(list)
  ReadEnTrans(en_path, records)
  ReadJaTrans(ja_path, records)
  for word, trans in records.items():
    counts = collections.defaultdict(int)
    for tran in trans:
      counts[tran] += 1
    counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    trans = [x[0] for x in counts]
    print("{}\t{}".format(word, "\t".join(trans)))


def ReadEnTrans(input_path, records):
  with open(input_path) as input_file:
    for line in input_file:
      line = unicodedata.normalize('NFKC', line)
      word = ""
      trans = set()
      for field in line.strip().split("\t"):
        columns = field.split("=", 1)
        if len(columns) != 2: continue
        name, value = columns
        if name == "word":
          word = value
        if name in poses:
          value = regex.sub(r"\[-.*", "", value)
          match = regex.match(r"^\[translation\]: (.*)", value)
          if match:
            text = match.group(1)
            text = regex.sub(r"\(.*?\)", "", text)
            text = regex.sub(r"（.*?）", "", text)
            for tran in regex.split(r"[,、] *", text):
              tran = tran.strip()
              if tran:
                trans.add(tran)
      if word and trans:
        records[word].extend(trans)


def ReadJaTrans(input_path, records):
  tokenizer = tkrzw_tokenizer.Tokenizer()
  with open(input_path) as input_file:
    for line in input_file:
      line = unicodedata.normalize('NFKC', line)
      word = ""
      trans = set()
      for field in line.strip().split("\t"):
        columns = field.split("=", 1)
        if len(columns) != 2: continue
        name, value = columns
        if name == "word":
          word = value
        if name in poses:
          text = regex.sub(r"\[-.*", "", value)
          if regex.search(
              r"の(直接法|直説法|仮定法)?(現在|過去)?(第?[一二三]人称)?[ ・･、]?" +
              r"(単数|複数|現在|過去|比較|最上|進行|完了|動名詞|単純)+[ ・･、]?" +
              r"(形|型|分詞|級|動名詞|名詞|動詞|形容詞|副詞)+", text):
            continue
          if regex.search(r"の(直接法|直説法|仮定法)(現在|過去)", text):
            continue
          if regex.search(r"の(動名詞|異綴|異体|古語|略|省略|短縮|頭字語)", text):
            continue
          if regex.search(r"その他、[^。、]{12,}", text):
            continue
          text = regex.sub(r"\(.*?\)", "", text)
          text = regex.sub(r"（.*?）", "", text)
          text = regex.sub(r"《.*?》", "", text)
          text = regex.sub(r"〔.*?〕", "", text)
          text = regex.sub(r"\{.*?\}", "", text)
          for tran in regex.split(r"[,、。] *", text):
            tran = regex.sub(r"^[\p{S}\p{P}]+ *(が|の|を|に|へ|と|より|から|で|や)", "", tran)
            tran = tran.strip()
            if not regex.match(r"[\p{Han}\p{Hiragana}\p{Katakana}ー]", tran): continue
            tokens = tokenizer.Tokenize("ja", tran, False, False)
            if len(tokens) > 6: continue
            trans.add(tran)
      if word and trans:
        records[word].extend(trans)


def main():
  args = sys.argv[1:]
  if len(args) != 2:
    raise ValueError("two arguments are required")
  en_path = args[0]
  ja_path = args[1]
  Run(en_path, ja_path)


if __name__=="__main__":
  main()
