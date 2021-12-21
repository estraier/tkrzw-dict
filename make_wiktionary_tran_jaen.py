#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to parse the Japense-English grossary file to make a translation TSV file
#
# Usage:
#   make_wiktionary_tran_jaen.py
#
# Example:
#   $ cat wiktionary-gross-jaen.tsv |
#     ./make_wiktionary_tran_jaen.py > wiktionary-tran-jaen.tsv
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
import regex
import sys
import tkrzw_tokenizer

tokenizer = tkrzw_tokenizer.Tokenizer()
word_dict = collections.defaultdict(list)
alt_source = None
alt_targets = None
for line in sys.stdin:
  fields = line.strip().split("\t")
  if len(fields) != 3: continue
  word, pos, text = fields
  word = tokenizer.NormalizeJaWordForPos(pos, word)
  if pos == "alternative":
    alt_source = word
    alt_targets = set()
    for alt in regex.split(r"[,;]", text):
      if regex.fullmatch(r"[\p{Han}\p{Hiragana}\p{Katakana}ー]+", alt):
        alt_targets.add(alt)
    continue
  text = regex.sub(r"\.$", "", text).strip()
  for tran in regex.split(r"[,;]", text):
    tran = tran.strip()
    if pos == "verb":
      tran = regex.sub(r"^to ", "", tran)
    if pos == "noun":
      tran = regex.sub(r"(?i)^(a|an|the) ", "", tran)
    if not regex.fullmatch(r"[-_\p{Latin}0-9'. ]+", tran): continue
    tokens = tran.split(" ")
    if len(tokens) < 1 or len(tokens) > 5: continue
    word_dict[tran].append(word)
    if alt_source == word:
      for alt in alt_targets:
        word_dict[tran].append(alt)

for word, trans in word_dict.items():
  print("{}\t{}".format(word, "\t".join(set(trans))))
