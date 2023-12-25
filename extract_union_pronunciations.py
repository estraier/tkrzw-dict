#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to extract pronunciations from the union dictionary
#
# Usage:
#   extract_union_pronunciations.py [--word] [--tran] [--norm] [--single] [--hint] input_db
#
# Example
#   ./extract_union_pronunciations.py union-body.tkh
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
import json
import math
import regex
import sys
import tkrzw


def main():
  args = []
  opt_word = False
  opt_tran = False
  opt_norm = False
  opt_single = False
  opt_hint = False
  for arg in sys.argv[1:]:
    if arg == "--word":
      opt_word = True
    elif arg == "--tran":
      opt_tran = True
    elif arg == "--norm":
      opt_norm = True
    elif arg == "--single":
      opt_single = True
    elif arg == "--hint":
      opt_hint = True
    elif arg.startswith("-"):
      raise ValueError("invalid arguments: " + arg)
    else:
      args.append(arg)
  if len(args) < 1:
    raise ValueError("invalid arguments")
  input_path = args[0]
  dbm = tkrzw.DBM()
  dbm.Open(input_path, False).OrDie()
  it = dbm.MakeIterator()
  it.First().OrDie()
  outputs = []
  while True:
    record = it.GetStr()
    if not record: break;
    key, data = record
    entries = json.loads(data)
    for entry in entries:
      word = entry["word"]
      if opt_single and word.find(" ") >= 0: continue
      prob = float(entry.get("probability") or 0)
      share = entry.get("share")
      if share:
        prob *= float(share)
      labels = set()
      for item in entry["item"]:
        labels.add(item["label"])
      label_score = math.log2(len(labels) + 1)
      aoa = float((entry.get("aoa") or entry.get("aoa_concept") or entry.get("aoa_base")) or 20)
      aoa_score = (25 - min(aoa, 20.0)) / 10.0
      score = prob * label_score * aoa_score
      pronunciation = entry.get("pronunciation")
      if not pronunciation: continue
      pronunciation = regex.sub(r"^\[([^\]]+)\]$", r"\1", pronunciation)
      pronunciation = regex.sub(r"\[(.*?)\]", r"(\1)", pronunciation)
      pronunciation = regex.sub(r"⟨(.*?)⟩", r"\1", pronunciation)
      pronunciation = regex.sub(r":", r"ː", pronunciation)
      pronunciation = regex.sub(r"·", r"", pronunciation)
      if opt_norm:
        pronunciation = regex.sub(r"\((.*?)\)", r"\1", pronunciation)
        pronunciation = regex.sub(r"[ˈ.ˌ]", r"", pronunciation)
      if not pronunciation: continue
      translation = ", ".join((entry.get("translation") or [])[:3])
      outputs.append((-score, word, pronunciation, translation))
    it.Next()
  dbm.Close().OrDie()
  outputs = sorted(outputs)
  if opt_hint:
    symbol_counts = collections.defaultdict(int)
    symbol_words = {}
    for score, word, pronunciation, translation in outputs:
      if word in ["the"]: continue
      pronunciation = regex.sub(r"\(.*?\)", r"", pronunciation)
      for symbol in pronunciation:
        symbol_counts[symbol] += 1
        rec = (word, pronunciation)
        old_recs = symbol_words.get(symbol)
        if old_recs:
          if len(old_recs) < 10:
            old_recs.append(rec)
        else:
          symbol_words[symbol] = [rec]
    symbol_counts = sorted(symbol_counts.items(), key=lambda x: (-x[1], x[0]))
    for symbol, count in symbol_counts:
      if count < 10: continue
      recs = symbol_words[symbol]
      fields = [symbol]
      for word, pron in recs:
        fields.append(word)
        fields.append(pron)
      print("\t".join(fields))
  else:
    uniq_pronunciations = set()
    for score, word, pronunciation, translation in outputs:
      fields = []
      if opt_word:
        fields.append(word)
      elif pronunciation in uniq_pronunciations:
        continue
      fields.append(pronunciation)
      if opt_tran:
        fields.append(translation)
      uniq_pronunciations.add(pronunciation)
      print("\t".join(fields))


if __name__=="__main__":
  main()
