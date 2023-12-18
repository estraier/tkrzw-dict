#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to extract pronunciations from the union dictionary
#
# Usage:
#   extract_union_pronunciations.py [--word] input_db
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

import json
import math
import regex
import sys
import tkrzw

def main():
  args = []
  opt_word = False
  for arg in sys.argv[1:]:
    if arg == "--word":
      opt_word = True
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
      prob = float(entry.get("probability") or 0)
      labels = set()
      for item in entry["item"]:
        labels.add(item["label"])
      score = prob * len(labels)
      pronunciation = entry.get("pronunciation")
      if not pronunciation: continue
      pronunciation = regex.sub(r"\((.*?)\)", r"\1", pronunciation)
      pronunciation = regex.sub(r"[ˈ.ˌ]", r"", pronunciation)
      if not pronunciation: continue
      outputs.append((-score, word, pronunciation))
    it.Next()
  dbm.Close().OrDie()
  outputs = sorted(outputs)
  uniq_pronunciations = set()
  for score, word, pronunciation in outputs:
    fields = []
    if opt_word:
      fields.append(word)
    elif pronunciation in uniq_pronunciations:
      continue
    fields.append(pronunciation)
    uniq_pronunciations.add(pronunciation)
    print("\t".join(fields))


if __name__=="__main__":
  main()
