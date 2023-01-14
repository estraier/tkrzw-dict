#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to extract good words from the union dictionary
#
# Usage:
#   extract_union_good_words.py input_db [core_label] [min_labels] [keyword_files]
#
# Example
#   ./extract_union_good_words.py union-body.tkh wn 2 
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
  args = sys.argv[1:]
  if len(args) < 1:
    raise ValueError("invalid arguments")
  input_path = args[0]
  core_label = args[1] if len(args) > 1 else "wn"
  min_labels = int(args[2]) if len(args) > 2 else 0
  keywords = set()
  if len(args) > 3:
    with open(args[3]) as input_file:
      for line in input_file:
        keyword = line.strip().lower()
        if keyword:
          keywords.add(keyword)
  dbm = tkrzw.DBM()
  dbm.Open(input_path, False).OrDie()
  it = dbm.MakeIterator()
  it.First().OrDie()
  outputs = []
  while True:
    record = it.GetStr()
    if not record: break;
    key, data = record
    is_keyword = key in keywords
    entries = json.loads(data)
    for entry in entries:
      word = entry["word"]
      prob = float(entry.get("probability") or 0)
      space_count = word.count(" ")
      labels = set()
      poses = []
      for item in entry["item"]:
        label = item["label"]
        pos = item["pos"]
        labels.add(label)
        if label == core_label and pos not in poses:
          poses.append(pos)
      if not poses:
        for item in entry["item"]:
          poses.append(pos)
          break
      core = entry.get("etymology_core")
      if ((is_keyword and prob > 0.000001) or
          (core and core in keywords) or
          (core_label in labels and len(labels) >= min_labels and prob > 0) or
          (space_count < 1 and len(labels) >= min_labels and prob >= 0.00001)):
        fields = []
        fields.append(word)
        fields.append("{:.7f}".format(prob))
        fields.append(",".join(labels))
        fields.append(",".join(poses))
        output = "\t".join(fields)
        score = math.log(prob + 0.00000001) + 22
        score += len(labels) + math.log(len(entry["item"]))
        score -= space_count
        outputs.append((score, output))
    it.Next()
  outputs = sorted(outputs, key=lambda x: x[0], reverse=True)
  for score, data in outputs:
    print(data)
  dbm.Close().OrDie()


if __name__=="__main__":
  main()
