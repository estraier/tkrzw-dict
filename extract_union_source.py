#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to extract source TSV from the union dictionary
#
# Usage:
#   extract_union_source_tsv.py input_db label
#
# Example
#   ./extract_union_source_tsv.py union-body.tkh xa
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
import regex
import sys
import tkrzw


def main():
  args = sys.argv[1:]
  if len(args) != 2:
    raise ValueError("invalid arguments")
  input_path = args[0]
  label = args[1]
  dbm = tkrzw.DBM()
  dbm.Open(input_path, False).OrDie()
  it = dbm.MakeIterator()
  it.First().OrDie()
  while True:
    record = it.GetStr()
    if not record: break;
    key, data = record
    entries = json.loads(data)
    for entry in entries:
      word = entry["word"]
      translations = entry.get("translation")
      for item in entry["item"]:
        if item["label"] != label: continue
        fields = []
        fields.append("word={}".format(word))
        fields.append("{}={}".format(item["pos"], item["text"]))
        for name, value in item.items():
          if name in ("word", "label", "pos", "text"): continue
          fields.append("{}={}".format(name, value))
        print("\t".join(fields))
    it.Next()
  dbm.Close().OrDie()


if __name__=="__main__":
  main()
