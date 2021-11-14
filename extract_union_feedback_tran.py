#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to extract translations from the union dictionary
#
# Usage:
#   extract_union_feedback_tran.py input_db [--synset]
#
# Example
#   ./extract_union_feedback_tran.py union-body.tkh
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
  if len(args) < 1:
    raise ValueError("invalid arguments")
  input_path = args[0]
  is_synset = False
  for arg in args[1:]:
    if arg == "--synset":
      is_synset = True
    else:
      raise ValueError("invalid arguments")
  
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
      if is_synset:
        for item in entry["item"]:
          text = item["text"]
          syn_match = regex.search(r"\[synset\]: ([-0-9a-z]+)", text)
          tran_match = regex.search(r"\[translation\]: ([^\[]+)", text)
          if syn_match and tran_match:
            syn = syn_match.group(1)
            tran = tran_match.group(1)
            syn_trans = [x.strip() for x in tran.split(",")]
            print("{}:{}\t{}".format(word, syn, "\t".join(syn_trans)))
      else:
        translations = entry.get("translation")
        if translations:
          print("{}\t{}".format(word, "\t".join(translations)))
    it.Next()
  dbm.Close().OrDie()


if __name__=="__main__":
  main()
