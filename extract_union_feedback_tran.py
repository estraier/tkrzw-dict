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
import tkrzw_tokenizer


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
  tokenizer = tkrzw_tokenizer.Tokenizer()
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
          pos = item["pos"]
          text = item["text"]
          syn_match = regex.search(r"\[synset\]: ([-0-9a-z]+)", text)
          tran_match = regex.search(r"\[translation\]: ([^\[]+)", text)
          if syn_match and tran_match:
            syn = syn_match.group(1)
            tran = tran_match.group(1)
            tran = regex.sub(r"\([^)]+\)", "", tran)
            norm_trans = []
            uniq_trans = set()
            for syn_tran in tran.split(","):
              norm_tran = tokenizer.NormalizeJaWordForPos(pos, syn_tran.strip())
              if norm_tran and norm_tran not in uniq_trans:
                norm_trans.append(norm_tran)
                uniq_trans.add(norm_tran)
            if norm_trans:
              print("{}:{}\t{}".format(word, syn, "\t".join(norm_trans)))
      else:
        poses = set()
        tran_poses = {}
        for item in entry["item"]:
          pos = item["pos"]
          text = item["text"]
          poses.add(pos)
          tran_match = regex.search(r"\[translation\]: ([^\[]+)", text)
          if tran_match:
            tran = tran_match.group(1)
            tran = regex.sub(r"\([^)]+\)", "", tran)
            for syn_tran in tran.split(","):
              syn_tran = syn_tran.strip()
              if syn_tran and syn_tran not in tran_poses:
                tran_poses[syn_tran] = pos
        only_pos = list(poses)[0] if len(poses) == 1 else None
        translations = entry.get("translation")
        if translations:
          norm_trans = []
          uniq_trans = set()
          for tran in translations:
            pos = only_pos
            if not pos:
              pos = tran_poses.get(tran)
            norm_tran = tokenizer.NormalizeJaWordForPos(pos, tran) if pos else tran
            if norm_tran and norm_tran not in uniq_trans:
              norm_trans.append(norm_tran)
              uniq_trans.add(norm_tran)
          if norm_trans:
            print("{}\t{}".format(word, "\t".join(norm_trans)))
    it.Next()
  dbm.Close().OrDie()


if __name__=="__main__":
  main()
