#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to extract inflections from the union dictionary
#
# Usage:
#   extract_union_inflections.py input_db
#
# Example
#   ./extract_union_inflections.py union-body.tkh
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

inflection_names = [
  ("noun_plural", "np"),
  ("verb_singular", "vs"),
  ("verb_present_participle", "vc"),
  ("verb_past", "vp"),
  ("verb_past_participle", "vx"),
  ("adjective_comparative", "ajc"),
  ("adjective_superlative", "ajs"),
  ("adverb_comparative", "avc"),
  ("adverb_superlative", "avs"),
  ("alternative", "a"),
]

def main():
  args = sys.argv[1:]
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
      has_symbol = bool(regex.search("[^\p{Latin}]", word))
      fields = [word]
      for attr, abbr in inflection_names:
        values = entry.get(attr)
        if values:
          infls = []
          for infl in values:
            if has_symbol != bool(regex.search("[^\p{Latin}]", infl)): continue
            infls.append(infl)
          if infls:
            fields.append(abbr + ":" + ",".join(infls))
      if len(fields) > 1:
        prob = float(entry.get("probability") or 0)
        idfexpr = "{:.3f}".format(-math.log(prob)) if prob > math.exp(-20) else "20"
        fields.append("i:" + idfexpr)
        print("\t".join(fields))
    it.Next()
  dbm.Close().OrDie()


if __name__=="__main__":
  main()
