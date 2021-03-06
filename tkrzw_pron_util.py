#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Utility to handle pronunciation
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

_rules_sampa2ipa = [
  ("a", "a"),
  ("b", "b"),
  ("b_<", "ɓ"),
  ("c", "c"),
  ("d", "d"),
  ("d`", "ɖ"),
  ("d_<", "ɗ"),
  ("e", "e"),
  ("f", "f"),
  ("g", "ɡ"),
  ("g_<", "ɠ"),
  ("h", "h"),
  ("h\\", "ɦ"),
  ("i", "i"),
  ("j", "j"),
  ("j\\", "ʝ"),
  ("k", "k"),
  ("l", "l"),
  ("l`", "ɭ"),
  ("l\\", "ɺ"),
  ("m", "m"),
  ("n", "n"),
  ("n`", "ɳ"),
  ("o", "o"),
  ("p", "p"),
  ("p\\", "ɸ"),
  ("q", "q"),
  ("r", "r"),
  ("r`", "ɽ"),
  ("r\\", "ɹ"),
  ("r\\`", "ɻ"),
  ("s", "s"),
  ("s`", "ʂ"),
  ("s\\", "ɕ"),
  ("t", "t"),
  ("t`", "ʈ"),
  ("u", "u"),
  ("v", "v"),
  ("v\\", "ʋ"),
  ("w", "w"),
  ("x", "x"),
  ("x\\", "ɧ"),
  ("y", "y"),
  ("z", "z"),
  ("z`", "ʐ"),
  ("z\\", "ʑ"),
  ("A", "ɑ"),
  ("B", "β"),
  ("B\\", "ʙ"),
  ("C", "ç"),
  ("D", "ð"),
  ("E", "ɛ"),
  ("F", "ɱ"),
  ("G", "ɣ"),
  ("G\\", "ɢ"),
  ("G\\_<", "ʛ"),
  ("H", "ɥ"),
  ("H\\", "ʜ"),
  ("I", "ɪ"),
  ("I\\", "ᵻ"),
  ("J", "ɲ"),
  ("J\\", "ɟ"),
  ("J\\_<", "ʄ"),
  ("K", "ɬ"),
  ("K\\", "ɮ"),
  ("L", "ʎ"),
  ("L\\", "ʟ"),
  ("M", "ɯ"),
  ("M\\", "ɰ"),
  ("N", "ŋ"),
  ("N\\", "ɴ"),
  ("O", "ɔ"),
  ("O\\", "ʘ"),
  ("P", "ʋ"),
  ("Q", "ɒ"),
  ("R", "ʁ"),
  ("R\\", "ʀ"),
  ("S", "ʃ"),
  ("T", "θ"),
  ("U", "ʊ"),
  ("U\\", "ᵿ"),
  ("V", "ʌ"),
  ("W", "ʍ"),
  ("X", "χ"),
  ("X\\", "ħ"),
  ("Y", "ʏ"),
  ("Z", "ʒ"),
  (".", "."),
  ("\"", "ˈ"),
  ("%", "ˌ"),
  ("'", "ʲ"),
  (":", "ː"),
  (":\\", "ˑ"),
  ("-", ""),
  ("@", "ə"),
  ("@\\", "ɘ"),
  ("@`", "ɚ"),
  ("{", "æ"),
  ("}", "ʉ"),
  ("1", "ɨ"),
  ("2", "ø"),
  ("3", "ɜ"),
  ("3\\", "ɞ"),
  ("4", "ɾ"),
  ("5", "ɫ"),
  ("6", "ɐ"),
  ("7", "ɤ"),
  ("8", "ɵ"),
  ("9", "œ"),
  ("&", "ɶ"),
  ("?", "ʔ"),
  ("?\\", "ʕ"),
  ("*", ""),
  ("/", ""),
  ("<", ""),
  ("<\\", "ʢ"),
  (">", ""),
  (">\\", "ʡ"),
  ("^", "ꜛ"),
  ("!", "ꜜ"),
  ("!\\", "ǃ"),
  ("|", "|"),
  ("|\\", "ǀ"),
  ("||", "‖"),
  ("||\\", "ǁ"),
  ("=\\", "ǂ"),
  ("-\\", "‿"),
  ("_\"", "̈"),
  ("_+", "̟"),
  ("_-", "̠"),
  ("_/", "̌"),
  ("_0", "̥"),
  ("_<", ""),
  ("=", "̩"),
  ("_>", "ʼ"),
  ("_?\\", "ˤ"),
  ("_\\", "̂"),
  ("_^", "̯"),
  ("_}", "̚"),
  ("`", "˞"),
  ("~", "̃"),
  ("_A", "̘"),
  ("_a", "̺"),
  ("_B", "̏"),
  ("_B_L", "᷅"),
  ("_c", "̜"),
  ("_d", "̪"),
  ("_e", "̴"),
  ("<F>", "↘"),
  ("_F", "̂"),
  ("_G", "ˠ"),
  ("_H", "́"),
  ("_H_T", "᷄"),
  ("_h", "ʰ"),
  ("_j", "ʲ"),
  ("_k", "̰"),
  ("_L", "̀"),
  ("_l", "ˡ"),
  ("_M", "̄"),
  ("_m", "̻"),
  ("_N", "̼"),
  ("_n", "ⁿ"),
  ("_O", "̹"),
  ("_o", "̞"),
  ("_q", "̙"),
  ("<R>", "↗"),
  ("_", "̌"),
  ("_R_F", "᷈"),
  ("_", "̝"),
  ("_T", "̋"),
  ("_t", "̤"),
  ("_v", "̬"),
  ("_w", "ʷ"),
  ("_X", "̆"),
  ("_x", "̽"),
]
_rules_sampa2ipa = sorted(_rules_sampa2ipa, key=lambda x: len(x[0]), reverse=True)

def SampaToIPA(sampa):
  ipa = ""
  i = 0
  while i < len(sampa):
    step = 1
    hit = False
    for rule in _rules_sampa2ipa:
      if sampa[i:i + len(rule[0])] == rule[0]:
        ipa += rule[1]
        step = len(rule[0])
        hit = True
        break
    if not hit and sampa[i] not in ("(", ")") and ord(sampa[i]) < 128:
      raise RuntimeError("no rule: " + sampa + " : " + sampa[i])
    i += step
  return ipa

