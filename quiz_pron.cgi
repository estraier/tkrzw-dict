#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# CGI Script to perform quiz of pronunciation of English words
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


import cgi
import collections
import datetime
import html
import json
import os
import math
import random
import regex
import sys
import time
import tkrzw
import tkrzw_dict
import tkrzw_union_searcher
import urllib


PRON_TABLE_PATH = "union-pron-table.tsv"
PRON_AUX_PATH = "pronunciation-ipa.tsv"
PRON_USGB_PATH = "pronunciation-ipa-usgb.tsv"
DICT_URL = "https://dbmx.net/dict/search_union.cgi"
RESULT_DIR = "quiz-pron-result"
NUM_QUESTIONS = 10
LEVEL_BASE_RANGE = 1500
LEVEL_GAMMA = 1.6666
MAX_RECORDS = 30000
STOP_WORDS = {"bes", "mores", "di", "ei"}

RESULT_HTML_HEADER = """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" lang="ja">
<head>
<title>英単語発音記号検定</title>
<meta name="robots" content="noindex,nofollow,noarchive"/>
<style type="text/css"><![CDATA[
html,body,article,p,pre,code,li,dt,dd,td,th,div { font-size: 12pt; }
html { margin: 0; padding: 0; background: #eeeeee; }
body { width: 100%; margin: 0; padding: 0; background: #eeeeee; text-align: center; }
article { display: inline-block; width: 100ex; overflow: hidden; border: 1px solid #aaaaaa; border-radius: 2ex;
  margin: 2ex 1ex; padding: 3ex 3ex; background: #ffffff; text-align: left; line-height: 1.6; color: #111111; }
h1,h2,h3,h4,h5,h6 { color: #000000; margin: 0; text-indent: 0; }
h1 { text-align: center; margin: 0.5ex 0 1.5ex 0; }
h2 small { font-weight: normal; font-size: 90%; }
p { text-indent: 0; }
a { color: #000000; text-decoration: none; }
a:hover { color: #002299; text-decoration: underline; }
.meta_line { text-align: right; margin: 0.2ex 0; padding: 0 1ex; }
.meta_line b { font-size: 110%; }
.pass_label { color: #008811; font-size: 150%; padding: 0.5ex 1ex; }
.fail_label { color: #881100; font-size: 150%; padding: 0.5ex 1ex; }
table { border-collapse: collapse; }
th,td { text-align: left; border: solid 1px #dddddd; padding: 0 1ex; }
td.pron { width: 20ex; }
td.word { width: 20ex; }
td.trans { width: 32ex; }
td.time { width: 15ex; }
]]></style>
<script type="text/javascript"><![CDATA[
"use strict";
function voice_text(text, locale) {
  if (!SpeechSynthesisUtterance) return;
  window.speechSynthesis.cancel();
  let utter = new SpeechSynthesisUtterance(text);
  if (locale == "gb") {
    utter.lang = "en-GB";
  }  else {
    utter.lang = "en-US";
  }
  window.speechSynthesis.speak(utter);
}
]]></script>
</head>
<body>
<article>
"""
RESULT_HTML_FOOTER = """</article>
</body>
</html>
"""


QUIZ_HTML_HEADER = """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" lang="ja">
<head>
<title>英単語発音記号検定</title>
<meta name="robots" content="noindex,nofollow,noarchive"/>
<style type="text/css"><![CDATA[
html,body,article,p,pre,code,li,dt,dd,td,th,div { font-size: 12pt; }
html { margin: 0; padding: 0; background: #eeeeee; }
body { width: 100%; margin: 0; padding: 0; background: #eeeeee; text-align: center; }
article { display: inline-block; width: 100ex; overflow: hidden; border: 1px solid #aaaaaa; border-radius: 2ex;
  margin: 2ex 1ex; padding: 3ex 3ex; background: #ffffff; text-align: left; line-height: 1.6; color: #111111; }
h1,h2,h3,h4,h5,h6 { color: #000000; margin: 0; text-indent: 0; }
h1 { text-align: center; margin: 0.5ex 0 1.5ex 0; }
p { text-indent: 0; }
a { color: #000000; text-decoration: none; }
a:hover { color: #002299; text-decoration: underline; }
#message { display: none; }
#intro input { font-size: 110%; }
#intro select { font-size: 120%; }
#intro button { font-size: 110%; }
#quiz { display: none; }
#quiz_num { text-align: right; font-size: 100%; color: #999999; }
#pron_line { font-size: 120%; color: #999999; }
#quiz_pron { padding: 0 0.1ex 0 0.2ex; font-size: 125%; color: #000000; }
#quiz_answer { font-size: 125%; }
#quiz_submit { font-size: 110%; }
#quiz_hint { font-size: 110%; }
#quiz_stop { font-size: 110%; }
#result { display: none; }
#result_link { font-size: 110%; color: #001199; }
#hint { display: none; margin: 3ex 0ex 0.2ex 0ex; }
#hint table { border-collapse: collapse; table-layout: fixed; }
#hint table td { border: 1px solid #dddddd; border-collapse: collapse; padding: 0.1ex 0.3ex; }
#hint .pron { color: #aaaaaa; font-family: monospace; }
#hint .pron b { color: #000000; font-weight: bold; padding: 0 0.3ex; }
#hint .label { color: #333333; font-size: 90%; width: 23ex; white-space: nowrap; overflow: hidden; }
#hint .examples { color: #333333; width: 70ex; white-space: nowrap; overflow: hidden; }
#hint .examples span { cursor: pointer; }
]]></style>
<script type="text/javascript"><![CDATA[
"use strict";
const symbol_labels = {
  "p": "無声両唇破裂音",
  "b": "有声両唇破裂音",
  "t": "無声歯茎破裂音",
  "d": "有声歯茎破裂音",
  "ʈ": "無声そり舌破裂音",
  "ɖ": "有声そり舌破裂音",
  "c": "無声硬口蓋破裂音",
  "ɟ": "有声硬口蓋破裂音",
  "k": "無声軟口蓋破裂音",
  "ɡ": "有声軟口蓋破裂音",
  "q": "無声口蓋垂破裂音",
  "ɢ": "有声口蓋垂破裂音",
  "ʔ": "声門破裂音",
  "m": "有声両唇鼻音",
  "ɱ": "有声唇歯鼻音",
  "n": "歯茎鼻音",
  "ɳ": "有声反り舌鼻音",
  "ɲ": "有声硬口蓋鼻音",
  "ŋ": "有声軟口蓋鼻音",
  "ɴ": "有声口蓋垂鼻音",
  "ʙ": "有声両唇震え音",
  "r": "有声歯或は歯茎震え音",
  "ʀ": "有声口蓋垂震え音",
  "ɾ": "歯茎はじき音",
  "ɽ": "有声反り舌弾き音",
  "ɸ": "無声両唇摩擦音",
  "β": "有声両唇摩擦音",
  "f": "無声唇歯摩擦音",
  "v": "有声唇歯摩擦音",
  "θ": "無声歯摩擦音",
  "ð": "有声歯摩擦音",
  "s": "無声歯茎摩擦音",
  "z": "有声歯茎摩擦音",
  "ʃ": "無声後部歯茎摩擦音",
  "ʒ": "有声後部歯茎摩擦音",
  "ʂ": "無声反り舌摩擦音",
  "ʐ": "有声反り舌摩擦音",
  "ç": "無声硬口蓋摩擦音",
  "ʝ": "有声硬口蓋摩擦音",
  "x": "無声軟口蓋摩擦音",
  "ɣ": "有声軟口蓋摩擦音",
  "χ": "無声口蓋垂摩擦音",
  "ʁ": "有声口蓋垂摩擦音",
  "ħ": "無声咽頭摩擦音",
  "ʕ": "有声咽頭摩擦音或は接近音",
  "h": "無声声門摩擦音",
  "ɦ": "有声声門摩擦音",
  "ɬ": "無声歯茎側面摩擦音",
  "ɮ": "有声歯或は歯茎側面摩擦音",
  "ʋ": "有声唇歯接近音",
  "ɹ": "有声歯或は歯茎接近音",
  "ɻ": "有声反り舌接近音",
  "j": "有声硬口蓋接近音",
  "ɰ": "有声軟口蓋接近音",
  "l": "有声歯或は歯茎側面接近音",
  "ɭ": "有声反り舌側面接近音",
  "ʎ": "有声硬口蓋側面接近音",
  "ʟ": "有声軟口蓋側面接近音",
  "ƥ": "無声両唇入破音",
  "ɓ": "有声両唇入破音",
  "ƭ": "無声歯或は歯茎入破音",
  "ɗ": "有声歯或は歯茎入破音",
  "ƈ": "無声硬口蓋入破音",
  "ʄ": "有声硬口蓋入破音",
  "ƙ": "無声軟口蓋入破音",
  "ɠ": "有声軟口蓋入破音",
  "ʠ": "無声口蓋垂入破音",
  "ʛ": "有声口蓋垂入破音",
  "ʍ": "無声両唇軟口蓋摩擦音",
  "w": "有声両唇軟口蓋接近音",
  "ɥ": "有声両唇硬口蓋接近音",
  "ʜ": "無声喉頭蓋摩擦音",
  "ʡ": "喉頭蓋破裂音",
  "ʢ": "有声喉頭蓋摩擦音或は接近音",
  "ɧ": "無声後部歯茎・軟口蓋摩擦音",
  "ʘ": "両唇吸着音",
  "ǀ": "歯吸着音",
  "ǃ": "(後部)歯茎吸着音",
  "ǂ": "硬口蓋歯茎吸着音",
  "ǁ": "歯茎側面吸着音",
  "ɺ": "有声歯茎側面弾き音",
  "ɕ": "無声歯茎硬口蓋摩擦音",
  "ʑ": "有声歯茎硬口蓋摩擦音",
  "ⱱ": "有声唇歯弾き音",
  "ʇ": "歯吸着音",
  "ʗ": "後部歯茎吸着音",
  "ʖ": "歯茎側面吸着音",
  "ʆ": "無声歯茎硬口蓋摩擦音",
  "ʓ": "有声歯茎硬口蓋摩擦音",
  "ɼ": "有声歯茎摩擦ふるえ音",
  "ˢ": "",
  "ƫ": "硬口蓋化無声歯或は歯茎破裂音",
  "ɫ": "軟口蓋化有声歯或は歯茎側面接近音",
  "g": "有声軟口蓋破裂音",
  "ʦ": "無声歯茎破擦音",
  "ʣ": "有声歯茎破擦音",
  "ʧ": "無声後部歯茎破擦音",
  "ʤ": "有声後部歯茎破擦音",
  "ʨ": "無声歯茎硬口蓋破擦音",
  "ʥ": "有声歯茎硬口蓋破擦音",
  "ᶿ": "無声歯摩擦音を伴う開放",
  "ᵊ": "中段中舌母音を伴う開放",
  "ᶑ": "そり舌入破音",
  "ƻ": "有声歯茎破擦音",
  "ʞ": "軟口蓋吸着音",
  "ˣ": "無声軟口蓋摩擦音を伴う開放",
  "ƞ": "成節の鼻音",
  "ƛ": "無声歯或は歯茎側面破擦音",
  "λ": "有声歯或は歯茎側面破擦音",
  "ž": "有声後部歯茎摩擦音",
  "š": "無声後部歯茎摩擦音",
  "ǰ": "有声後部歯茎破擦音",
  "č": "無声後部歯茎破擦音",
  "i": "狭前舌非円唇母音",
  "e": "半狭前舌非円唇母音",
  "ɛ": "半開前舌非円唇母音",
  "a": "開前舌非円唇母音",
  "ɑ": "開後舌非円唇母音",
  "ɔ": "半開後舌円唇母音",
  "o": "半狭後舌円唇母音",
  "u": "狭後舌円唇母音",
  "y": "狭前舌円唇母音",
  "ø": "半狭前舌円唇母音",
  "œ": "半開前舌円唇母音",
  "ɶ": "開前舌円唇母音",
  "ɒ": "開後舌円唇母音",
  "ʌ": "半開後舌非円唇母音",
  "ɤ": "半狭後舌非円唇母音",
  "ɯ": "狭後舌非円唇母音",
  "ɨ": "狭中舌非円唇母音",
  "ʉ": "狭中舌円唇母音",
  "ɪ": "準狭準前舌非円唇母音",
  "ʏ": "準狭準前舌円唇母音",
  "ʊ": "準狭準後舌円唇母音",
  "ə": "中段中舌母音",
  "ɵ": "半狭中舌円唇母音",
  "ɐ": "準開中舌母音",
  "æ": "準開前舌非円唇母音",
  "ɜ": "半開中舌非円唇母音",
  "ɚ": "R音性中段中舌母音",
  "ı": "",
  "ɞ": "半開中舌円唇母音",
  "ʚ": "",
  "ɘ": "半狭中舌非円唇母音",
  "ɷ": "準狭準後舌円唇母音",
  "ɩ": "準狭準前舌非円唇母音",
  "ʼ": "放出音",
  "̥": "無声音",
  "̊": "無声音",
  "̬": "有声音",
  "ʰ": "帯気音",
  "̤": "息もれ声",
  "̰": "きしみ声",
  "̼": "舌唇音",
  "̪": "歯音",
  "̺": "舌尖音",
  "̻": "舌端音",
  "̹": "円唇性強",
  "̜": "円唇性弱",
  "̟": "前寄り",
  "̠": "後ろ寄り",
  "̈": "中舌寄り",
  "̽": "中段中舌寄り",
  "̘": "舌根前進",
  "̙": "舌根後退",
  "˞": "R音性",
  "ʷ": "円唇化",
  "ʲ": "硬口蓋化",
  "ˠ": "軟口蓋化",
  "ˤ": "咽頭化",
  "̃": "鼻音化",
  "ⁿ": "鼻腔開放",
  "ˡ": "側面開放",
  "̚": "無開放",
  "̴": "軟口蓋化或は咽頭化",
  "̝": "	上寄り",
  "̞": "	下寄り",
  "̩": "音節主音",
  "̯": "音節副音",
  "͡": "破擦音或は二重調音",
  "̢": "R音性",
  "〓": "開放・破裂",
  ",": "休止",
  "ʻ": "弱い気音",
  "̇": "硬口蓋化・中舌化",
  "˗": "後ろ寄りの変種",
  "˖": "前寄りの変種",
  "ʸ": "高前舌円唇化・硬口蓋化",
  "̣": "狭い変種・摩擦音",
  "̡": "硬口蓋化",
  "̫": "円唇化",
  "ˈ": "第一強勢",
  "ˌ": "第二強勢",
  "ː": "長音",
  "ˑ": "半長音",
  "̆": "超短音",
  ".": "音節の切れ目",
  "|": "小さな纏り",
  "‖": "大きな纏り",
  "‿": "連結",
  "↗": "全体的上昇",
  "↘": "全体的下降",
};
const symbol_examples = {
  ".": ["also", "ˈɔl.soʊ", "after", "ˈæf.tɚ", "only", "ˈoʊn.li", "into", "ˈɪn.tu", "over", "ˈoʊ.vɚ", "example", "ɪɡˈzæm.pəl", "during", "ˈdjʊə.ɹɪŋ", "information", "ˌɪn.fɚˈmeɪ.ʃən", "information", "ˌɪn.fɚˈmeɪ.ʃən", "area", "ˈɛɚ.i.ə"],
  "ˈ": ["a", "ˈeɪ", "that", "ˈðæt", "can", "ˈkæn", "being", "ˈbiɪŋ", "an", "ˈæn", "about", "əˈbaʊt", "also", "ˈɔl.soʊ", "after", "ˈæf.tɚ", "his", "ˈhɪz", "other", "ˈʌðɚ"],
  "ɪ": ["a", "ˈeɪ", "in", "ɪn", "with", "wɪθ", "I", "aɪ̯", "by", "baɪ", "it", "ɪt", "this", "ðɪs", "which", "wɪt͡ʃ", "they", "ðeɪ", "being", "ˈbiɪŋ"],
  "ə": ["about", "əˈbaʊt", "there", "ðɛə", "year", "jɪə", "people", "ˈpipəl", "between", "bəˈtwin", "system", "ˈsɪstəm", "provide", "pɹəˈvaɪd", "article", "ˈɑːtɪkəl", "him", "əm", "example", "ɪɡˈzæm.pəl"],
  "n": ["and", "ænd", "in", "ɪn", "on", "ɒn", "not", "nɒt", "can", "ˈkæn", "an", "ˈæn", "one", "wʌn", "when", "ʍɛn", "know", "noʊ", "include", "ɪnˈkluːd"],
  "t": ["to", "tu", "that", "ˈðæt", "it", "ɪt", "at", "æt", "not", "nɒt", "but", "bʌt", "which", "wɪt͡ʃ", "about", "əˈbaʊt", "what", "ʍʌt", "get", "ɡɛt"],
  "l": ["all", "ɔl", "also", "ˈɔl.soʊ", "will", "wɪl", "like", "laɪ̯k", "include", "ɪnˈkluːd", "people", "ˈpipəl", "only", "ˈoʊn.li", "well", "wɛl", "look", "lʊk", "article", "ˈɑːtɪkəl"],
  "s": ["this", "ðɪs", "use", "juːs", "say", "seɪ", "also", "ˈɔl.soʊ", "first", "fɝst", "so", "soʊ", "such", "sʌt͡ʃ", "see", "ˈsi", "just", "d͡ʒʌst", "some", "sʌm"],
  "ɹ": ["for", "fɔɹ", "from", "fɹʌm", "or", "ɔɹ", "are", "ɑɹ", "your", "jɔɹ", "her", "ɜɹ", "right", "ˈɹaɪt", "through", "θɹu", "provide", "pɹəˈvaɪd", "part", "pɑɹt"],
  "k": ["can", "ˈkæn", "make", "meɪk", "like", "laɪ̯k", "include", "ɪnˈkluːd", "take", "teɪk", "work", "wɜːk", "think", "θɪŋk", "come", "kʌm", "because", "biˈkɔz", "look", "lʊk"],
  "i": ["be", "bi", "he", "hi", "being", "ˈbiɪŋ", "we", "wi", "see", "ˈsi", "people", "ˈpipəl", "only", "ˈoʊn.li", "she", "ʃi", "these", "ðiz", "any", "ˈɛni"],
  "d": ["and", "ænd", "do", "du", "include", "ɪnˈkluːd", "would", "wʊd", "just", "d͡ʒʌst", "should", "ʃʊd", "need", "nid", "provide", "pɹəˈvaɪd", "good", "ɡʊd", "find", "faɪnd"],
  "ʊ": ["about", "əˈbaʊt", "also", "ˈɔl.soʊ", "out", "aʊt", "go", "ɡoʊ", "know", "noʊ", "so", "soʊ", "would", "wʊd", "only", "ˈoʊn.li", "show", "ʃoʊ", "should", "ʃʊd"],
  "m": ["from", "fɹʌm", "make", "meɪk", "time", "taɪm", "more", "mɔː", "my", "maɪ", "some", "sʌm", "them", "ðʌm", "come", "kʌm", "most", "ˈmoʊst", "system", "ˈsɪstəm"],
  "ɛ": ["when", "ʍɛn", "there", "ðɛə", "get", "ɡɛt", "their", "ðɛɚ", "then", "ðɛn", "any", "ˈɛni", "where", "ʍɛɚ", "well", "wɛl", "many", "ˈmɛni", "very", "ˈvɛɹi"],
  "o": ["also", "ˈɔl.soʊ", "go", "ɡoʊ", "know", "noʊ", "so", "soʊ", "only", "ˈoʊn.li", "show", "ʃoʊ", "over", "ˈoʊ.vɚ", "most", "ˈmoʊst", "both", "boʊθ", "control", "kənˈtɹoʊl"],
  "p": ["up", "ʌp", "people", "ˈpipəl", "people", "ˈpipəl", "provide", "pɹəˈvaɪd", "part", "pɑɹt", "page", "peɪd͡ʒ", "example", "ɪɡˈzæm.pəl", "play", "pleɪ", "present", "ˈpɹɛzənt", "place", "pleɪs"],
  "æ": ["and", "ænd", "that", "ˈðæt", "have", "hæv", "as", "æz", "at", "æt", "can", "ˈkæn", "an", "ˈæn", "after", "ˈæf.tɚ", "than", "ðæn", "back", "bæk"],
  "ː": ["you", "jʉː", "use", "juːs", "more", "mɔː", "include", "ɪnˈkluːd", "who", "huː", "work", "wɜːk", "new", "njuː", "our", "ɑː", "article", "ˈɑːtɪkəl", "before", "bɪˈfɔː"],
  "ˌ": ["information", "ˌɪn.fɚˈmeɪ.ʃən", "application", "ˌæplɪˈkeɪʃən", "operation", "ˌɒp.əˈɹeɪ.ʃən", "program", "ˈpɹoʊˌɡɹæm", "access", "ˈækˌsɛs", "illustration", "ˌɪl.əˈstɹeɪ.ʃən", "temperature", "ˈtɛm.pəɹ.əˌt͡ʃɚ", "project", "ˈpɹɑˌd͡ʒɛkt", "January", "ˈd͡ʒænjuˌɛɹi", "population", "ˌpɒpjʊˈleɪʃən"],
  "a": ["I", "aɪ̯", "by", "baɪ", "about", "əˈbaʊt", "out", "aʊt", "time", "taɪm", "like", "laɪ̯k", "my", "maɪ", "now", "naʊ", "high", "ˈhaɪ", "right", "ˈɹaɪt"],
  "b": ["be", "bi", "by", "baɪ", "but", "bʌt", "being", "ˈbiɪŋ", "about", "əˈbaʊt", "between", "bəˈtwin", "because", "biˈkɔz", "before", "bɪˈfɔː", "number", "ˈnʌmbɚ", "back", "bæk"],
  "e": ["a", "ˈeɪ", "they", "ðeɪ", "say", "seɪ", "make", "meɪk", "take", "teɪk", "may", "meɪ", "name", "neɪm", "way", "weɪ", "page", "peɪd͡ʒ", "change", "t͡ʃeɪnd͡ʒ"],
  "f": ["for", "fɔɹ", "from", "fɹʌm", "if", "ɪf", "after", "ˈæf.tɚ", "first", "fɝst", "find", "faɪnd", "before", "bɪˈfɔː", "free", "fɹiː", "form", "fɔːm", "information", "ˌɪn.fɚˈmeɪ.ʃən"],
  "ɑ": ["are", "ɑɹ", "our", "ɑː", "want", "wɑnt", "article", "ˈɑːtɪkəl", "part", "pɑɹt", "large", "lɑɹd͡ʒ", "product", "ˈpɹɑ.dəkt", "start", "stɑːt", "process", "ˈpɹɑsɛs", "problem", "ˈpɹɑbləm"],
  "ʃ": ["which", "wɪt͡ʃ", "such", "sʌt͡ʃ", "she", "ʃi", "show", "ʃoʊ", "should", "ʃʊd", "each", "it͡ʃ", "change", "t͡ʃeɪnd͡ʒ", "information", "ˌɪn.fɚˈmeɪ.ʃən", "much", "mʌt͡ʃ", "discussion", "dɪˈskʌʃən"],
  "ʌ": ["of", "ʌv", "from", "fɹʌm", "but", "bʌt", "one", "wʌn", "what", "ʍʌt", "up", "ʌp", "other", "ˈʌðɚ", "such", "sʌt͡ʃ", "just", "d͡ʒʌst", "some", "sʌm"],
  "z": ["as", "æz", "his", "ˈhɪz", "these", "ðiz", "because", "biˈkɔz", "example", "ɪɡˈzæm.pəl", "result", "ɹɪˈzʌlt", "present", "ˈpɹɛzənt", "bes", "biːz", "those", "ðoʊz", "design", "dɪˈzaɪn"],
  "r": ["configuration", "kənˌfɪɡ.jəˈreɪ.ʃən", "trust", "trʌst", "numbering", "ˈnʌmbəriŋ", "articled", "ˈɑrtɪkəld", "struggle", "ˈstrʌɡəl", "ordering", "ˈɔɹdərɪŋ", "normally", "ˈnɔːrmə.li", "breakfast", "ˈbreɪkˌfɑːst", "membership", "ˈmɛm.bər.ʃɪp", "watering", "ˈwɔː.tər.iŋ"],
  "u": ["to", "tu", "do", "du", "use", "juːs", "include", "ɪnˈkluːd", "who", "huː", "into", "ˈɪn.tu", "two", "tu", "new", "njuː", "through", "θɹu", "group", "ɡɹuːp"],
  "v": ["of", "ʌv", "have", "hæv", "over", "ˈoʊ.vɚ", "provide", "pɹəˈvaɪd", "give", "ɡɪv", "very", "ˈvɛɹi", "even", "ˈivən", "above", "əˈbʌv", "having", "ˈhævɪŋ", "however", "haʊˈɛvɚ"],
  "ɒ": ["on", "ɒn", "not", "nɒt", "off", "ɒf", "follow", "ˈfɒləʊ", "offer", "ˈɒfə", "operation", "ˌɒp.əˈɹeɪ.ʃən", "top", "tɒp", "possible", "ˈpɒsɪbl̩", "body", "ˈbɒdi", "stop", "stɒp"],
  "ɡ": ["get", "ɡɛt", "go", "ɡoʊ", "give", "ɡɪv", "good", "ɡʊd", "example", "ɪɡˈzæm.pəl", "group", "ɡɹuːp", "game", "ɡeɪm", "great", "ɡɹeɪt", "against", "əˈɡɛnst", "government", "ˈɡʌvəmənt"],
  "ɔ": ["for", "fɔɹ", "or", "ɔɹ", "all", "ɔl", "also", "ˈɔl.soʊ", "your", "jɔɹ", "more", "mɔː", "because", "biˈkɔz", "before", "bɪˈfɔː", "form", "fɔːm", "support", "səˈpɔːt"],
  "ʒ": ["just", "d͡ʒʌst", "page", "peɪd͡ʒ", "change", "t͡ʃeɪnd͡ʒ", "image", "ˈɪmɪd͡ʒ", "large", "lɑɹd͡ʒ", "range", "ɹeɪnd͡ʒ", "age", "eɪd͡ʒ", "version", "ˈvɝʒən", "region", "ˈɹiːd͡ʒn̩", "project", "ˈpɹɑˌd͡ʒɛkt"],
  "ɚ": ["their", "ðɛɚ", "after", "ˈæf.tɚ", "other", "ˈʌðɚ", "over", "ˈoʊ.vɚ", "where", "ʍɛɚ", "number", "ˈnʌmbɚ", "under", "ˈʌndɚ", "information", "ˌɪn.fɚˈmeɪ.ʃən", "area", "ˈɛɚ.i.ə", "here", "hɪɚ̯"],
  "h": ["have", "hæv", "he", "hi", "his", "ˈhɪz", "who", "huː", "high", "ˈhaɪ", "how", "haʊ", "here", "hɪɚ̯", "had", "hæd", "having", "ˈhævɪŋ", "however", "haʊˈɛvɚ"],
  "j": ["you", "jʉː", "use", "juːs", "your", "jɔɹ", "year", "jɪə", "new", "njuː", "during", "ˈdjʊə.ɹɪŋ", "value", "ˈvæljuː", "user", "ˈjuzɚ", "view", "vjuː", "review", "ɹɪˈvjuː"],
  "ŋ": ["being", "ˈbiɪŋ", "think", "θɪŋk", "during", "ˈdjʊə.ɹɪŋ", "thing", "θɪŋ", "having", "ˈhævɪŋ", "long", "lɔŋ", "link", "lɪŋk", "something", "ˈsʌmθɪŋ", "single", "ˈsɪŋɡəl", "function", "ˈfʌŋkʃən"],
  "w": ["with", "wɪθ", "which", "wɪt͡ʃ", "we", "wi", "one", "wʌn", "will", "wɪl", "would", "wʊd", "work", "wɜːk", "between", "bəˈtwin", "well", "wɛl", "want", "wɑnt"],
  "ɨ": ["estimate", "ˈɛstɨmɨt", "estimate", "ˈɛstɨmɨt", "deposit", "dɨˈpɑzɪt", "apparently", "əˈpæɹ.ɨnt.li", "specification", "ˈspɛsɪfɨˈkeɪʃən", "evaluate", "ɨˈvaljʊeɪt", "utilize", "ˈjuːtɨlaɪz", "devices", "dɪˈvaɪ.sɨz", "resort", "ɹɨˈzɔt", "experimental", "ɨkspɛɹəˈmɛntəl"],
  "θ": ["with", "wɪθ", "think", "θɪŋk", "through", "θɹu", "both", "boʊθ", "thing", "θɪŋ", "three", "θɹiː", "without", "wɪθˈaʊt", "method", "ˈmɛθəd", "something", "ˈsʌmθɪŋ", "month", "mʌnθ"],
  "͡": ["which", "wɪt͡ʃ", "such", "sʌt͡ʃ", "just", "d͡ʒʌst", "each", "it͡ʃ", "page", "peɪd͡ʒ", "change", "t͡ʃeɪnd͡ʒ", "change", "t͡ʃeɪnd͡ʒ", "image", "ˈɪmɪd͡ʒ", "much", "mʌt͡ʃ", "large", "lɑɹd͡ʒ"],
  "̯": ["I", "aɪ̯", "like", "laɪ̯k", "day", "deɪ̯", "here", "hɪɚ̯", "light", "ɫɐɪ̯ʔ", "air", "ɛə̯", "phone", "foʊ̯n", "I've", "aɪ̯v", "item", "ˈaɪ̯ɾəm", "lie", "laɪ̯"],
  "g": ["goss", "gɑs", "servicing", "ˈsɜːvɪsɪŋg", "guideline", "ˈgaɪd.laɪn", "signaling", "ˈsɪg.nəl.ɪŋ", "investigator", "ɪnˈvɛs.tɪˌgeɪ.tɚ", "guitarist", "gɪˈtɑɹɪst", "focusing", "ˈfoʊkəs.ɪŋg", "agreeing", "əˈgɹiːŋ", "sensing", "ˈsɛnsɪŋg", "totaling", "ˈtoʊ.təl.ɪŋg"],
  "ɝ": ["first", "fɝst", "world", "wɝld", "return", "ɹɪˈtɝn", "turn", "tɝn", "serve", "sɝv", "early", "ˈɝli", "term", "tɝm", "surface", "ˈsɝːfəs", "version", "ˈvɝʒən", "per", "pɝ"],
  "ɜ": ["her", "ɜɹ", "work", "wɜːk", "service", "ˈsɜːvɪs", "person", "ˈpɜːsən", "commercial", "kəˈmɜːʃəl", "search", "sɜːt͡ʃ", "certain", "ˈsɜːtn̩", "circuit", "ˈsɜː.kɪt", "American", "əˈmɜɹ.ə.kən", "works", "wɜːks"],
  "̩": ["model", "ˈmɑdl̩", "possible", "ˈpɒsɪbl̩", "local", "ˈləʊkl̩", "region", "ˈɹiːd͡ʒn̩", "able", "ˈeɪ.bl̩", "multiple", "ˈmʌltɪpl̩", "certain", "ˈsɜːtn̩", "finally", "ˈfaɪ.nl̩.i", "medical", "ˈmɛdɪkl̩", "handle", "ˈhæn.dl̩"],
  "ð": ["that", "ˈðæt", "this", "ðɪs", "they", "ðeɪ", "there", "ðɛə", "their", "ðɛɚ", "other", "ˈʌðɚ", "than", "ðæn", "them", "ðʌm", "these", "ðiz", "then", "ðɛn"],
  "ʤ": ["majors", "ˈmeɪ.ʤəɹz", "jointly", "ˈʤɔɪntli", "mg", "ɛmˈʤi", "eligibility", "ɛlɪˈʤɪbɨlɪti", "Justice", "ˈʤʌstɪs", "rearrange", "ˌri.əˈɹeɪnʤ", "generalize", "ˈʤɛnəɹəlaɪz", "Nazi Germany", "ˈnɑː.t͡si ˈʤəɹ.mɨ.ni", "genomic", "ʤɛˈnoʊ.mɨk", "stoppage", "ˈstɒpɪʤ"],
  "̬": ["capability", "ˌkeɪ.pəˈbɪl.ə.t̬i", "veteran", "ˈvɛ.t̬ə.ɹən", "routeing", "ˈɹuː.t̬ɪŋɡ", "fundamentally", "ˈfʌndəˈmɛn.t̬li", "battlefield", "ˈbæ.t̬əl.fiːld", "genetically", "dʒəˈnɛ.t̬ɪk.li", "geometrical", "ˌdʒiː.əˈmɛt̬ɹɪk.əl", "saturated", "ˈsætʃ.ɚ.eɪ.t̬ɪd", "bottleneck", "ˈbɑt̬l.nɛk", "multilateral", "mʌl.tɪˈlæt̬.ə.rəl"],
  "ʰ": ["characterization", "kʰær.ək.təˈɹaɪˌzeɪʃən", "European Parliament", "jʊɹəˈpʰiən ˈpɑɹl.əmənt", "VPN", "ˌviː.pʰiːˈɛn", "PlayStation", "ˈpʰleɪˌsteɪ.ʃən", "Kawasaki", "kʰɑ.wɑˈsɑ.ki", "pitting", "ˈpʰɪtɪŋ", "New Testament", "nʰjuː ˈtɛstəmənt", "togetherness", "tʰə.ɡɛˈðɚ.nəs", "B party", "bi ˈpaɹtʰi", "P1", "ˈpʰi wʌn"],
  "ʧ": ["structured", "ˈstrʌkʧərd", "picturing", "ˈpɪk.ʧə.ɹɪŋ", "matched", "ˈmæʧt", "HD", "ˈeɪʧ-ˈdi", "HTML", "eɪʧˌtiːɛmˈɛl", "purchasing", "ˈpəɹʧəsɪŋ", "checkout", "ˈʧɛk.aʊ̯t", "culturally", "ˈkʌlʧɹə.li", "enrichment", "ɛnˈɹɪʧ.mənt", "Republic of China", "ɹɪˈpʌb.lɪk əv ˈʧaɪ.nə"],
  "ɾ": ["city", "ˈsɪɾi", "better", "ˈbɛɾɚ", "metal", "ˈmɛɾəɫ", "item", "ˈaɪ̯ɾəm", "evidence", "ˈɛvəɾɪns", "relative", "ˈɹɛl.ə.ɾɪv", "possibility", "ˌpɑ.səˈbɪl.ə.ɾi", "beauty", "ˈbjuɾi", "duty", "duːɾi", "medal", "ˈmɛɾ.ɫ̩"],
  "ɫ": ["still", "stɪɫ", "light", "ɫɐɪ̯ʔ", "metal", "ˈmɛɾəɫ", "fuel", "ˈfjuwəɫ", "circle", "ˈsɝ.kəɫ", "medal", "ˈmɛɾ.ɫ̩", "shoulder", "ˈʃəʊɫdə", "holding", "ˈhoʊɫdɪŋ", "holder", "ˈhoʊɫdɚ", "folder", "ˈfoʊɫdɚ"],
  "ɵ": ["electrolyte", "ɨˈlɛk.tɹɵˌlaɪt", "metropolitan", "mɛtɹɵˈpɑlɨtən", "electrolytic", "ɨˌlɛk.tɹɵˈlɪ.tɪk", "intonation", "ɪntɵˈneɪʃən", "diplomatically", "dɪplɵˈmæt.ɪ.kli", "chromatogram", "kɹoʊˈmæt.ɵ.ɡɹæm", "neoprene", "ˈniɵpɹin", "encyclopaedic", "ɛnsɨklɵˈpɛd.ɪk", "cardiomyopathy", "kɑɹ.di.oʊ.maɪˈɵ.pə.θi", "Molina", "mɵˈlinə"],
  "y": ["UTC", "yu ˈtiː siː", "in Tokyo", "ɪn ˈtoʊ.kyoʊ", "Suzuki", "sy.ˈzyː.ki", "Suzuki", "sy.ˈzyː.ki", "Formula One", "ˈfɔɹmyʊlə wʌn", "especially Tokyo", "ɪˈspɛʃəli ˈtoʊkyoʊ", "immunoassay", "ɪˈmyʊ.nɒs.eɪ", "royally", "ˈɹɔɪəl.y", "New Norwegian", "ˈnyː ˈnoːɹɡiːjən"],
  "ɐ": ["no", "nɐʉ", "light", "ɫɐɪ̯ʔ", "bike", "bɐɪk", "slight", "sl̥ɐɪʔ", "Ernst", "ˈɐrnst", "R acid", "ɐː ˈæsɪd", "four L", "ˈfoʊɐɹ el", "Adolf Hitler", "ˈɑːdɒlf ˈhɪtlɐ", "Wehrmacht", "ˈvæɐ̯.maxt", "Brasil", "bɾɐˈzil"],
  "̃": ["am", "ẽə̃ːm", "identity", "aɪˈdɛɾ̃əɾi", "counter", "ˈkaʊ.ɾ̃ɚ", "dismantle", "dɪsˈmæ̃nɾɫ̩", "Grand Prix", "ɡɹɑ̃ ˈpɹiː", "Atlanta", "ætˈlæɾ̃ə", "incidental", "ɪnsɪˈdɛ.ɾ̃əl", "Punjab", "pə̃.ˈdʒaːb", "Lyon", "ˈli.ɔ̃n", "conversational", "ˌkɑ̃n.və.ˈseɪ.ʃən.əl"],
  "-": ["jurisdiction", "d͡ʒʊɹɪz-", "HD", "ˈeɪʧ-ˈdi", "Seattle", "-ɾɫ", "CLI", "sɪˈɛl.aɪ ˈsiː-ɛl-aɪ", "CLI", "sɪˈɛl.aɪ ˈsiː-ɛl-aɪ", "LSI", "ɛl-es-aɪ", "LSI", "ɛl-es-aɪ", "MVP", "ɛm-vi-ˈpi", "MVP", "ɛm-vi-ˈpi", "unofficially", "ʌn-əˈfɪ.ʃəl.i"],
  "ʉ": ["you", "jʉː", "no", "nɐʉ", "enthusiast", "ɪnˈθʉu̯.ziˌəst", "modular", "ˈmɑdʒʉlɑr", "solubility", "sɒ.lʉˈbɪl.ɪ.ti", "unilateral", "ˌjʉː.nəˈɫæɾ.ɚ.ɻɫ̩", "intramuscular", "ˌɪn.tɹəˈmʌskjʉ.lər", "superannuation", "ˈsʉːpəˌɹænjʉːˈæɪʃn̩", "superannuation", "ˈsʉːpəˌɹænjʉːˈæɪʃn̩", "Fortuna", "foɹˈtʉn.ə"],
  "'": ["structural", "'stɻʌk.tʃhə.ɹəl", "divided", "dɪ'vaɪdɪd", "Roman Catholic", "ˈɹoʊ.mən 'kæθ.ə.lɪk", "FCC", "ˌɛf.siː'siː", "Ibrahim", "ɪ'bɹæ.hɪːm", "2H", "tu'eɪç", "Roman mile", "ɹoʊ̯mən 'maɪ̯l", "boneless", "'boʊnlɪs","CDMA", "ˌsi di ɛm 'ei"],
  "x": ["I say", "x.seɪ", "Reich", "ɹaɪx", "loch", "lɑx", "Ahmed", "ˈɑːx.mɛd", "Bach", "bɑːx", "Kharkiv", "ˈxɑɹkiv", "Wehrmacht", "ˈvæɐ̯.maxt", "context ID", "ˈkɑntɛxt ai di", "Hebei", "xəˈbeɪ", "Cochrane", "ˈkɒxɹən"],
  "‿": ["IMF", "aɪ‿ɛm‿ɛf", "IMF", "aɪ‿ɛm‿ɛf", "NMR", "ɛn‿ˌɛm‿ˈɑɹ", "NMR", "ɛn‿ˌɛm‿ˈɑɹ", "TL", "ˈtiː‿ˈɛl", "Columbia University", "kəˈlʌmbiə‿ˌjuːnɪˈvɜːrsɪti", "TLC", "tiː‿ɛlˈsiː", "shore up", "ˈʃɔɹ‿ʌp", "KK", "ˌkeɪ‿keɪ", "CMP", "ˈsiː‿ɛmˈpiː"],
  "c": ["benchmark", "ˈbɛnchmɑɹk", "CSR", "ciɛs.ɑr", "practicality", "ˌpɹæc.tɪˈkæl.ɪ.ti", "inadequacy", "ɪnˈædəkwəc.i", "PAC", "pæc", "Black Twitter", "ˈblæck ˈtwɪtər", "Versace", "vɛːrsace", "Aceh", "ˈɑcɛh", "Partick", "ˈpaːɹtɨck"],
  "`": ["Almaty", "ɑːl`mɑːtɨ", "CT scan", "`siː`tiː skɹæn", "CT scan", "`siː`tiː skɹæn", "Atami", "ɑˈtɑːmi```", "Atami", "ɑˈtɑːmi```", "Atami", "ɑˈtɑːmi```", "nurturer", "ˈnəɹʧɚ```", "nurturer", "ˈnəɹʧɚ```", "nurturer", "ˈnəɹʧɚ```", "balaclavaed", "bœˈlæ.klæ.vəd```"],
  "ɕ": ["Shizuoka", "ɕizɯoka", "Shinjuku", "ɕindʑukɯ", "Kagoshima", "kagoɕima", "Singapore English", "siŋaˈpɔ.iŋˈɡliɕ", "Bydgoszcz", "ˈbɨdgɒɕ", "Jiangxi", "ʨjan³⁵ɕi", "Szczecin", "ˈʃtʃɛtɕɨn", "Kashima", "kɑɕimɑ", "Beijing dialect", "peɪˈtɕiŋ dɪ.a.lekt", "Shizuoka Prefecture", "ɕizɯoka ken"],
  "ʁ": ["heartbroken", "ˈhɑɹt.bʁoʊken", "Brahms", "ˈbʁɑːms", "Augsburg", "ˈaʊgzbuʁk", "Darmstadt", "ˈdaʁmʃtat", "Marie Antoinette", "mɑˈʁi ɑ̃.twa.ˈnɛt", "Regensburg", "ˈɹɛɡ.əns.bʊʁɡ", "Montmartre", "mɔ̃.maʁtʁø", "Montmartre", "mɔ̃.maʁtʁø", "Weinberg", "ˈwaɪ̯nbɛʁk", "Bruxelles", "bʁy.sɛl"],
  "ɯ": ["Fukuoka", "ɸukɯoka", "Shizuoka", "ɕizɯoka", "Shinjuku", "ɕindʑukɯ", "Tohoku", "toːhokɯꜜ", "Suzuka", "sɯːdzuka", "Hamamatsu", "hɑ.mɑ.ˈmɑ.t͡sɯ", "Takamatsu", "tɑkɑmɑˈt͡sɯ", "ugh", "ɯx", "Dentsu", "de̞n.tsɯ", "Portuguese guitar", "pɔɹˈtɯ.ɡiːz ɡɪˈtɑɹ"],
  "ä": ["phase out", "feɪ̯z äʊ̯t", "endoscopic", "ɛn.doʊˈskäp.ɨk", "microscopical", "maɪ.kɹəˈskäp.ɪ.kəl", "thrice", "θɾ̪̊äɪs", "artisanal", "ˈäɹtəzænl", "Japanese writing", "pʰɛɪ̯.pʰɔʊ̯ ˈɹäɪ̯tɪŋ", "Harvard format", "ˈhäɹ.vɚd ˈfɔɹ.mæt", "Han script", "ˈxän sɹiːpt|ˈxan ˈsɹɪpt", "mademoiselle", "ˌmæd.mwä.ˈzel", "spectroscopical", "spɛk.tɹəˈskäp.ɪ.kəl"],
  "ǝ": ["visualize", "ˈvɪʒ.wǝ.laɪz", "American Indian", "əˈmɛɹɪkən ˈɪndiǝn", "derailment", "dɪˈɹeɪ̯l.mǝnt", "Jordan curve", "ˈʤoɹ.dǝn ˈkɝv", "escapism", "ˈɛskǝpˌɪzm", "Egyptian blue", "ɪˈʤɪp.ʃǝn bluː", "correlative", "kəˈɹɛlǝtɪv", "Semarang", "sǝˈmɑɹaŋ", "cancelation", "ˈkænt.sə.leɪʃǝn", "herb Robert", "ɜrb ˈɹɑbǝɹt"],
  "ʔ": ["light", "ɫɐɪ̯ʔ", "slight", "sl̥ɐɪʔ", "app", "ʔæʔp̚", "app", "ʔæʔp̚", "leastest", "ˈliʔɪst", "fountain", "ˈfaʊn.ʔn̩", "RF", "ˈɑɹʔˈɛf", "Satan", "ˈseɪ.ʔn̩", "brighten", "ˈbɹaɪ̯ʔn̩", "British Empire", "brɪʔɪʃ ˈɛm.paɪə"],
  "ɘ": ["colour", "ˈkhʌ.lɘ", "rifled", "ˈɹaɪ.fɘld", "anodize", "ˈæn.ɘ.daɪz", "smokeless", "ˈsmʊklɘs", "plutocrat", "ˈpluː.tɘ.kræt", "bottlenose", "ˈbɑt.ɘl.noʊz", "fervour", "ˈfəːvɘ", "field of vision", "ˈfild ɘv ˈvɪʒn", "retroperitoneal", "ɹɛtɹoʊˌpɛɹɪ.tɘˈniː.əl", "autoerotic", "ˈɔːtəʊ.ɘɹɒt.ɪk"],
  "ʍ": ["when", "ʍɛn", "what", "ʍʌt", "where", "ʍɛɚ", "while", "ʍaɪl", "why", "ʍaɪ", "wheel", "ʍil", "elsewhere", "ˈɛlsˌʍɛɹ", "overwhelm", "ˌoʊvɚˈʍɛlm", "whereabouts", "ˌʍɛəɹəˈbaʊts", "whisker", "ʍɪskə"],
  "̪": ["thread", "θɾ̪̊ɛd", "thrill", "θɾ̪̊ɪɫ", "Gujarat", "ɡʊ.dʒə.ˈɾaːt̪", "thrice", "θɾ̪̊äɪs", "Sindh", "sɪnd̪ɦ", "Hindustan", "hɪnd̪ʊˈst̪ɑn", "Hindustan", "hɪnd̪ʊˈst̪ɑn", "Aditya", "ˈə.d̪ɪ.t̪yɑ", "Aditya", "ˈə.d̪ɪ.t̪yɑ", "Mahatma Gandhi", "ˈmahatma ˈgan̪dʱi"],
  "œ": ["President Taylor", "ˈprɛzɪdɨnt ˈteɪlœr", "mesoderm", "ˈmiːzoʊ.dœɹm", "Meuse", "ˈmœz", "Gauguin", "goʊˈgœːŋ", "Eure", "ˈœʁ", "Husserl", "ˈhæsœl", "Goering", "ˈɡœɹɪŋ", "Harmsworth", "ˈhɑɹms.wœɹθ", "Seurat", "ˈsœ.ʁa", "Koln", "ˈkœln"],
  "ʑ": ["Shinjuku", "ɕindʑukɯ", "Gifu", "d͡ʑi.ɸu", "Jeju", "dʑɛ.dzu", "Japanese style", "d͡ʑæpˈænˌiːz ‿ staɪ̯l", "Meiji period", "ˈmeɪ.dʑi piː.ɹiː.od", "Japanese restaurant", "d͡ʑæp​.æˈniz ɹɪsˈtɒɹ.ɑːnt", "Gwangju", "k͡ʍaŋ.d͡ʑu", "Japanese garden", "dʑæpənˈiːz gɑːɹdn", "Tsukiji", "ˌt͡suːˈkiː.d͡ʑiː", "Awaji", "ˈaː.wa.d͡ʑi"],
  "ø": ["Montmartre", "mɔ̃.maʁtʁø", "Villeneuve", "vilˈnøːv", "dominos", "ˈdømɨ.noʊz", "Monsieur", "ˈmɑ̃sjø", "monsieur", "mə̃ː.sjø", "Lom", "ˈløːm", "aneuploidy", "æ.njuˈpløɪ.di", "Depardieu", "dɛːˈpaʁdjø", "Reuss", "ɹøːs", "banlieue", "bɑ̃.ljø"],
  "ḷ": ["biologically", "bɑɪ.əˈlɑdʒ.ɪ.kḷi", "crumpled", "ˈkɹʌmpḷd", "National Science Foundation", "ˈnæʃ.ən.ḷ ˈsaɪ.əns ˈfaʊn.deɪ.ʃən", "dismantled", "dɪsˈmæntḷd", "Doctor Google", "ˈdɑkɹ̩ ˈguːgḷ", "terrifically", "təˈɹɪfɪkḷi", "inaudibly", "ɪnˈɔːdəbḷi", "idiomatically", "ɪdɪəˈmætɪkḷi", "stablemaster", "ˈsteɪ.bḷ̩mæs.təɹ", "metonymically", "mɛtəˈnɪmɪkḷi"],
  "∫": ["UHF", "ˈju.ˌɛɪ.ˌɛt∫", "VHS", "vi ˈeɪt∫ ˈɛs", "English version", "ɪŋ.ɡlɪʃ ˈvːɹ̩∫ən", "Los Islands", "ˈlɒs ˈaɪ.lənd∫", "HBV", "eɪt∫ˈbiːˈviː", "Channel Island", "ˈt∫æn.əl ˈaɪ̯.lənd", "German and French", "dʒəɹmən ænd fɹɛnt∫", "disproportionally", "dɪs.pɹoʊˈpɔːɹ∫ənəl.li", "rapaciousness", "ɹæpˈeɪ∫əsnəs", "poststructural", "poʊstˈstɹʌkt∫ʊɹəl"],
  "ʳ": ["floored", "flɔːʳd", "Bachelor of Science", "ˈbætʃələʳ əv ˈsaɪəns", "spyware", "ˈspaɪ.weəʳ", "Labour and Welfare", "ˈleɪbəʳ ænd ˈwelfɛəʳ", "Labour and Welfare", "ˈleɪbəʳ ænd ˈwelfɛəʳ", "forcedly", "ˈfɔːʳ.sɪd.li", "gearset", "ˈɡɪʳ.sɛt", "semidiameter", "ˌsɛːmɪˈdaɪ.əmɪtəʳ", "sartor", "ˈsɑːʳtər", "narcissistically", "nɑːʳˈsɪs.tɪk.li"],
  "ɲ": ["California coffee", "kælɪˈfoɹ.ɲɨ kɒfi", "ligne", "ˈlɪŋ.ɲɑ", "Bretagne", "bʁə.tɑɲ", "Emilia-Romagna", "ɛˈmiː.li‿a‿roˈmaɲɲa", "Emilia-Romagna", "ɛˈmiː.li‿a‿roˈmaɲɲa", "Espana", "ɛˈspaɲa", "Jinja", "ˈdʑiɲ.dʑa", "intergranular", "ɪn.tɚˈɡɹæɲ.jə.ləɹ", "Sardegna", "sɐrˈdeɲɲa", "Sardegna", "sɐrˈdeɲɲa"],
  "̈": ["attribute", "ˈæt.ɹɪ̈ˌbjut", "United States", "juˌnaɪtɪ̈d ˈsteɪts", "priority", "pɹaɪˈɔɹɪ̈ti", "orange", "ˈɑɹ.ɪ̈nd͡ʒ", "capitalism", "ˈkæpɪ̈tl̩ˌɪzm̩", "hood", "hʊ̈d", "spotted", "ˈspɑtɪ̈d", "pesticide", "ˈpɛstɪ̈ˌsaɪd", "prophecy", "ˈpɹɑfɪ̈si", "unequivocal", "ˌʌnɪ̈ˈkwɪvəkəl"],
  "̥": ["slight", "sl̥ɐɪʔ", "tomato", "thə̥ˈmeɪɾoʊ", "culprit", "ˈkhʌɫpɹ̥ɪt", "cryptography", "kɹ̥ɪpˈthɒɡɹəfiː", "Hiratsuka", "hiɾɑˈtsɯ̥kɑ", "tsuki", "ˈt͡sɯ̥ki", "sternotomy", "stɹ̥ˈnɒt.əm.i", "moted", "ˈmoʊ̯.tɪd̥", "ochazuke", "o̞t͡ɕad͡zɯ̥ke̞", "zenzai", "d̥ʑeɲ.d̥ʑai̯"],
  "ō": ["Tokyo Stock Exchange", "ˈtō.keɪ.ō ˈstɑːk ɪksˈt͡ʃeɪndʒ", "Tokyo Stock Exchange", "ˈtō.keɪ.ō ˈstɑːk ɪksˈt͡ʃeɪndʒ", "Robespierre", "ˈɹō.bɛs.pɪəɹ", "monosilane", "ˌmänōˈsīˌlān", "iodoacetamide", "aɪˌō.doʊ.æ.sɛ.tæ.ˈmaɪd", "hemodiafiltration", "ˌhī.mō.dī.ə.fɨl'trā.shən", "coherer", "kōˈhē.ər", "subsolar", "sʌbˈsōlə", "photoperiodism", "ˈfoʊ.tōʊ.pɪə.ɹi.ˌdɪz.əm", "apotemnophobia", "ˌæpəˌtɛmnəˈfōbiə"],
  "ɻ": ["structural", "'stɻʌk.tʃhə.ɹəl", "unilateral", "ˌjʉː.nəˈɫæɾ.ɚ.ɻɫ̩", "scorched", "skɔɻtʃt", "cartoonish", "kɑːɻˈtuːn.iʃ", "Charlestown", "ˈtʃɑɻlz.tɑʊn", "Argos", "ˈɑɻɡoʊs", "Kartikeya", "ˈkəːɻ.tɪ.keɪ.ə", "pentacarbonyl", "pɛn.təˈkaɻ.bɔn.iːl", "secretarybird", "ˈsɛkɹɪtɛɻ.bɝd", "veratrine", "ˈvɛɹətɻiːn"],
  "ʲ": ["Kyoto man", "ˈkʲoːto ˈman", "Vladimir Lenin", "ˈvlɑ.dʲi.mʲɪr ˈlʲe.nʲɪn", "Vladimir Lenin", "ˈvlɑ.dʲi.mʲɪr ˈlʲe.nʲɪn", "Vladimir Lenin", "ˈvlɑ.dʲi.mʲɪr ˈlʲe.nʲɪn", "Vladimir Lenin", "ˈvlɑ.dʲi.mʲɪr ˈlʲe.nʲɪn", "Akatsuki", "ɐkɑtˈsɯkʲi", "Gyeonggi-do", "kʲʰʌŋɡi.do", "Vitebsk", "ˈvʲi.tʲebsk", "Vitebsk", "ˈvʲi.tʲebsk", "Mikio", "mʲikio"],
  "ç": ["Heinrich", "ˈhaɪnɹɪç", "2H", "tu'eɪç", "Dietrich", "ˈdiː.tɹɪç", "French sole", "ˈfɹɛnç soʊl", "Schleswig-Holstein", "ˈʃlɛs.vɪç ˈhol.ʃtaɪ̯n", "Reichstag", "ˈɹaɪçˌstɑːk", "Munich beer", "mjuːnɪç bɪə", "Ludwig van Beethoven", "ˈluːtvɪç fan ˈbeː.thoː.vən", "Lichtenstein", "ˈlɪç.tɛn.ʃtaɪn", "Ehrlich", "ˈɛɹ.lɪç"],
  "̞": ["Ehime", "eˈhiːme̞", "Dentsu", "de̞n.tsɯ", "Ney", "ne̞j", "tokusatsu", "ˈto̞.ku.sat.su", "Otsu", "o̞.tsu", "satoyama", "sɑto̞jɑmɑ", "Nijinsky", "ˈnɪʤ.ɪ̞n.ski", "amazake", "ˈɑːmɑˈzɑːke̞", "mozuku", "mo̞ˈzukɯ", "omurice", "o̞.mu.ɾi.tɕe̞"],
  "ɦ": ["uh-huh", "ʌ˨ˈɦʌ˦", "sledgehammer", "ˈslɛdʒɦɑm.ɚ", "Sindh", "sɪnd̪ɦ", "Akihabara", "ɑkiˈɦɑbɑɾɑ", "Hindustani", "ɦɪndʊˈstɑːni", "Maha", "ˈmaɦa", "Den Haag", "dɑn ˈɦɑːɡ", "Narasimha", "ˈnaːrɑsɪmɦɑ", "Hallstatt", "ˈɦaːlstatˌ -ˈhaːlʃtatˌ -ˈhaːlˌʃtat", "Ahriman", "ˈaɦ.ɹi.mɑn"],
  "̆": ["creolization", "ˌkɹiːəʊ̆laɪˈzeɪʃə̆n", "creolization", "ˌkɹiːəʊ̆laɪˈzeɪʃə̆n", "necrophile", "ˈnɛkɹəʊ̆faɪl", "augustly", "ˈɔː.ɡə̆st.li", "engladden", "ɛnˈɡlædə̆n", "engolden", "ɛnˈɡəʊldə̆n", "enharden", "ɛnˈhɑːdə̆n", "enhearten", "ɛnˈhɑːtə̆n", "enlengthen", "ɛnˈlɛŋθə̆n", "enmilden", "ɛnˈmaɪldə̆n"],
  "ʂ": ["Prakash", "ˈprəkɑʂ", "Song dynasty", "ʰsɔŋ.tʂʰau.ˈdi.nə.stiʰ", "Chinese chess", "t͡ʂʰai̯ˈneːs ˈxɛs", "Changsha", "t͡ʃʰɑŋˈʂʰɑ", "Chang'an", "ʈʂʰɑŋˈɑŋ", "Chiayi", "ʈ͡ʂaɪ̯.i", "Wuchang", "ˌwuː.ˈʈʂʰɐŋ", "Zhenjiang", "ˌʈʂʂɛnˈtɕjaŋ", "Zhenjiang", "ˌʈʂʂɛnˈtɕjaŋ", "Jingdezhen", "ˈʈ͡ɕiŋ.ʈə.ʈʂən"],
  "ʈ": ["Maharashtra", "ˌməˈhəːɹɑːʃʈɹə", "Ministry of Education", "ˈmɪnɪs.ʈɹi ˌɒv ɛdʊ.ˈkeɪ.ʃən", "Maratha", "mə.ˈɾaː.ʈhə", "Chang'an", "ʈʂʰɑŋˈɑŋ", "Chiayi", "ʈ͡ʂaɪ̯.i", "Thapa", "ˈʈʰapa", "insupportable", "ɪnsʌˈpɔɹʈə.bəl", "Wuchang", "ˌwuː.ˈʈʂʰɐŋ", "Zhenjiang", "ˌʈʂʂɛnˈtɕjaŋ", "Jingdezhen", "ˈʈ͡ɕiŋ.ʈə.ʈʂən"],
  "̮": ["National Security Council", "ˈnæʃənəl ˌsɪkjʊˈɹɪt̮i ˈkaʊnsl", "infragravity", "ɪnfɹəˈɡɹævɪt̮i", "antiballet", "ænt̮ibæˈlɛt", "chartable", "ˈʧɑːrt̮ə.bəl", "cupboardless", "ˈkəbəd̮les", "excitably", "ɛkˈsaɪt̮ə.bli", "hoydenishness", "ˈhɔɪd̮ən.ɪʃ.nəs", "mettlesomely", "ˈmɛt̮.ə.səm.li", "nanocrystallinity", "noʊˌkɹɪstəˈlɪnɪt̮i", "nonreactivity", "ˌnɑn.ɹiˌækˈtɪv.ɪ.t̮i"],
  "ʷ": ["Susquehanna River", "sʌskʷɨhænə ɹɪvəɹ", "aequorin", "ɛˈkʷoʊ.ɹn", "kwacha", "ˈkʷɑːt͡ʃə", "quasispecies", "ˈkʷeɪzɪˌspiːʃiːz", "quasilinear", "kʷæˌsɪˈlɪ.nə.ɹ", "quaternionic", "kʷɑːtərˈoʊn.ɪk", "quadricolor", "kʷæ.dɹɨˈkʌ.lər", "Quinquagesima", "kʷɪŋ.kʷɑdʒ.ˈi.sə.mə", "Quinquagesima", "kʷɪŋ.kʷɑdʒ.ˈi.sə.mə", "liquefacient", "lɪkʷɪˈfæs.ənt"],
  "ʏ": ["Rasmussen", "ˈɹæːs.mʏːs.ən", "Deutschland", "ˈdɔʏtʃ.lant", "Münster", "ˈmʏnstɐ", "Saarbrücken", "ˈzaːɐ̯ˈbrʏkɛn", "Deutsche Mark", "ˈdɔʏ.t͡ʃə ˈmaʁk", "Hilversum", "hɪlˈvɛr.sʏm", "bioadhesive", "baʏ.oʊˈædhɪˌsiːv", "Nurnberg", "ˈnʏʁnbɛʁk", "Deutschmark", "ˈdɔʏtʃˌmɑːk", "Hakenkreuz", "ˈhɑː.kən.ˌkroʏts"],
  "ɸ": ["Fukuoka", "ɸukɯoka", "Gifu", "d͡ʑi.ɸu", "futanari", "ˈɸɯ̹.ˈta.na.ɾi", "Fukushima Prefecture", "pɹɪˈfɛk.t͡ʃɹ̩ ɸɯkɯˈɕːima", "pfft", "p͡ɸːt", "fusuma", "ˈɸɯ.sɯ.mɑ", "futomaki", "ˈɸuː.to.ma.kiː", "fudegaki", "ˈɸuː.de.ɡa.ki", "fukugi", "ɸʊˈkʊɡiː", "hikifuda", "hikiˈɸuda"],
  "́": ["thioether", "ˈθaɪoʊ̯éːtʰɚ", "Fer", "ˈfɛ́ːr", "Muong", "ˈmwɔ́ʊ̯ŋ", "autopista", "ɑʊ.təˈpís.tɑ", "repetiteur", "ɹɛ́pətɪˈtɝ", "bioentrepreneur", "baɪ.oʊntɹ̩praˈnɝ́ːɹ̩", "blownup", "ˈbloʊnɑ́p", "electrorefine", "ɪˈlɛk.tɹo.ɹə́.fajn", "immunobuffer", "ɪ́m.ju.noʊ.ˈbʌf.ɚ", "qieyun", "kʰjɛ̀ jýn"],
  "ʱ": ["Madhya Pradesh", "mə͜ʱdʱjə pɾəˈdeːʃ", "Madhya Pradesh", "mə͜ʱdʱjə pɾəˈdeːʃ", "Bharatiya", "bʱɑːr.tɪ.jə", "Mahatma Gandhi", "ˈmahatma ˈgan̪dʱi", "Hefei", "ˈxʱə.ˌfei", "Bhubaneswar", "ˌbʱuːbəˈneːswɑːr", "Bahasa Indonesia", "bʱa.ha.sa ɪn.do.nɛ.si.a", "Bharata", "bʱɑˈɹɑːt.ɑ", "Phalguna", "ˈpʰʌl.ɡʱə.na", "dharmapala", "dʱɑːrmɐˈpɑːlɐ"],
  "ī": ["Lijiang", "lǐjīāng", "monosilane", "ˌmänōˈsīˌlān", "hemodiafiltration", "ˌhī.mō.dī.ə.fɨl'trā.shən", "hemodiafiltration", "ˌhī.mō.dī.ə.fɨl'trā.shən", "commelina", "ˈkäməˌlīnə", "autohybridize", "ȯ.tö.hī.ˈbrɪ-dīz", "autohybridize", "ȯ.tö.hī.ˈbrɪ-dīz", "bistroic", "ˈbīsˌtrōik", "compiland", "ˈkäm.pī.lænd", "picrylhydrazyl", "pɪˈkɹɪlˈhaɪ̯dɹə.zīl"],
  "ɣ": ["Zaragoza", "θæɹ.əˈɣoʊ.zə", "Borges", "ˈboɹ.ɣe.s", "Antofagasta", "ɑŋ.tu.fɑ.ˈɣɑs.tɑ", "Gelderland", "ˈɣɛl.dɛr.lɑnt", "Ghaziabad", "ɣɑːziˈɑːbɑːd", "augite", "oʊˈɣaɪt", "reagin", "ˈɹiː.ɣən", "groenendael", "ˈɣɹuen̩ˌdɑl", "forlagen", "fɔːˈlːɑːɣʰɛn```", "phytodemographic", "faɪ̯toʊ.dɨˈmoɣɹæf.ɨk"],
  "ʎ": ["Liaoning", "ˈʎaʊnɪŋ", "Vuelta", "ˈbweʎ.ta", "Villarreal", "viˈʎe.ɾɛ.al", "William IV", "ˈwɪʎəm ˈði ˈfoɹθ", "Vallarta", "vaˈʎaɾta", "Puglia", "ˈpuʎːa", "Modigliani", "modɨʎˈjʌ.ni", "Villahermosa", "vi.ʎæˈɛɾ.mosa", "filhos", "ˈfiʎos", "bersagliere", "ˌbær.sæʎˈjɛ.re"],
};
function show_hint() {
  document.getElementById("quiz_answer").focus();
  const question = questions[quiz_index];
  const core_pron = question[0];
  const core_word = question[1];
  const hint_div = document.getElementById("hint");
  hint_div.innerHTML = "";
  hint_div.style.display = "block";
  const table = document.createElement("table");
  hint_div.appendChild(table);
  for (let symbol of core_pron.split("")) {
    const label = symbol_labels[symbol];
    if (!label) {
      label = "-";
      if (!examples) continue;
    }
    const examples = symbol_examples[symbol];
    const row = document.createElement("tr");
    table.appendChild(row);
    const col_symbol = document.createElement("td");
    row.appendChild(col_symbol);
    col_symbol.className = "pron";
    col_symbol.appendChild(document.createTextNode("/"));
    const b_symbol = document.createElement("b");
    col_symbol.appendChild(b_symbol);
    b_symbol.textContent = symbol;
    col_symbol.appendChild(document.createTextNode("/"));
    const col_label = document.createElement("td");
    row.appendChild(col_label);
    col_label.className = "label";
    col_label.textContent = label;
    if (examples) {
      const col_examples = document.createElement("td");
      row.appendChild(col_examples);
      col_examples.className = "examples";
      let index = 0
      const shown_list = [];
      while (index < examples.length) {
        const word = examples[index];
        const pron = examples[index + 1];
        if (word != core_word) {
          shown_list.push([word, pron]);
        }
        index += 2;
      }
      shuffle(shown_list);
      index = 0;
      while (index < shown_list.length && index < 3) {
        const word = shown_list[index][0];
        const pron = shown_list[index][1];
        if (index > 0) {
          col_examples.appendChild(document.createTextNode(", "));
        }
        const span_word = document.createElement("span");
        col_examples.appendChild(span_word);
        span_word.textContent = word;
        span_word.onclick = function() { voice_text(word, quiz_locale); };
        col_examples.appendChild(document.createTextNode(" "));
        const span_pron = document.createElement("span");
        col_examples.appendChild(span_pron);
        span_pron.appendChild(document.createTextNode("/"));
        const b_pron = document.createElement("b");
        span_pron.appendChild(b_pron);
        span_pron.onclick = function() { voice_text(word, quiz_locale); };
        b_pron.textContent = pron;
        span_pron.appendChild(document.createTextNode("/"));
        index++;
      }
    }
  }
}
function shuffle(array) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
}
function set_message(text, color) {
  const message_p = document.getElementById("message");
  const message_text = message_p.childNodes[0];
  message_text.textContent = "";
  message_p.display = "none";
  if (text.length > 0) {
    message_text.textContent = text;
    message_p.style.color = color;
    message_p.style.display = "block";
  }
}
let self_url = location.href;
self_url = self_url.replace(/#.*/, "");
self_url = self_url.replace(/\?.*/, "");
let user_name = "";
let questions = null;
let answers = null;
let quiz_level = 0;
let quiz_locale = "";
let quiz_index = 0;
let quiz_time = 0;
function start_quiz() {
  user_name = document.getElementById("intro_name").value.trim();
  quiz_level = parseInt(document.getElementById("intro_level").value)
  quiz_locale = document.getElementById("intro_locale").value.trim();
  quiz_index = 0;
  set_message("");
  if (user_name.length < 1) {
    set_message("ユーザ名を入力してください。", "#ff1100");
    return;
  }
  let gen_url = self_url + "?gen=" + quiz_level + "&loc=" + quiz_locale;
  let xhr = new XMLHttpRequest();
  xhr.onload = function() {
    if (this.status == 200) {
      const data = JSON.parse(this.responseText);
      questions = data["questions"];
      answers = [];
      render_quiz();
    } else {
      set_message("問題取得エラー:" + this.status, "#ff1100");
    }
  }
  xhr.open("GET", gen_url, true);
  xhr.send();
}
function render_quiz() {
  const question = questions[quiz_index];
  const intro = document.getElementById("intro");
  intro.style.display = "none";
  const quiz = document.getElementById("quiz");
  quiz.style.display = "block";
  const quiz_num = document.getElementById("quiz_num").childNodes[0];
  quiz_num.textContent = "Q" + (quiz_index + 1);
  const quiz_pron = document.getElementById("quiz_pron").childNodes[0];
  quiz_pron.textContent = question[0];
  quiz_time = new Date().getTime();
  set_message("", "");
  document.getElementById("quiz_answer").value = "";
  document.getElementById("quiz_answer").focus();
  document.getElementById("hint").style.display = "none";
}
function answer_quiz() {
  const time = new Date().getTime() - quiz_time;
  const question = questions[quiz_index];
  const quiz_answer = document.getElementById("quiz_answer").value.trim().toLowerCase();
  if (quiz_answer.length < 1) {
    set_message("英単語を入力してください。", "#ff1100");
    return;
  }
  let match_item = null;
  for (let item of question[1]) {
    if (item[0].toLowerCase() == quiz_answer) {
      match_item = item;
      break;
    }
  }
  if (!match_item) {
    set_message("不正解: " + quiz_answer, "#ff1100");
    return;
  }
  set_message("正解: " + match_item[0] + " (" + match_item[1] + ")", "#009911");
  const answer = [question[0], match_item[0], match_item[1], time];
  answers.push(answer);
  voice_text(match_item[0], quiz_locale);
  quiz_index++;
  if (quiz_index < questions.length) {
    setTimeout(render_quiz, 1000);
  } else {
    setTimeout(finish_quiz, 1000);
  }
}
function stop_quiz() {
  const pron = questions[quiz_index][0];
  const item = questions[quiz_index][1][0];
  set_message("/" + pron + "/ の解答例は...: " + item[0] + " (" + item[1] + ")", "#bb3300");
  voice_text(item[0], quiz_locale);
  document.getElementById("quiz").style.display = "none";
}
function voice_text(text, locale) {
  if (!SpeechSynthesisUtterance) return;
  window.speechSynthesis.cancel();
  let utter = new SpeechSynthesisUtterance(text);
  if (locale == "gb") {
    utter.lang = "en-GB";
  }  else {
    utter.lang = "en-US";
  }
  window.speechSynthesis.speak(utter);
}
function finish_quiz() {
  const save_data = {
    "user": user_name,
    "level": quiz_level,
    "locale": quiz_locale,
    "answers": answers,
  };
  let xhr = new XMLHttpRequest();
  xhr.onreadystatechange = function() {
    if (this.readyState != 4) return;
    document.getElementById("quiz").style.display = "none";
    if (this.status == 200) {
      set_message("全問終了。お連れ様でした。", "#009911");
      const result_url = self_url + "?z=" + this.responseText;
      document.getElementById("result_link").href = result_url;
      document.getElementById("result").style.display = "block";
    } else {
      set_message("保存エラー:" + this.status, "#ff1100");
    }
  }
  xhr.open("POST", self_url);
  xhr.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
  xhr.send("save=" + encodeURIComponent(JSON.stringify(save_data)));
}
]]></script>
</head>
<body>
<article>
"""
QUIZ_HTML_BODY = """<h1><a href="{}">英単語発音記号検定</a></h1>
<div id="intro">
<p>IPA発音記号の読解量を測る検定です。発音記号を見て、それに該当する英単語を当てるクイズに答えてください。</p>
<p>例えば <code>/ˈæ.nɪ.meɪ.tə.bəl/</code> と表示されたら、「<code>animatable</code>」と入力してください。「回答」ボタンを押すか、Enterキーを押すと回答が送信されます。</p>
<p>10問の問題が出題されます。全てに正答するまでの経過時間が60秒以内なら合格です。</p>
<p>レベルは1から5まであります。基本的な発音記号を覚えるにはレベル1だけやれば十分です。</p>
<p>答えは必ず単語です。複数語のフレーズは含まれません。大文字と小文字は区別しません。同じ発音の語が複数該当する場合、どれを入力しても正解になります。正解すると、その語が読み上げられます。</p>
<p>「手掛」ボタンを押すと、それぞれの発音記号の名前と他の単語の使用例が表示されます。それらの単語か発音記号をクリックするとその語が読み上げられます。「降参」ボタンを押すと、正答が見られますが、検定はそこで終了になります。</p>
<form id="start_form" onsubmit="start_quiz(); return false">
あなたの名前:<input type="text" id="intro_name" size="16" value=""/>
<select id="intro_level">
<option value="1">レベル1: 中学生並</option>
<option value="2">レベル2: 高校生並</option>
<option value="3">レベル3: 大学生並</option>
<option value="4">レベル4: 留学生並</option>
<option value="5">レベル5: ネイティブ並</option>
</select>
<select id="intro_locale">
<option value="us">アメリカ式</option>
<option value="gb">イギリス式</option>
</select>
<button type="button" onclick="start_quiz()">クイズを始める</button>
</form>
</div>
<div id="quiz">
<form id="quiz_form" onsubmit="answer_quiz(); return false">
<h2 id="quiz_num">_</h2>
<div id="pron_line">/<code id="quiz_pron">_</code>/</div>
<div id="answerline">
<input type="text" id="quiz_answer" size="24" value=""/>
<input type="submit" id="quiz_submit" value="回答"/>
<button type="button" id="quiz_hint" onclick="show_hint()">手掛</button>
<button type="button" id="quiz_stop" onclick="stop_quiz()">降参</button>
</div>
</form>
</div>
<div id="hint">_</div>
<p id="message">_</p>
<p id="result"><a id="result_link">⇨ 結果を見る</a></p>
"""

QUIZ_HTML_FOOTER = """</article>
</body>
</html>
"""


def ReadQuestions(level, locale):
  min_rank = int(((level - 1) ** LEVEL_GAMMA) * LEVEL_BASE_RANGE)
  max_rank = int((level ** LEVEL_GAMMA) * LEVEL_BASE_RANGE)
  indices = []
  uniq_indices = set()
  while len(indices) < NUM_QUESTIONS:
    index = random.randrange(min_rank, max_rank)
    if index not in uniq_indices:
      indices.append(index)
  us_word_dict = collections.defaultdict(list)
  gb_word_dict = collections.defaultdict(list)
  if PRON_USGB_PATH:
    num_lines = 0
    with open(PRON_USGB_PATH) as input_file:
      for line in input_file:
        if len(us_word_dict) >= MAX_RECORDS: break
        fields = line.strip().split("\t")
        if len(fields) != 3: continue
        word, pron_us, pron_gb = fields
        if not regex.search(r"^[A-Za-z][a-z]+$", word): continue
        us_word_dict[word].append(pron_us)
        gb_word_dict[word].append(pron_gb)
  pron_dict = collections.defaultdict(list)
  norm_pron_dict = collections.defaultdict(list)
  pron_list = []
  word_trans_dict = {}
  with open(PRON_TABLE_PATH) as input_file:
    for line in input_file:
      if len(pron_list) >= MAX_RECORDS: break
      fields = line.strip().split("\t")
      if len(fields) != 3: continue
      word, pron, trans = fields
      if not regex.search(r"^[A-Za-z][a-z]+$", word): continue
      if word in STOP_WORDS: continue
      if locale == "gb":
        pron_gbs = gb_word_dict.get(word)
        if pron_gbs:
          for pron_gb in pron_gbs:
            if pron_gb != pron:
              us_word_dict[word].append(pron)
              pron = pron_gb
              break
      pron_dict[pron].append((word, trans))
      norm_pron1 = regex.sub(r"\(.*\)", r"", pron)
      norm_pron1 = regex.sub(r"[.ˈˌ‿]", r"", norm_pron1)
      if norm_pron1 != pron:
        norm_pron_dict[norm_pron1].append((word, trans))
      norm_pron2 = regex.sub(r"\((.*)\)", r"\1", pron)
      norm_pron2 = regex.sub(r"[.ˈˌ‿]", r"", norm_pron2)
      if norm_pron2 != pron and norm_pron2 != norm_pron1:
        norm_pron_dict[norm_pron2].append((word, trans))
      pron_list.append(pron)
      word_trans_dict[word] = trans
  prons = []
  uniq_prons = set()
  for index in indices:
    pron = pron_list[index]
    if pron in uniq_prons:
      pron = pron_list[index + 1]
    prons.append(pron)
    uniq_prons.add(pron)
  if PRON_AUX_PATH:
    with open(PRON_AUX_PATH) as input_file:
      num_lines = 0
      for line in input_file:
        if num_lines >= MAX_RECORDS: break
        fields = line.strip().split("\t")
        if len(fields) != 2: continue
        word, pron = fields
        trans = word_trans_dict.get(word)
        if trans:
          norm_pron1 = regex.sub(r"\(.*\)", r"", pron)
          norm_pron1 = regex.sub(r"[.ˈˌ‿]", r"", norm_pron1)
          norm_pron2 = regex.sub(r"\((.*)\)", r"\1", pron)
          norm_pron2 = regex.sub(r"[.ˈˌ‿]", r"", norm_pron2)
          for tmp_pron in [pron, norm_pron1, norm_pron2]:
            if tmp_pron in uniq_prons:
              norm_pron_dict[tmp_pron].append((word, trans))
        num_lines += 1
  for sub_dict in [us_word_dict, gb_word_dict]:
    for word, sub_prons in sub_dict.items():
      trans = word_trans_dict.get(word)
      if trans:
        for pron in sub_prons:
          norm_pron1 = regex.sub(r"\(.*\)", r"", pron)
          norm_pron1 = regex.sub(r"[.ˈˌ‿]", r"", norm_pron1)
          norm_pron2 = regex.sub(r"\((.*)\)", r"\1", pron)
          norm_pron2 = regex.sub(r"[.ˈˌ‿]", r"", norm_pron2)
          for tmp_pron in [pron, norm_pron1, norm_pron2]:
            if tmp_pron in uniq_prons:
              norm_pron_dict[tmp_pron].append((word, trans))
  questions = []
  for pron in prons:
    recs = list(pron_dict[pron])
    norm_pron1 = regex.sub(r"\(.*\)", r"", pron)
    norm_pron1 = regex.sub(r"[.ˈˌ‿]", r"", norm_pron1)
    norm_pron2 = regex.sub(r"\((.*)\)", r"\1", pron)
    norm_pron2 = regex.sub(r"[.ˈˌ‿]", r"", norm_pron2)
    if norm_pron1 != pron:
      norm_recs = pron_dict.get(norm_pron1)
      if norm_recs:
        recs.extend(norm_recs)
      norm_recs = norm_pron_dict.get(norm_pron1)
      if norm_recs:
        recs.extend(norm_recs)
    if norm_pron2 != pron:
      norm_recs = pron_dict.get(norm_pron2)
      if norm_recs:
        recs.extend(norm_recs)
      norm_recs = norm_pron_dict.get(norm_pron2)
      if norm_recs:
        recs.extend(norm_recs)
    uniq_recs = []
    uniq_words = set()
    for word, trans in recs:
      if word in uniq_words: continue
      uniq_words.add(word)
      uniq_recs.append((word, trans))
    questions.append((pron, uniq_recs))
  data = {
    "questions": questions,
  }
  return data
  

def SendMessage(code, message):
  print("Status: " + str(code))
  print("Content-Type: text/plain")
  print("Access-Control-Allow-Origin: *")
  print()
  print(message)


def GenerateQuestions(level, locale):
  print("Content-Type: application/json")
  print("Access-Control-Allow-Origin: *")
  print()
  level = min(5, max(1, int(level)))
  data = ReadQuestions(level, locale)
  serialized = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
  print(serialized)


def GetResultDir():
  script_name = os.environ.get("SCRIPT_NAME", sys.argv[0])
  if RESULT_DIR.startswith("/"):
    result_dir = RESULT_DIR
  else:
    document_root = os.environ.get("DOCUMENT_ROOT", "/")
    result_dir = os.path.join(document_root, os.path.dirname(script_name)[1:], RESULT_DIR)
  return result_dir


def esc(expr):
  if expr is None:
    return ""
  return html.escape(str(expr), True)


def P(*args, end="\n", file=sys.stdout):
  esc_args = []
  for arg in args[1:]:
    if isinstance(arg, str):
      arg = esc(arg)
    esc_args.append(arg)
  print(args[0].format(*esc_args), end=end, file=file)


def ShowResult(uid, script_url):
  result_dir = GetResultDir()
  tmp_path = os.path.join(result_dir, uid)
  try:
    with open(tmp_path) as tmp_file:
      serialized = tmp_file.read()
  except:
    SendMessage(404, "No such data")
    return
  try:
    result = json.loads(serialized)
  except:
    SendMessage(404, "Bad data")
    return
  user_name = result.get("user")
  unix_time = result.get("time") or 0
  level = result.get("level")
  locale = result.get("locale")
  answers = result.get("answers")
  if not user_name or not level or not locale or not answers:
    SendMessage(400, "Broken data")
  print("Content-Type: application/xhtml+xml")
  print("Cache-Control: public")
  print()
  print(RESULT_HTML_HEADER, end="")
  P('<h1><a href="{}">英単語発音記号検定</a></h1>', script_url)
  P('<p class="meta_line"><b>{}</b> 殿</p>', user_name)
  jst_time = datetime.datetime.fromtimestamp(unix_time, datetime.timezone(datetime.timedelta(hours=9)))
  jst_date = regex.sub(r"^(\d+)-(\d+)-(\d+) .*", r"\1年\2月\3日", str(jst_time)).strip()
  P('<p class="meta_line">{}</p>', jst_date)
  level_label = ["中学生並", "高校生並", "大学生並", "留学生並", "ネイティブ並"][level - 1]
  if locale == "gb":
    locale_label = "イギリス式"
  else:
    locale_label = "アメリカ式"
  P('<h2>レベル{}: {} <small>（{}）</small></h2>', level, level_label, locale_label)
  total_time = 0.0
  for answer in answers:
    total_time += answer[3]
  total_time /= 1000
  passed = total_time <= 60
  if passed:
    P('<div class="pass_label">[合格]</div>')
  else:
    P('<div class="fail_label">[不合格]</div>')
  P('<p>経過時間: <b>{:.3f}</b>秒</p>', total_time)
  P('<table>')
  P('<tr>')
  P('<th>発音</th>')
  P('<th>英単語</th>')
  P('<th>和訳</th>')
  P('<th>経過時間</th>')
  P('</tr>')
  for answer in answers:
    pron, word, trans, elapsed = answer
    P('<tr>')
    P('<td class="pron"><a onclick="voice_text(\'{}\', \'{}\')">/{}/</a></td>'.format(word, locale, pron))
    P('<td class="word"><a href="{}?q={}">{}</a></td>'.format(DICT_URL, word, word))
    P('<td class="trans">{}</td>'.format(trans))
    P('<td class="time">{:.3f}秒</td>'.format(elapsed / 1000))
    P('</tr>')
  P('</table>')
  tweet_msg = "#発音記号検定: {}さんは、レベル{}の語彙を{:.0f}秒で回答".format(
    user_name, level, total_time)
  if passed:
    tweet_msg += "したので、{}の発音記号読解能力を持つと認められます。".format(level_label)
  else:
    tweet_msg += "しましたが、規定時間に間に合いませんでした。"
  P('<p>結果をつぶやく: ', end="")
  P('<a href="https://twitter.com/share" class="twitter-share-button" data-text="{}" data-count="none">Tweet</a>', tweet_msg, end="")
  P('<script>!function(d,s,id){{var js,fjs=d.getElementsByTagName(s)[0];if(!d.getElementById(id)){{js=d.createElement(s);js.id=id;js.src="//platform.twitter.com/widgets.js";fjs.parentNode.insertBefore(js,fjs);}}}}(document,"script","twitter-wjs");</script>', end="")
  P('</p>')
  P('<p><a href="{}">⇨ 再挑戦する</a></p>', script_url)
  P(RESULT_HTML_FOOTER, end="")
  

def SaveResult(save_result):
  if len(save_result) > 8192:
    SendMessage(400, "Too long data")
    return
  try:
    data = json.loads(save_result)
  except:
    SendMessage(400, "Bad JSON")
    return
  user_name = (data.get("user") or "")[:32].strip()
  if not user_name or type(user_name) != str:
    SendMessage(400, "No name")
    return
  level = data.get("level")
  if not level or type(level) != int:
    SendMessage(400, "No level")
    return
  answers = data.get("answers")
  if not answers or type(answers) != list:
    SendMessage(400, "No answers")
    return
  now = time.time()
  data["time"] = int(now)
  uid = regex.sub(r"^-*0x", "", hex(hash(save_result)))
  result_dir = GetResultDir()  
  serealized = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
  tmp_path = os.path.join(result_dir, uid)
  with open(tmp_path, "w") as tmp_file:
    print(serealized, file=tmp_file)
  SendMessage(200, uid)


def GenerateUserInterface(script_url):
  print("Content-Type: application/xhtml+xml")
  print("Cache-Control: no-cache")
  print()
  print(QUIZ_HTML_HEADER, end="")
  print(QUIZ_HTML_BODY.format(script_url, script_url), end="")
  print(QUIZ_HTML_FOOTER, end="")

  
def main():
  http_scheme = os.environ.get("REQUEST_SCHEME", "http")
  host_name = os.environ.get("HTTP_HOST", "localhost")
  script_name = os.environ.get("SCRIPT_NAME", sys.argv[0])
  script_url = http_scheme + "://" + regex.sub(r"/+(\./+)?", "/", host_name + "/" + script_name)
  if RESULT_DIR.startswith("/"):
    result_dir = RESULT_DIR
  else:
    document_root = os.environ.get("DOCUMENT_ROOT", "/")
    result_dir = os.path.join(document_root, os.path.dirname(script_name)[1:], RESULT_DIR)
  params = {}
  form = cgi.FieldStorage()
  for key in form.keys():
    value = form[key]
    if isinstance(value, list):
      params[key] = value[0].value
    else:
      params[key] = value.value
  generate_level = regex.sub(r"[^0-9]", "", params.get("gen") or "")[:2]
  locale = regex.sub(r"[^a-z]", "", params.get("loc") or "")[:2]
  show_id = regex.sub("r[a-z0-9]", "", (params.get("z") or "")[:64])
  save_result = params.get("save") or ""
  if generate_level:
    GenerateQuestions(generate_level, locale)
  elif show_id:
    ShowResult(show_id, script_url)
  elif save_result:
    SaveResult(save_result)
  else:
    GenerateUserInterface(script_url)


if __name__=="__main__":
  main()
