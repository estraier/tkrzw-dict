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
#quiz_stop { font-size: 110%; }
#result { display: none; }
#result_link { font-size: 110%; color: #001199; }
]]></style>
<script type="text/javascript"><![CDATA[
"use strict";
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
  console.log(gen_url);
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
<p>レベルは1から5まであります。高いレベルでは「communicable」「paralyzed」「kabuki」「Beethoven」のような派生後や外来語や固有名詞も含むので柔軟な思考と広い知識が求められます。</p>
<p>答えは必ず単語です。複数語のフレーズは含まれません。大文字と小文字は区別しません。同じ発音の語が複数該当する場合、どれを入力しても正解になります。正解すると、その語が読み上げられます。</p>
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
<button type="button" id="quiz_stop" value="降参" onclick="stop_quiz()">降参</button>
</div>
</form>
</div>
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
      norm_pron1 = regex.sub(r"[.ˌ]", r"", norm_pron1)
      if norm_pron1 != pron:
        norm_pron_dict[norm_pron1].append((word, trans))
      norm_pron2 = regex.sub(r"\((.*)\)", r"\1", pron)
      norm_pron2 = regex.sub(r"[.ˌ]", r"", norm_pron2)
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
          norm_pron1 = regex.sub(r"[.ˌ]", r"", norm_pron1)
          norm_pron2 = regex.sub(r"\((.*)\)", r"\1", pron)
          norm_pron2 = regex.sub(r"[.ˌ]", r"", norm_pron2)
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
          norm_pron1 = regex.sub(r"[.ˌ]", r"", norm_pron1)
          norm_pron2 = regex.sub(r"\((.*)\)", r"\1", pron)
          norm_pron2 = regex.sub(r"[.ˌ]", r"", norm_pron2)
          for tmp_pron in [pron, norm_pron1, norm_pron2]:
            if tmp_pron in uniq_prons:
              norm_pron_dict[tmp_pron].append((word, trans))
  questions = []
  for pron in prons:
    recs = list(pron_dict[pron])
    norm_pron1 = regex.sub(r"\(.*\)", r"", pron)
    norm_pron1 = regex.sub(r"[.ˌ]", r"", norm_pron1)
    norm_pron2 = regex.sub(r"\((.*)\)", r"\1", pron)
    norm_pron2 = regex.sub(r"[.ˌ]", r"", norm_pron2)
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
  level = result.get("level")
  locale = result.get("locale")
  answers = result.get("answers")
  if not user_name or not level or not locale or not answers:
    SendMessage(400, "Broken data")
  print("Content-Type: application/xhtml+xml")
  print("Cache-Control: public")
  print()
  print(RESULT_HTML_HEADER, end="")
  P('<h1><a href="{}">英単語発音記号クイズ</a></h1>', script_url)
  P('<p>ユーザ名: <b>{}</b></p>', user_name)
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
