#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# CGI Script to perform quiz of words to check AOA level
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
import html
import os
import math
import random
import regex
import sys
import tkrzw
import tkrzw_dict
import tkrzw_union_searcher
import urllib
import urllib.request
import zlib


AOA_RANKS_PATH = "union-aoa-ranks.tks"
DICT_URL = "https://dbmx.net/dict/search_union.cgi"
RESULT_DIR = "quiz-aoa-result"
START_RANK = 400
WINDOW_WIDTH = 2000
NUM_CANDIDATES = 5
NUM_SHOWN_TRANS = 4
NUM_QUIZES = 22
NUM_RAND_TRIES = 40


def GetRecords(aoa_dbm, core_rank):
  window_width = min(core_rank, WINDOW_WIDTH)
  start_rank = max(0, int(core_rank - window_width / 2))
  start_key = "{:05d}".format(start_rank)
  it = aoa_dbm.MakeIterator()
  it.Jump(start_key)
  records = []
  while True:
    record = it.GetStr()
    if not record: break
    records.append((int(record[0]), record[1]))
    if len(records) >= window_width: break
    it.Next()
  return records


def ParseRecord(expr):
  word, aoa, poses, trans = expr.split("\t")
  aoa = float(aoa)
  poses = poses.split(",")
  trans = trans.split(",")
  return (word, aoa, poses, trans)


def IsSimilarKanaTran(word, tran):
  kana_tran = regex.sub(r"[^\p{Katakana}]", "", tran)
  if not kana_tran:
    return False
  top_letters = []
  for token in word.split(" "):
    if token:
      top_letters.append(token[:1].lower())
  for top_letter in top_letters:
    if top_letter == "a":
      if regex.search(r"^[アエ]", kana_tran): return True
    elif top_letter == "b":
      if regex.search(r"^[バビブベボ]", kana_tran): return True
    elif top_letter == "c":
      if regex.search(r"^[カキクケコサシスセソチ]", kana_tran): return True
    elif top_letter == "d":
      if regex.search(r"^[ダヂヅデド]", kana_tran): return True
    elif top_letter == "e":
      if regex.search(r"^[アエイユヨ]", kana_tran): return True
    elif top_letter == "f":
      if regex.search(r"^[ハヒフヘホ]", kana_tran): return True
    elif top_letter == "g":
      if regex.search(r"^[ガギグゲゴジ]", kana_tran): return True
    elif top_letter == "h":
      if regex.search(r"^[ハヒフヘホア]", kana_tran): return True
    elif top_letter == "i":
      if regex.search(r"^[イア]", kana_tran): return True
    elif top_letter == "j":
      if regex.search(r"^[ジヤユヨイエ]", kana_tran): return True
    elif top_letter == "k":
      if regex.search(r"^[カキクケコ]", kana_tran): return True
    elif top_letter == "l":
      if regex.search(r"^[ラリルレロ]", kana_tran): return True
    elif top_letter == "m":
      if regex.search(r"^[マミムメモ]", kana_tran): return True
    elif top_letter == "n":
      if regex.search(r"^[ナニヌネノ]", kana_tran): return True
    elif top_letter == "o":
      if regex.search(r"^[アウオ]", kana_tran): return True
    elif top_letter == "p":
      if regex.search(r"^[パピプペポサフ]", kana_tran): return True
    elif top_letter == "q":
      if regex.search(r"^[ク]", kana_tran): return True
    elif top_letter == "r":
      if regex.search(r"^[ラリルレロ]", kana_tran): return True
    elif top_letter == "s":
      if regex.search(r"^[サシスセソ]", kana_tran): return True
    elif top_letter == "t":
      if regex.search(r"^[タチツテトデ]", kana_tran): return True
    elif top_letter == "u":
      if regex.search(r"^[ウアユ]", kana_tran): return True
    elif top_letter == "v":
      if regex.search(r"^[バビブベボヴ]", kana_tran): return True
    elif top_letter == "w":
      if regex.search(r"^[ウワヴホ]", kana_tran): return True
    elif top_letter == "x":
      if regex.search(r"^[ザジズゼゾ]", kana_tran): return True
    elif top_letter == "y":
      if regex.search(r"^[ヤユヨイエ]", kana_tran): return True
    elif top_letter == "z":
      if regex.search(r"^[ザジズゼゾ]", kana_tran): return True
  return False
  

def GetCandidates(records, used_words):
  popular_poses = ("noun", "verb", "adjective", "adverb")
  checked_words = set()
  checked_trans = set()
  candidates = []
  num_tries = 0
  top_pos = None
  while len(candidates) < NUM_CANDIDATES and num_tries < NUM_RAND_TRIES:
    num_tries += 1
    rank, expr = random.choice(records)
    word, aoa, poses, trans = ParseRecord(expr)
    if word in used_words: continue
    if len(word) <= 1: continue
    if regex.search(r"\d", word): continue
    if word.count(" ") and random.randint(0, 1): continue
    if regex.search(r"\p{Lu}", word) and random.randint(0, 1): continue
    if top_pos and top_pos in popular_poses and top_pos not in poses and random.randint(0, 2):
      continue
    if word in checked_words: continue
    checked_words.add(word)
    top_letters = []
    for token in word.split(" "):
      if token:
        top_letters.append(token[:1].lower())
    good_trans = []
    for tran in trans:
      if IsSimilarKanaTran(word, tran): continue
      if regex.search("[\p{Latin}]", tran): continue
      good_trans.append(tran)
      if len(good_trans) >= NUM_SHOWN_TRANS: break
    if not good_trans: continue
    has_dup_tran = False
    for tran in good_trans:
      if tran in checked_trans:
        has_dup_tran = True
    if has_dup_tran: continue
    for tran in good_trans:
      checked_trans.add(tran)
    if not candidates:
      top_pos = poses[0]
    candidates.append((rank, word, aoa, good_trans))
  return candidates
  

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
  user_name = (params.get("u") or "")[:64].strip()
  random_seed = int(params.get("s") or -1)
  current_rank = int(params.get("c") or -1)
  question = int(params.get("q") or -1)
  answer = int(params.get("a") or -1)
  history_expr = (params.get("h") or "")[:2048]
  result_name = regex.sub(r"[^a-f0-9]", "", (params.get("z") or "")[:16])
  history = []
  if result_name:
    result_path = os.path.join(result_dir, result_name)
    if os.path.isfile(result_path):
      with open(result_path) as input_file:
        user_name = ""
        for line in input_file:
          line = line.strip()
          if user_name:
            columns = line.split("\t")
            if len(columns) != 2: continue
            history.append((int(columns[0]), int(columns[1])))
          else:
            user_name = line
  for field in history_expr.split(","):
    columns = field.split(":")
    if len(columns) != 2: continue
    history.append((int(columns[0]), int(columns[1])))
  if random_seed >= 0:
    random.seed(random_seed + len(history))
  print("""Content-Type: application/xhtml+xml

""", end="")
  P("""<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" lang="ja">
<head>
<title>語彙力年齢診断</title>
<style type="text/css">/*<![CDATA[*/
html {{ margin: 0ex; padding: 0ex; background: #eeeeee; font-size: 12pt; color: }}
body {{ margin: 0ex; padding: 0ex; text-align: center; -webkit-text-size-adjust: 100%; }}
h1 {{ margin: 0.5ex 0; padding 0; font-size: 100%; }}
h1 a {{ color: #000000; text-decoration: none; }}
article {{ display: inline-block; width: 80ex; text-align: left; padding: 1ex 2ex;
  background: #ffffff; border: solid 1px #dddddd; border-radius: 0.5ex; }}
.goto {{ color: #001188; text-decoration: none; }}
.start_button {{ width: 10ex; }}
.theme_word {{ margin: 0ex 0.2ex; }}
.candidates_list {{ list-style: none; margin-left: 0; padding-left: 1ex; color: #333333; }}
.dunno {{ font-size: 95%; color: #666666; }}
.result_age {{ font-size: 110%; }}
.answers_list {{ list-style: none; margin-left: 0; padding-left: 1ex; color: #888888; font-size: 80%; }}
.answer_correct:before {{ color: #001188; content: "○ "; }}
.answer_incorrect:before {{ color: #881100; content: "╳︎ ️"; }}
.answer_word {{ font-size: 125%; color: #001111; text-decoration: none; }}
.answer_word:hover {{ text-decoration: underline; }}
.answer_trans,.answer_age {{font-size: 110%; color: #333333; }}
.answer_button {{ width: 20ex; margin-left: 2ex; }}
.retry:hover {{ text-decoration: underline; }}
.info {{ font-size: 80%; color: #333333; margin: 4ex 0.8ex 1ex 0.8ex; ;}}
.info a {{ text-decoration: none; }}
@media (max-device-width:720px) {{
  html {{ background: #eeeeee; font-size: 32pt; }}
  body {{ padding: 0; }}
  h1 {{ padding: 5ex 0 0 8ex; }}
  article {{ width: 100%; overflow-x: hidden; }}
  input {{ font-size: 12pt; zoom: 250%; }}
  .search_form,.entry_view,.list_view,.annot_view,.message_view,.license {{
    padding: 0.8ex 0.8ex; }}
  .attr {{ margin-left: 1ex; }}
  .item_text1 {{ margin-left: 1ex; }}
  .item_text2 {{ margin-left: 3ex; }}
  .item_text3 {{ margin-left: 5ex; }}
  .item_text4 {{ margin-left: 7ex; }}
  .item_text_n {{ font-size: 90%; }}
  .list_view {{ padding: 0.6ex 0.5ex 0.8ex 0.8ex; }}
  .annot_view .text {{ margin: 0.3ex 0.2ex; font-size: 95%; }}
  .word .tip {{
    font-size: 85%;
    width: 35ex;
    height: 30ex;
  }}
}}
/*]]>*/</style>
</head>
<body>
""", end="")
  P('<h1><a href="{}">語彙力年齢診断</a></h1>', script_url)
  if result_name and user_name and history:
    aoa_dbm = tkrzw.DBM()
    aoa_dbm.Open(AOA_RANKS_PATH, False, dbm="SkipDBM").OrDie()
    P('<article>')
    correct_aoas = []
    wrong_aoas = []
    record_scores = []
    for rank, score in history:
      key = "{:05d}".format(rank)
      value = aoa_dbm.GetStr(key)
      record = ParseRecord(value)
      if score:
        correct_aoas.append(record[1])
      else:
        wrong_aoas.append(record[1])
      record_scores.append((rank, record, score))        
    if correct_aoas:
      correct_aoas = sorted(correct_aoas, reverse=True)
      aoa = correct_aoas[min(2, len(correct_aoas) - 1)]
      if len(correct_aoas) < 3:
        aoa -= 3 - len(correct_aoas)
    else:
      aoa = 0
    if wrong_aoas:
      wrong_aoas = sorted(wrong_aoas)
      min_index = max(0, int(NUM_QUIZES / 3 - len(wrong_aoas)))
      wrong_aoa = wrong_aoas[min(min_index, len(wrong_aoas) - 1)]
      if len(wrong_aoas) >= 3:
        wrong_aoa -= 0.5
      aoa = (aoa * len(correct_aoas) + wrong_aoa * len(wrong_aoas)) / (len(correct_aoas) + len(wrong_aoas))
    aoa = min(max(3, round(aoa)), 20)
    P('<p>{}さんの語彙力は、ネイティブスピーカの<strong class="result_age">{}</strong>歳相当です。</p>', user_name, aoa)
    if aoa >= 20:
      msg = "あなたは、ネイティブスピーカ並みかそれ以上の語彙力を持っています。"
    elif aoa >= 19:
      msg = "あなたは、ほとんどネイティブスピーカ並みの語彙力を持っています。"
    elif aoa >= 18:
      msg = "海外や外資系企業で仕事をするのにも問題ない水準の語彙力です。"
    elif aoa >= 17:
      msg = "英語の新聞や小説を辞書なしで読める水準の語彙力です。"
    elif aoa >= 16:
      msg = "海外の大学への留学を検討できる水準の語彙力です。"
    elif aoa >= 15:
      msg = "ネイティブスピーカと楽しく会話できる水準の語彙力です。"
    elif aoa >= 14:
      msg = "海外旅行で困らない水準の語彙力です。"
    elif aoa >= 13:
      msg = "海外の高校への留学を検討できる水準の語彙力です。"
    elif aoa >= 12:
      msg = "辞書があれば英語の新聞や小説が読める水準の語彙力です。"
    elif aoa >= 11:
      msg = "英語で片言で会話できる水準の語彙力です。"
    elif aoa >= 10:
      msg = "何とか海外旅行ができる水準の語彙力です。"
    elif aoa >= 9:
      msg = "英語で道案内ができる水準の語彙力です。"
    elif aoa >= 8:
      msg = "小学校からやり直しましょう。"
    elif aoa >= 6:
      msg = "幼稚園からやり直しましょう。"
    elif aoa >= 4:
      msg = "ご冗談でしょう？"
    else:
      msg = "ばぶばぶ・・・"
    P('<p>{}</p>', msg)
    tweet_msg = "#語彙力年齢診断: {}さんの語彙力は、ネイティブスピーカの{}歳相当です。".format(user_name, aoa)
    P('<p>結果をつぶやく: ', end="")
    P('<a href="https://twitter.com/share" class="twitter-share-button" data-text="{}" data-count="none">Tweet</a>', tweet_msg, end="")
    P('<script>!function(d,s,id){{var js,fjs=d.getElementsByTagName(s)[0];if(!d.getElementById(id)){{js=d.createElement(s);js.id=id;js.src="//platform.twitter.com/widgets.js";fjs.parentNode.insertBefore(js,fjs);}}}}(document,"script","twitter-wjs");</script>', end="")
    P('</p>')
    P('<p><a href="{}" class="goto">⇨ 再挑戦する</a></p>', script_url)
    P('<ul class="answers_list">')
    for rank, record, score in record_scores:
      class_expr = "answer_correct" if score else "answer_incorrect"
      word_url = "{}?q={}".format(DICT_URL, urllib.parse.quote(record[0]))
      tran_expr = ", ".join(record[3][:5])
      P('<li class="{}"><a href="{}" class="answer_word">{}</a> : <span class="answer_trans">{}</span> : <span class="answer_age">{:.2f}歳</span></li>',
        class_expr, word_url, record[0], tran_expr, record[1])
    P('</ul>')
    P('</article>')
    aoa_dbm.Close().OrDie()    
  elif user_name and current_rank >= 0:
    aoa_dbm = tkrzw.DBM()
    aoa_dbm.Open(AOA_RANKS_PATH, False, dbm="SkipDBM").OrDie()
    num_ranks = aoa_dbm.Count()
    if question >= 0 and answer >= 0:
      history.append((question, answer))
      if answer:
        move = (num_ranks - current_rank) / 8
        move = int(min(move, num_ranks / (len(history) + 1)))
        current_rank += move
        current_rank = min(current_rank, int(num_ranks - WINDOW_WIDTH / 4))
      else:
        move = current_rank / 6
        move = int(min(move, num_ranks / (len(history) + 1)))
        current_rank -= move
        current_rank = max(current_rank, 50)
    current_aoa = ParseRecord(aoa_dbm.GetStr("{:05d}".format(current_rank)))[1]
    if len(history) >= NUM_QUIZES:
      tmp_name = "{:04x}{:08x}".format(
        zlib.adler32(user_name.encode()) % 0x10000, random_seed % 100000000)
      tmp_path = os.path.join(result_dir, tmp_name)
      with open(tmp_path, "w") as tmp_file:
        print(user_name, file=tmp_file)
        for rank, score in history:
          expr = "{}\t{}".format(rank, score)
          print(expr, file=tmp_file)
      P('<article>')
      result_url = "{}?z={}".format(script_url, tmp_name)
      P('<p>検査終了。お疲れ様でした。</p>')
      P('<p><a href="{}" class="goto">⇨ 結果を見る</a></p>', result_url)
      P('</article>')
    else:
      P('<article>')    
      used_words = set()
      for rank, score in history:
        key = "{:05d}".format(rank)
        value = aoa_dbm.GetStr(key)
        record = ParseRecord(value)
        used_words.add(record[0])
      records = GetRecords(aoa_dbm, current_rank)
      core_rank, core_expr = records[int(len(records) / 2)]
      core_record = ParseRecord(core_expr)
      aoa = core_record[1]
      candidates = GetCandidates(records, used_words)
      if candidates:
        correct = candidates[0]
        random.shuffle(candidates)
        P('<p>Q{}: "<strong class="theme_word">{}</strong>" の意味は？</p>', len(history) + 1, correct[1])
        P('<form method="get" name="search_form" action="{}">', script_url)
        P('<ul class="candidates_list">')
        for i, candidate in enumerate(candidates):
          P('<li><input type="radio" name="a" value="{}" id="a{}"/> <label for="a{}">{}</label></li>',
            int(candidate[0] == correct[0]), i, i, ", ".join(candidate[3]))
        P('<li><input type="radio" name="a" value="0" id="ax" checked="checked"/> <label for="ax" class="dunno">（わからない）</label></li>')
        P('</ul>')
        P('<div id="submit_line">')
        P('<input type="submit" value="回答" class="answer_button"/>')
        history_exprs = []
        for rank, score in history:
          history_exprs.append("{}:{}".format(rank, score))
        P('<input type="hidden" name="q" value="{}"/>', correct[0])
        P('<input type="hidden" name="u" value="{}"/>', user_name)
        P('<input type="hidden" name="s" value="{}"/>', random_seed)
        P('<input type="hidden" name="c" value="{}"/>', current_rank)
        P('<input type="hidden" name="h" value="{}"/>', ",".join(history_exprs))
        P('</div>')
        P('</form>')
      else:
        P('<p>予期せぬエラー: レコード不足</p>')
      P('</article>')
    aoa_dbm.Close().OrDie()    
  else:
    P('<article>')
    P("""<p>あなたの英語の語彙力は、ネイティブスピーカの何歳に相当するでしょうか？</p>
<p>{}問のクイズで診断してみましょう。</p>
<div>
<form name="start" method="get" action="{}">
あなたの名前:<input type="text" name="u" size="16" value=""/>
<input type="submit" value="開始" class="start_button"/>
<input type="hidden" name="c" value="{}"/>
<input type="hidden" name="s" value="{}"/>
</form>
</div>
<p class="info">本診断は<a href="https://dbmx.net/dict/">Tkrzw-Dict</a>のサブプロジェクトです。本診断は<a href="http://crr.ugent.be/papers/Kuperman%20et%20al%20AoA%20ratings.pdf">Kupermanらのデータ</a>を使って作成されています。</p>
""", NUM_QUIZES, script_url, START_RANK, random.randint(1, 1<<47), end="")
    P('</article>')
  print("""</body>
</html>
""", end="")


if __name__=="__main__":
  main()
