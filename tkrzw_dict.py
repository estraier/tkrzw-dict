#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Common constants and functions for Tkrzw-dict
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

import importlib
import logging
import math
import operator
import random
import re
import regex
import sys
import tkrzw
import unicodedata


COOC_BASE_SCORE = 1000
NUMERIC_WORD_WEIGHT = 0.2
STOP_WORD_WEIGHT = 0.5
MAX_IDF_WEIGHT = 10.0
IDF_POWER = 1.5
MAX_PROB_SCORE = 0.05


def GetLogger():
  log_format = "%(levelname)s\t%(message)s"
  logging.basicConfig(format=log_format, stream=sys.stderr)
  logger = logging.getLogger("parse_wikipedia")
  logger.setLevel(logging.INFO)
  return logger


def GetCommandFlag(args, flag, num_args):
  arg_index = 0
  value = None
  rest_args = []
  while arg_index < len(args):
    arg = args[arg_index]
    if arg == flag:
      if arg_index + num_args >= len(args):
        raise RuntimeError("insufficient flag arguments")
      if num_args == 0:
        value = True
      elif num_args == 1:
        value = args[arg_index + 1]
        arg_index += num_args
      else:
        value = args[arg_index + 1:arg_index + 1 + num_args]
        arg_index += num_args
    else:
      rest_args.append(arg)
    arg_index += 1
  if num_args == 0 and not value:
    value = False
  args.clear()
  args.extend(rest_args)
  return value


def GetUnusedFlag(args):
  for arg in args:
    if arg == "--":
      break
    elif arg.startswith("--"):
      return arg
  return None


def GetArguments(args):
  result = []
  fixed = False
  for arg in args:
    if fixed:
      result.append(arg)
    elif arg == "--":
      fixed = True
    else:
      result.append(arg)
  return result


def GetWordCountPath(data_prefix):
    return "{}-word-count.tks".format(data_prefix)


def GetCoocCountPath(data_prefix):
    return "{}-cooc-count.tks".format(data_prefix)


def GetPhraseCountPath(data_prefix):
    return "{}-phrase-count.tks".format(data_prefix)


def GetWordProbPath(data_prefix):
    return "{}-word-prob.tkh".format(data_prefix)


def GetCoocProbPath(data_prefix):
    return "{}-cooc-prob.tkh".format(data_prefix)


def GetPhraseProbPath(data_prefix):
    return "{}-phrase-prob.tkh".format(data_prefix)


def GetCoocScorePath(data_prefix):
    return "{}-cooc-score.tkh".format(data_prefix)


def GetCoocIndexPath(data_prefix):
    return "{}-cooc-index.tkh".format(data_prefix)


def RemoveDiacritic(text):
  decomposed = unicodedata.normalize('NFD', text)
  stripped = ""
  removable = True
  for c in decomposed:
    if unicodedata.combining(c) == 0:
      removable = bool(regex.match(r"\p{Latin}", c))
      stripped += c
    elif not removable:
      stripped += c
  return unicodedata.normalize('NFC', stripped)


_regex_spaces = regex.compile(r"[\s]+")
_regex_middle_dots = regex.compile(r"[・·]+")
def NormalizeWord(text):
  text = _regex_spaces.sub(" ", text)
  text = _regex_middle_dots.sub("", text)
  return RemoveDiacritic(text.lower()).strip()


_regex_numeric_word = re.compile(r"^[-0-9.]+$")
def IsNumericWord(word):
  if _regex_numeric_word.search(word):
    return True
  return False


_regex_stop_word_num = re.compile(r"[0-9]")
_set_en_stop_words = set(("the", "a", "an", "be", "do", "not", "and", "or", "but",
                          "no", "any", "some", "this", "these", "that", "those",
                          "i", "my", "me", "mine", "you", "your", "yours", "we", "our", "us", "ours",
                          "he", "his", "him", "she", "her", "hers",
                          "it", "its", "they", "them", "their"))
_regex_stop_word_ja_hiragana = regex.compile(r"^[\p{Hiragana}ー]+$")
_regex_stop_word_ja_date = re.compile(r"^[年月日]*$")
_regex_stop_word_ja_latin = regex.compile(r"[\p{Latin}]")
def IsStopWord(language, word):
  if _regex_stop_word_num.search(word):
    return True
  if language == "en":
    if word in _set_en_stop_words:
      return True
  if language == "ja":
    if _regex_stop_word_ja_hiragana.search(word):
      return True
    if _regex_stop_word_ja_date.search(word):
      return True
    if _regex_stop_word_ja_latin.search(word):
      return True
  return False


_regex_predict_japanese = regex.compile(r"[\p{Hiragana}\p{Katakana}ー\p{Han}]")
def PredictLanguage(text):
  if _regex_predict_japanese.search(text):
    return "ja"
  return "en"


_regex_katakana_only = regex.compile(r"^[\p{Katakana}ー]+$")
def DeduplicateWords(words):
  uniq_words = []
  norm_uniq_words = []
  for word in words:
    norm_word = NormalizeWord(word)
    dup = False
    uniq_min_dist_ratio = 0.21
    if _regex_katakana_only.search(word):
      uniq_min_dist_ratio = 0.41
    for norm_uniq_word in norm_uniq_words:
      dist = tkrzw.Utility.EditDistanceLev(norm_word, norm_uniq_word)
      dist_ratio = dist / max(len(norm_word), len(norm_uniq_word))
      if dist_ratio < uniq_min_dist_ratio:
        dup = True
    if not dup:
      uniq_words.append(word)
      norm_uniq_words.append(norm_word)
  return uniq_words


def TwiddleWords(words, query):
  scored_words = []
  for i, word in enumerate(words):
    score = 10 / (10 + i)
    gain = 1.0
    if word == query:
      gain = 1.6
    elif len(query) > 1 and word.startswith(query):
      gain = 1.4
    elif len(word) > 1 and query.startswith(word):
      gain = 1.3
    elif word.find(query) >= 0 or query.find(word) >= 0:
      gain = 1.2
    else:
      dist = tkrzw.Utility.EditDistanceLev(word, query) / max(len(word), len(query))
      if dist <= 0.3:
        gain = 1.2
      elif dist <= 0.5:
        gain = 1.1
    if len(words) > 8:
      gain **= math.log(len(words)) / math.log(8)
    score *= gain
    scored_words.append((word, score))
  scored_words = sorted(scored_words, key=lambda x: x[1], reverse=True)
  return [x[0] for x in scored_words]
