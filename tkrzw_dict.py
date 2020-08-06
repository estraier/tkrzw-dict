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

import logging
import random
import re
import regex
import sys


COOC_BASE_SCORE = 1000
NUMERIC_WORD_WEIGHT = 0.2
STOP_WORD_WEIGHT = 0.5
MAX_IDF_WEIGHT = 10.0
IDF_POWER = 1.4


def GetLogger():
    log_format = "%(levelname)s\t%(message)s"
    logging.basicConfig(format=log_format, stream=sys.stderr)
    logger = logging.getLogger("parse_wikipedia")
    logger.setLevel(logging.INFO)
    return logger


def GetWordCountPath(data_prefix):
    return "{}-word-count.tks".format(data_prefix)


def GetCoocCountPath(data_prefix):
    return "{}-cooc-count.tks".format(data_prefix)


def GetWordProbPath(data_prefix):
    return "{}-word-prob.tkh".format(data_prefix)


def GetCoocProbPath(data_prefix):
    return "{}-cooc-prob.tkh".format(data_prefix)


def GetCoocScorePath(data_prefix):
    return "{}-cooc-score.tkh".format(data_prefix)


def GetCoocIndexPath(data_prefix):
    return "{}-cooc-index.tkh".format(data_prefix)


_regex_numeric_word = re.compile(r"^[0-9]+$")
def IsNumericWord(word):
  if _regex_numeric_word.search(word):
    return True
  return False


_regex_stop_word_num = re.compile(r"[0-9]")
_set_en_stop_words = set(("the", "a", "an"))
_regex_stop_word_ja_hiragana = regex.compile(r"^[\p{Hiragana}ー]*$")
_regex_stop_word_ja_date = re.compile(r"^[年月日]*$")
_regex_stop_word_ja_latin = regex.compile(r"[\p{Latin}]")
def IsStopWord(word, lang):
  if _regex_stop_word_num.search(word):
    return True
  if lang == "en":
    if word in _set_en_stop_words:
      return True
  if lang == "ja":
    if _regex_stop_word_ja_hiragana.search(word):
      return True
    if _regex_stop_word_ja_date.search(word):
      return True
    if _regex_stop_word_ja_latin.search(word):
      return True
  return False
