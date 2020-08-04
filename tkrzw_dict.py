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
import regex
import sys


NUMERIC_WORD_WEIGHT = 0.2
STOP_WORD_WEIGHT = 0.5
MAX_IDF_WEIGHT = 10.0
IDF_POWER = 1.6


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


def IsNumericWord(word):
  if regex.search(r"^[0-9]+$", word):
    return True


def IsStopWord(word, lang):
  if regex.search(r"[0-9]", word):
    return True
  if lang == "en":
    if word in ("the", "a", "an"):
      return True
  if lang == "ja":
    if regex.search(r"^[\p{Hiragana}ー]*$", word):
      return True
    if regex.search(r"^[年月日]*$", word):
      return True
    if regex.search(r"[\p{Latin}]", word):
      return True
  return False
