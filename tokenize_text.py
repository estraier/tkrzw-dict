#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to tokenize sentences in TSV
#
# Usage:
# $ bzcat enwiki-raw.tsv.bz2 |
#   ./tokenize_text.py en 100 | bzip2 -c > enwiki-tokenized.tsv.bz2
# $ bzcat jawiki-raw.tsv.bz2 |
#   ./tokenize_text.py ja 100 | bzip2 -c > jawiki-tokenized.tsv.bz2
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
import MeCab
import regex
import sys
import unicodedata


log_format = "%(levelname)s\t%(message)s"
logging.basicConfig(format=log_format, stream=sys.stderr)
logger = logging.getLogger("tokenize_text")
logger.setLevel(logging.INFO)


def SplitSentences(text):
  text = regex.sub(r'(^|\W)(Mr\.|Mrs\.|Dr\.|Prof\.|Esq\.)', r'\1\2{_XxX_}', text)
  text = regex.sub(r'(^|\W)(e\.g\.|eg\.|i\.e\.|ie\.|p\.s\.|ps\.)',
                   r'\1\2{_XxX_}', text, flags=regex.IGNORECASE)
  text = regex.sub(r'(^|\W)(\p{Lu}\.) *(\p{Lu}\.) *(\p{Lu}\.) *(\p{Lu}\.)', r'\1\2\3\4\5', text)
  text = regex.sub(r'(^|\W)(\p{Lu}\.) *(\p{Lu}\.) *(\p{Lu}\.)', r'\1\2\3\4', text)
  text = regex.sub(r'(^|\W)(\p{Lu}\.) *(\p{Lu}\.)', r'\1\2\3', text)
  text = regex.sub(r'([.?!]) +([\"\p{Lu}\p{Lo}])', '\\1\n\\2', text)
  text = regex.sub(r'{_XxX_}', '', text)
  text = regex.sub('。', '。\n', text)
  sentences = []
  for sentence in text.split('\n'):
    sentence = sentence.strip()
    if sentence:
      sentences.append(sentence)
  return sentences


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


def NormalizeWords(text):
  text = regex.sub(r"(^|\W)(\p{Ll})\.(\p{Ll})\.(\p{Ll})\.(\p{Ll})\.", r"\1\2\3\4\5", text)
  text = regex.sub(r"(^|\W)(\p{Ll})\.(\p{Ll})\.(\p{Ll})\.", r"\1\2\3\4", text)
  text = regex.sub(r"(^|\W)(\p{Ll})\.(\p{Ll})\.", r"\1\2\3", text)
  text = regex.sub(r"[\p{Ps}\p{Pd}\p{Pi}\p{Pf}\p{S}~!@#$%^&*+|\\:;,/?]", " ", text)
  text = text.replace("\u30FB", "\u00B7")
  return text


def GetWordsEn(sentence):
  words = []
  for word in regex.findall(r"[\p{Latin}0-9]+[-_'\p{Latin}0-9]*", sentence):
    words.append(word)
  return words


tagger = MeCab.Tagger(r"--node-format=%m\t%ps\t%pe\n")
def GetWordsJa(sentence):
  words = []
  last_word = None
  last_end = -1
  for token in tagger.parse(sentence).split("\n"):
    fields = token.split("\t")
    if len(fields) != 3: continue
    word = fields[0]
    begin = int(fields[1])
    end = int(fields[2])
    if (last_word and begin == last_end and
        regex.search(r"[-_'\p{Latin}0-9]$", last_word) and
        regex.search(r"^[-_'\p{Latin}0-9]", word)):
      last_word += word
    else:
      if last_word:
        words.append(last_word)
      last_word = word
    last_end = end
  if last_word:
    words.append(word)
  good_words = []
  for word in words:
    if regex.search(r"[\p{Katakana}\p{Hiragana}ー\p{Han}\p{Latin}0-9]", word):
      good_words.append(word)
  return good_words


def ProcessTSV(language, max_sentences, tsv):
  num_sentences, num_words = 0, 0
  sentences = []
  for section in tsv.split("\t"):
    sentences.extend(SplitSentences(section))
  sentences = sentences[:max_sentences]
  output_fields = []
  for sentence in sentences:
    sentence = RemoveDiacritic(sentence.lower())
    sentence = NormalizeWords(sentence)
    if language == "en":
      words = GetWordsEn(sentence)
    elif language == "ja":
      words = GetWordsJa(sentence)
    else:
      raise ValueError("unsupported language: " + language)
    if words:
      output_fields.append(" ".join(words))
      num_sentences += 1
      num_words += len(words)
  if output_fields:
    print("\t".join(output_fields))
    return num_sentences, num_words
  return None
    
      
def main():
  language = sys.argv[1] if len(sys.argv) > 1 else "en"
  max_sentences = int(sys.argv[2]) if len(sys.argv) > 2 else 100000
  logger.info("Process started: language={}, max_sentences_per_doc={}".format(
    language, max_sentences))
  count = 0
  num_records, num_sentences, num_words = 0, 0, 0
  for line in sys.stdin:
    line = line.strip()
    if not line: continue
    count += 1
    stats = ProcessTSV(language, max_sentences, line)
    if stats:
      num_records += 1
      num_sentences += stats[0]
      num_words += stats[1]
    if count % 1000 == 0:
      logger.info(
        "Processing: {} input records, {} output records, {} sentences, {} words".format(
          count, num_records, num_sentences, num_words))
  logger.info(
    "Process done: {} input records, {} output records, {} sentences, {} words".format(
      count, num_records, num_sentences, num_words))


if __name__=="__main__":
  main()
