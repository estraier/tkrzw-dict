#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Tokenizer to obtain words in a sentence
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
import re
import regex
import tkrzw_dict
import unicodedata


def SplitSentences(text):
  text = regex.sub(r'(^|\W)(Mr\.|Mrs\.|Dr\.|Prof\.|Esq\.)', r'\1\2{_XxX_}', text)
  text = regex.sub(r'(^|\W)(e\.g\.|eg\.|i\.e\.|ie\.|c\.f\.|cf\.|p\.s\.|ps\.)',
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


class Tokenizer:
  def __init__(self):
    self.regex_abbrev4 = regex.compile(r"(^|\W)(\p{Lu})\.(\p{Lu})\.(\p{Lu})\.(\p{Lu})\.")
    self.regex_abbrev3 = regex.compile(r"(^|\W)(\p{Lu})\.(\p{Lu})\.(\p{Lu})\.")
    self.regex_abbrev2 = regex.compile(r"(^|\W)(\p{Lu})\.(\p{Lu})\.")
    self.regex_symbols = regex.compile(r"[\p{Ps}\p{Pe}\p{Pi}\p{Pf}\p{S}~!@#$%^&*+|\\:;,/?]")
    self.regex_en_simple_words = regex.compile(r"-?[\p{Latin}0-9]+[-_'\p{Latin}0-9]*")
    self.lemmatizer_wordnet = None
    self.regex_ja_sections = regex.compile(r"([\p{Hiragana}\p{Katakana}ー\p{Han}]+)")
    self.tagger_mecab = None
    pass

  def Tokenize(self, language, sentence, lowering, stemming):
    sentence = self.NormalizeSentence(sentence)
    if language == "en":
      if stemming:
        words = self.TokenizeEnStemming(sentence)
      else:
        words = self.TokenizeEnSimple(sentence)
    elif language == "ja":
      words = self.TokenizeJaMecab(sentence, stemming)
    else:
      raise ValueError("unsupported language: " + language)
    if lowering:
      words = [tkrzw_dict.NormalizeWord(x) for x in words]
    return words

  def NormalizeSentence(self, text):
    text = self.regex_abbrev4.sub(r"\1\2\3\4\5", text)
    text = self.regex_abbrev3.sub(r"\1\2\3\4", text)
    text = self.regex_abbrev2.sub(r"\1\2\3", text)
    text = self.regex_symbols.sub(" ", text)
    return text

  def TokenizeEnSimple(self, sentence):
    return self.regex_en_simple_words.findall(sentence)

  def TokenizeEnStemming(self, sentence):
    nltk = importlib.import_module("nltk")
    if not self.lemmatizer_wordnet:
      self.lemmatizer_wordnet = nltk.stem.wordnet.WordNetLemmatizer()
    words = []
    tokens = nltk.word_tokenize(sentence)
    for word, pos in nltk.pos_tag(tokens):
      if not self.regex_en_simple_words.match(word): continue
      if not word or not pos: continue
      pos_class = pos[0]
      if pos_class == "V":
        lemma = self.lemmatizer_wordnet.lemmatize(word.lower(), nltk.corpus.wordnet.VERB)
      elif pos_class == "J":
        lemma = self.lemmatizer_wordnet.lemmatize(word.lower(), nltk.corpus.wordnet.ADJ)
      elif pos_class == "R":
        lemma = self.lemmatizer_wordnet.lemmatize(word.lower(), nltk.corpus.wordnet.ADV)
      else:
        lemma = self.lemmatizer_wordnet.lemmatize(word.lower(), nltk.corpus.wordnet.NOUN)
      if len(lemma) > 1 and lemma != word:
        if word.istitle():
          word = lemma.title()
        elif word.isupper():
          word = lemma.upper()
        else:
          word = lemma
      words.append(word)
    return words

  def InitMecab(self):
    mecab = importlib.import_module("MeCab")
    if not self.tagger_mecab:
      self.tagger_mecab = mecab.Tagger(r"--node-format=%m\t%f[0]\t%f[1]\t%f[6]\n")
    
  def TokenizeJaMecab(self, sentence, stemming):
    self.InitMecab()
    sentence = self.regex_ja_sections.sub(r" \1 ", sentence)
    words = []
    for section in sentence.split(" "):
      section = section.strip()
      if not section: continue
      if self.regex_ja_sections.match(section):
        for token in self.tagger_mecab.parse(section).split("\n"):
          fields = token.split("\t")
          if len(fields) != 4: continue
          word, pos, subpos, stem = fields
          if stemming and stem:
            word = stem
          words.append(word)
      else:
        words.extend(self.TokenizeEnSimple(section))
    return words

  def IsJaWordNoun(self, word):
    self.InitMecab()
    is_noun = False
    for token in self.tagger_mecab.parse(word).split("\n"):
      fields = token.split("\t")
      if len(fields) != 4: continue
      is_noun = fields[1] == "名詞"
    return is_noun

  def IsJaWordSahenNoun(self, word):
    self.InitMecab()
    is_sahen = False
    for token in self.tagger_mecab.parse(word).split("\n"):
      fields = token.split("\t")
      if len(fields) != 4: continue
      is_sahen = fields[1] == "名詞" and fields[2] == "サ変接続"
    return is_sahen

  def IsJaWordSahenVerb(self, word):
    self.InitMecab()
    tokens = []
    for token in self.tagger_mecab.parse(word).split("\n"):
      fields = token.split("\t")
      if len(fields) != 4: continue
      tokens.append(fields)
    if len(tokens) < 2:
      return False
    stem = tokens[-2]
    suffix = tokens[-1]
    return stem[1] == "名詞" and stem[2] == "サ変接続" and suffix[0] == "する"

  def IsJaWordAdjvNoun(self, word):
    self.InitMecab()
    if not regex.search(r"\p{Han}$", word): return False
    word += "な"
    tokens = []
    for token in self.tagger_mecab.parse(word).split("\n"):
      fields = token.split("\t")
      if len(fields) != 4: continue
      tokens.append(fields)
    if len(tokens) < 2:
      return False
    stem = tokens[-2]
    suffix = tokens[-1]
    if stem[1] == "名詞" and stem[2] == "形容動詞語幹" and suffix[0] == "な":
      return True
    if stem[0] == "的" and stem[1] == "名詞" and stem[2] == "接尾" and suffix[0] == "な":
      return True
    return False

  def CutJaWordNounParticle(self, word):
    self.InitMecab()
    tokens = []
    for token in self.tagger_mecab.parse(word).split("\n"):
      fields = token.split("\t")
      if len(fields) != 4: continue
      tokens.append(fields)
    if len(tokens) < 2:
      return word
    stem = tokens[-2]
    suffix = tokens[-1]
    if stem[1] == "名詞" and suffix[1] in ("助詞", "助動詞"):
      if word.endswith(suffix[0]):
        return word[:-len(suffix[0])]
    return word

  def GetJaLastPos(self, word):
    self.InitMecab()
    for token in reversed(self.tagger_mecab.parse(word).split("\n")):
      fields = token.split("\t")
      if len(fields) != 4: continue
      return fields
    return ["", "", "", ""]
