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

  def IsJaWordAdjvNounOnly(self, word):
    self.InitMecab()
    word += "の"
    tokens = []
    for token in self.tagger_mecab.parse(word).split("\n"):
      fields = token.split("\t")
      if len(fields) != 4: continue
      tokens.append(fields)
    if len(tokens) < 2:
      return False
    stem = tokens[-2]
    suffix = tokens[-1]
    if stem[1] == "名詞" and stem[2] == "形容動詞語幹":
      return True
    if stem[0] == "的" and stem[1] == "名詞" and stem[2] == "接尾":
      return True
    return False

  def RestoreJaWordAdjSaNoun(self, word):
    self.InitMecab()
    if not regex.search(r"[\p{Han}\p{Hiragana}]さ$", word):
      return word
    tokens = []
    for token in self.tagger_mecab.parse(word).split("\n"):
      fields = token.split("\t")
      if len(fields) != 4: continue
      tokens.append(fields)
    if len(tokens) < 2:
      return word
    stem = tokens[-2]
    suffix = tokens[-1]
    if stem[1] == "名詞" and stem[2] == "形容動詞語幹" and suffix[0] == "さ":
      return stem[0] + "な"
    if stem[1] == "形容詞" and suffix[0] == "さ":
      return stem[3]
    return word

  def ConvertJaWordBaseForm(self, word):
    self.InitMecab()
    tokens = []
    for token in self.tagger_mecab.parse(word).split("\n"):
      fields = token.split("\t")
      if len(fields) != 4: continue
      tokens.append(fields)
    if len(tokens) < 2:
      return word
    last = tokens[-1]
    if (last[0] == "た" and last[1] == "助動詞" and len(tokens) > 1 and
        tokens[-2][1] == "動詞" and word.endswith(last[0])):
      word = word[:-len(last[0])]
      last = tokens[-2]
    if (last[0] != last[3] and regex.search(r"\p{Hiragana}", last[3]) and
        word.endswith(last[0])):
      return word[:-len(last[0])] + last[3]
    return word

  def CutJaWordNounThing(self, word):
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
    if (suffix[0] in ("もの", "物", "こと", "事") and suffix[1] == "名詞" and
        stem[1] != "名詞" and stem[0] == stem[3] and word.endswith(suffix[0])):
      return word[:-len(suffix[0])]
    return word

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
    return ["", "", "", "", ""]

  def NormalizeJaWordForPos(self, pos, tran):
    if pos in ("verb", "adjective", "adverb"):
      tran = self.CutJaWordNounThing(tran)
    if pos == "noun":
      stem = regex.sub(
        r"を(する|される|行う|実行する|実施する|挙行する|遂行する)", "", tran)
      if stem != tran and len(stem) >= 2:
        return stem
      if self.IsJaWordSahenVerb(tran):
        return regex.sub(r"する$", "", tran)
      if tran and tran[-1] in ("な", "に"):
        stem = tran[:-1]
        if self.IsJaWordAdjvNoun(stem):
          return stem
      if tran.endswith("い"):
        pos = self.GetJaLastPos(tran)
        if pos and pos[1] == "形容詞":
          return tran[:-1] + "さ"
    if pos == "verb":
      if not tran.endswith("な"):
        restored = self.ConvertJaWordBaseForm(tran)
        if restored != tran:
          return restored
      if self.IsJaWordSahenNoun(tran):
        return tran + "する"
    if pos == "adjective":
      restored = self.RestoreJaWordAdjSaNoun(tran)
      if restored != tran:
        return restored
      if len(tran) >= 2 and tran.endswith("さ"):
        restored = tran[:-1]
        if self.IsJaWordAdjvNoun(restored):
          return restored + "な"
        restored = tran[:-1] + "い"
        pos = self.GetJaLastPos(restored)
        if pos and pos[1] == "形容詞":
          return restored
      if tran.endswith("である"):
        tran = tran[:-3]
      if self.IsJaWordAdjvNoun(tran):
        restored_na = tran + "な"
        restored_no = tran + "の"
        if tran.endswith("か") or tran.endswith("的") or self.IsJaWordAdjvNounOnly(tran):
          return restored_na
        return restored_no
      if (tran.endswith("の") and self.IsJaWordAdjvNoun(tran[:-1]) and
          self.IsJaWordAdjvNounOnly(tran[:-1])):
        return tran[:-1] + "な"
      if self.IsJaWordNoun(tran) and not tran.endswith("の"):
        return tran + "の"
      if tran.endswith("く"):
        tran = self.ConvertJaWordBaseForm(tran)
    if pos == "adverb":
      if self.IsJaWordAdjvNoun(tran):
        return tran + "に"
    return tran

  def StripJaParticles(self, word):
    for particle in ("のために", "のため", "ために", "ことから", "ことに", "ことの", "ことを"):
      if len(word) > len(particle):
        if word.startswith(particle):
          return (word[len(particle):], particle, "")
        if word.endswith(particle):
          return (word[:-len(particle)], "", particle)
    self.InitMecab()
    tokens = self.tagger_mecab.parse(word).split("\n")
    parsed = []
    for token in self.tagger_mecab.parse(word).split("\n"):
      fields = token.split("\t")
      if len(fields) != 4: continue
      parsed.append(fields)
    prefix = ""
    suffix = ""
    while (len(parsed) >= 2 and
           ((parsed[0][1] == "助詞") or
            (parsed[0][1] == "接続詞" and parsed[0][0] in ("と", "で")))):
      prefix = prefix + parsed[0][0]
      parsed = parsed[1:]
    while (len(parsed) >= 2 and
           ((parsed[-1][1] == "助詞" and parsed[-1][0] not in ("た", "て", "で")) or
            (parsed[-1][1] == "接続詞" and parsed[-1][0] in ("と", "で")))):
      suffix = parsed[-1][0] + suffix
      parsed = parsed[:-1]
    if len(parsed) == len(tokens):
      return (word, "", "")
    return ("".join([x[0] for x in parsed]), prefix, suffix)
