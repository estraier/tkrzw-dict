#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to build a union database by merging TSV dictionaries
#
# Usage:
#   build_union_db.py [--output str] [--core str] [--gross str] [--top str] [--slim str]
#     [--phrase_prob str] [--tran_prob str] [--tran_aux str] [--tran_aux_last str]
#     [--rev_prob str] [--cooc_prob str] [--aoa str] [--keyword str] [--min_prob str]
#     [--quiet] inputs...
#   (An input specified as "label:tsv_file".
#
# Example:
#   ./build_union_db.py --output union-body.tkh \
#     --phrase_prob enwiki-phrase-prob.tkh --tran_prob tran-prob.tkh \
#     --tran_aux dict1.tsv,dict2.tsv --rev_prob jawiki-word-prob.tkh \
#     --cooc_prob enwiki-cooc-prob.tkh --min_prob we:0.00001 \
#     wj:wiktionary-ja.tsv wn:wordnet.tsv we:wiktionary-en.tsv
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

import collections
import json
import logging
import math
import operator
import os
import regex
import sys
import time
import tkrzw
import tkrzw_dict
import tkrzw_pron_util
import tkrzw_tokenizer
import unicodedata


logger = tkrzw_dict.GetLogger()
poses = ("noun", "verb", "adjective", "adverb",
         "pronoun", "auxverb", "preposition", "determiner", "article",
         "interjection", "conjunction", "prefix", "suffix",
         "abbreviation", "phrase", "misc")
inflection_names = ("noun_plural", "verb_singular", "verb_present_participle",
                   "verb_past", "verb_past_participle",
                   "adjective_comparative", "adjective_superlative",
                   "adverb_comparative", "adverb_superlative")
etymology_names = ("etymology_prefix", "etymology_core", "etymology_suffix")
top_names = ("pronunciation",) + inflection_names + etymology_names
rel_weights = {"synonym": 1.0,
               "hypernym": 0.9,
               "hyponym": 0.8,
               "antonym": 0.2,
               "derivative": 0.7,
               "relation": 0.5}
noun_suffixes = [
  "es", "s", "ment", "age", "ics", "ness", "ity", "ism", "or", "er", "ist", "t", "pt", "th",
  "ian", "ee", "tion", "sion", "ty", "ance", "ence", "ency", "cy", "ry", "ary", "ery", "ory",
  "al", "age", "dom", "hood", "ship", "nomy", "ing", "ication", "icator", "ce", "se", "son",
  "iation", "ant", "faction", "ture", "sure", "nance", "y", "ess",
]
verb_suffixes = [
  "ify", "en", "ize", "ise", "fy", "ate", "age", "e",
]
adjective_suffixes = [
  "some", "able", "ible", "ic", "ical", "ial", "ive", "ful", "less", "ly", "ous", "y",
  "tic", "ine", "ised", "ing", "ed", "ish", "al", "ual", "icable", "er", "est", "ent", "ific",
  "ative", "tative", "ant", "ary",
]
adverb_suffixes = [
  "ly",
]
particles = {
  "aback", "about", "above", "abroad", "across", "after", "against", "ahead", "along",
  "amid", "among", "apart", "around", "as", "at", "away", "back", "before", "behind",
  "below", "beneath", "between", "beside", "beyond", "by", "despite", "during", "down",
  "except", "for", "forth", "from", "in", "inside", "into", "near", "of", "off", "on",
  "onto", "out", "outside", "over", "per", "re", "since", "than", "through", "throughout",
  "till", "to", "together", "toward", "under", "until", "up", "upon", "with", "within",
  "without", "via",
}
obj_pronouns = [
  "me", "us", "you", "it", "him", "her", "them", "this", "these", "that", "those",
  "myself", "ourselves", "yourself", "yourselves", "itself", "himself", "herself", "themselves",
]
misc_stop_words = {
  "the", "a", "an", "I", "my", "me", "mine", "you", "your", "yours", "he", "his", "him",
  "she", "her", "hers", "it", "its", "they", "their", "them", "theirs",
  "we", "our", "us", "ours", "some", "any", "one", "someone", "something",
  "myself", "yourself", "yourselves", "himself", "herself", "itself", "themselves", "oneself",
  "who", "whom", "whose", "what", "where", "when", "why", "how", "and", "but", "not", "no",
  "never", "ever", "time", "place", "people", "person", "this", "these", "that", "those",
  "other", "another", "yes", "thou",
  "back", "much", "many", "more", "most", "good", "well", "better", "best", "all",
}
phrase_wildcards = {
  "one", "one's", "someone", "someone's", "something", "something's",
}
wiki_stop_words = {
  "wikipedia", "encyclopedia", "page", "pages", "edit", "edits", "comment", "comments",
}
no_parents = {
  "number", "ground", "red", "happen", "letter", "monitor", "feed", "winter", "brake",
  "partner", "sister", "environment", "moment", "gun", "shower", "trigger", "wound", "bound",
  "weed", "saw", "copper", "buffer", "lump", "wary", "stove", "doctor", "hinder",
  "tower", "poetry", "parity", "fell", "lay", "bit", "drug", "grass", "shore", "notice",
  "butter", "slang", "grope", "feces", "left", "former", "found", "every", "scheme",
  "evening", "architecture", "hat", "slice", "bite", "tender", "bully", "translate",
  "fence", "liver", "special", "specific", "species", "statistics", "mathematics", "caution",
  "span", "fleet", "language", "gripe", "dribble", "total", "error", "option", "important",
  "shine", "dental", "irony", "transplant", "chemistry", "physics", "grocery", "grade",
  "gutter", "dove", "weary", "queer", "shove", "buggy", "twine", "tier", "rung", "spat",
  "pang", "jibe", "pent", "lode", "gelt", "plant", "plane", "pants", "craze", "grove",
  "downy", "musty", "mangy", "moped", "caper", "balmy", "tinny", "induce", "treaty",
  "chili", "chilli", "chile", "castor", "landry", "start", "baby", "means", "transfer",
  "interior", "exterior", "rabbit", "stripe", "fairy", "shunt", "clove", "abode", "bends",
  "molt", "holler", "feudal", "bounce", "livery", "wan", "sod", "dug", "het", "gat",
  "cover", "book", "cause", "quality", "process", "provide", "entry", "specify", "morning",
  "guarantee", "listen", "identity", "clone", "impress", "belly", "mansion",
}
force_parents = {
  "upwards": "upward", "towards": "toward", "identify": "identity", "guaranty": "guarantee",
  "advice": "advise", "device": "devise", "practice": "practise", "morn": "morning",
  "approximately": "approximate", "invocation": "invoke", "spec": "specify",
  "prisoner": "prison", "emission": "emit", "omission": "omit", "transmission": "transmit",
  "fission": "fissure", "competitive": "compete", "competitor": "compete",
  "conservative": "conserve", "pronunciation": "pronounce", "revelation": "reveal",
  "possession": "possess", "schema": "scheme", "further": "far", "farther": "far",
  "conjunction": "conjunct", "conjunctive": "conjunct", "location": "locate",
  "conjugation": "conjugate", "conjugative": "conjugate", "installation": "install",
  "translation": "translate", "formation": "form", "variation": "vary",
  "importance": "important", "innovative": "innovate", "bated": "bate",
  "chemist": "chemistry", "chemical": "chemistry", "chem": "chemistry",
  "architect": "architecture", "grocer": "grocery", "critic": "critique",
  "chilly": "chill", "launder": "laundry", "tension": "tense", "revolution": "revolve",
  "sensitive": "sense", "mutation": "mutate", "mutant": "mutate", "fated": "fate",
  "apery": "ape", "dingy": "dinge", "precession": "precess", "expertise": "expert",
  "dramatic": "drama", "pic": "picture", "tragic": "tragedy", "manse": "mansion",
  "administrate": "administer", "administrative": "administrate", "inquiry": "inquire",
  "administration": "administrate", "administrator": "administrate", "diplomat": "diplomacy",
  "federal": "federation", "analysis": "analyze", "emphasis": "emphasize",
  "chlorine": "chloride", "recognition": "recognize", "opposite": "oppose", "opponent": "oppose",
  "response": "respond", "tolerant": "tolerate", "remainder": "remain",
  "differential": "different", "differentiate": "different", "failure": "fail",
  "explosive": "explode", "civilization": "civil", "civilize": "civil",
  "success": "succeed", "application": "apply", "therapeutic": "therapy",
  "medical": "medicine", "beneficial": "benefit", "pianist": "piano",
  "blonde": "blond", "classification": "classify", "classify": "class",
  "technique": "technical", "technology": "technique", "technician": "technical",
  "millionaire": "million", "billionaire": "billion", "cigarette": "cigar",
  "adhesion": "adhere", "adhesive": "adhere", "chaotic": "chaos", "disclosure": "disclose",
  "destruction": "destroy", "concession": "concede", "rental": "rent",
  "influential": "influence", "strategic": "strategy", "minimal": "minimum",
  "mini": "minimum", "triangular": "triangle", "rebellion": "rebel",
  "intent": "intend", "replica": "replicate", "timer": "time", "timed": "time",
  "sparkle": "spark", "consensus": "consent", "probably": "probable", "pleasant": "please",
  "philosopher": "philosophy", "radiate": "radius", "tutorial": "tutor",
  "terminal": "terminus", "terminate": "terminus", "grief": "grieve", "grievance": "grieve",
  "anime": "animate", "surgeon": "surgery", "partition": "part", "pretense": "pretend",
  "concept": "conceive", "conceptual": "conceive", "solidarity": "solid",
  "economic": "economy", "economist": "economy", "decisive": "decide",
  "offense": "offend", "offensive": "offend", "necessary": "necessity",
  "frequency": "frequent", "portrait": "portray", "digital": "digit",
  "storage": "store", "nearly": "near", "granny": "grandmother", "sorry": "sorrow",
  "modification": "modify", "characteristic": "character", "anxious": "anxiety",
  "quantify": "quantity", "qualify": "quality", "appendix": "append",
  "quantitative": "quantity", "qualitative": "quality", "supremacy": "supreme",
  "imaginary": "imagine", "imaginative": "imagine", "disastrous": "disaster",
  "systematic": "system", "absorption": "absorb", "disciple": "discipline",
  "favorite": "favor", "prescription": "prescribe", "dominant": "dominate",
  "relief": "relieve", "laughter": "laugh", "participant": "participate",
  "companion": "company", "circular": "circle", "synthetic": "synthesis",
  "choice": "choose", "entrance": "entry", "maintenance": "maintain",
  "social": "society", "substantial": "substance", "identification": "identify",
  "assumption": "assume", "poet": "poetry", "info": "information", "information": "inform",
  "visible": "vision", "realistic": "real", "consumption": "consume", "reception": "recept",
  "photo": "photograph", "demo": "demonstrate", "publish": "public",
  "volunteer": "voluntary", "politician": "politics", "rationale": "rational",
  "physician": "physic", "physicist": "physics", "spectral": "specter",
  "birdie": "bird", "distillate": "distill", "earnings": "earn", "chimp": "chimpanzee",
  "nutrient": "nutrition", "nutritive": "nutrition", "delicacy": "delicate",
  "suspicion": "suspect", "disbelief": "disbelieve", "provocative": "provoke",
  "irritant": "irritate", "displeasure": "displease", "erroneous": "error",
  "humility": "humiliate", "consequence": "consequent", "barbaric": "barbarian",
  "mystic": "mystery", "festive": "festival", "festal": "festival", "intimacy": "intimate",
  "respiratory": "respiration", "respirator": "respiration", "sarcastic": "sarcasm",
  "crucify": "crucifix", "crucifixion": "crucifix", "abdominal": "abdomen",
  "medial": "median", "bureaucrat": "bureau", "wholly": "whole", "consul": "consulate",
  "repetition": "repeat", "repetitive": "repeat", "conquest": "conquer", "cavern": "cave",
  "rubbish": "rubble", "flammable": "flame", "ignorant": "ignore", "solitude": "solitary",
  "curiosity": "curious", "exceptionally": "exceptional", "blotch": "blot", "suckle": "suck",
  "negligent": "neglect", "negligence": "neglect", "infamous": "infamy",
  "deception": "deceit", "deceit": "deceive", "deceptive": "deceive",
  "irritable": "irritate", "prevalent": "prevail", "accusatory": "accuse",
  "conspiracy": "conspire", "envelop": "envelope", "capacitance": "capacitor",
  "romantic": "romance", "perm": "permanent", "feminist": "feminine",
  "demolition": "demolish", "trivial": "trivia", "instantaneous": "instant",
  "expense": "expend", "expenditure": "expend", "memorize": "memory",
  "memo": "memorandum", "consortium": "consort", "medallion": "medal", "godless": "god",
  "abrasion": "abrase", "abrasive": "abrase", "atheist": "atheism", "reunion": "reunite",
  "kindergartner": "kindergarten", "duckling": "duck", "introductory": "introduce",
  "baptism": "baptize", "sled": "sledge", "bobsled": "bobsleigh",
  "tarp": "tarpaulin", "intricacy": "intricate", "reverberate": "reverb",
  "glacial": "glacier", "legislature": "legislate", "redemption": "redeem",
  "predominant": "predominate", "lull": "lullaby", "butt": "buttock", "comfy": "comfort",
  "verification": "verify", "spectacular": "spectacle", "applause": "applaud",
  "theoretical": "theory", "curvature": "curve", "simply": "simple", "cafe": "cafeteria",
  "discussion": "discuss", "comparable": "compare", "comparative": "compare",
  "hysteric": "hysteria", "partial": "part", "generosity": "generous", "maths": "math",
  "prophecy": "prophesy", "prophet": "prophecy", "satisfactory": "satisfy",
  "fulfillment": "fulfill", "sufficient": "suffice", "energetic": "energy",
  "cosmic": "cosmos", "petrol": "petroleum", "applicable": "apply", "splendid": "splendor",
  "reproductive": "reproduce", "apologetic": "apology", "nervous": "nerve",
  "metabolic": "metabolism", "potency": "potent", "impotency": "impotent", "penal": "penalty",
  "migratory": "migrate", "migrant": "migrate", "immigrant": "immigrate", "emigrant": "emigrate",
  "amphibious": "amphibian", "menstrual": "menstruation", "president": "preside",
  "receptionist": "reception", "reception": "receive", "receipt": "receive",
  "receptive": "receive", "remembrance": "remember", "heartbroken": "heartbreak",
  "residential": "residence", "residency": "residence", "resident": "residence",
  "preparatory": "prepare", "glamorous": "glamour", "defense": "defend",
  "cellular": "cell", "viscosity": "viscous", "rhino": "rhinoceros", "hippo": "hippopotamus",
  "ancestral": "ancestor", "negative": "negate", "bacteria": "bacterium",
  "registration": "register", "registry": "register", "inaugural": "inaugurate",
  "alkaline": "alkali", "humane": "human", "divisible": "divide", "capacity": "capable",
  "grandpa": "grandfather", "grandma": "grandmother", "nauseous": "nausea",
  "luncheon": "lunch", "conscientious": "conscience", "mandatory": "mandate",
  "cleric": "clergy", "corrosion": "corrode", "limo": "limousine", "descriptive": "describe",
  "inflammable": "inflame", "inflammation": "inflame", "tremble": "tremor",
  "enthusiast": "enthusiasm", "pussy": "puss", "considerate": "consider",
  "eternity": "eternal", "monstrous": "monster", "clarity": "clarify", "illiteracy": "illiterate",
  "clarification": "clarify", "muscular": "muscle", "furniture": "furnish",
  "perception": "perceive", "percept": "perceive", "sensory": "sense", "symptomatic": "symptom",
  "destination": "destine", "categorical": "category", "ascent": "ascend",
  "ingenuity": "ingenious", "invention": "invent", "gymnast": "gymnastic",
  "propel": "propulsion", "belief": "believe", "whimsy": "whim", "disciplinary": "discipline",
  "mischievous": "mischief", "crazy": "craze", "liquefy": "liquid", "delicacy": "delicate",
  "confectionery": "confection", "resilience": "resilient", "grad": "graduate",
  "therapist": "therapy", "perseverance": "persevere", "intro": "introduction",
  "abolition": "abolish", "reparation": "repair", "testify": "testimony", "sports": "sport",
  "disqualification": "disqualify", "rectangular": "rectangle", "metropolitan": "metropolis",
  "sportsmanship": "sportsman", "atheist": "atheism", "prognostic": "prognosis",
  "assurance": "assure", "insurance": "insure", "extent": "extend", "mineral": "mine",
  "fort": "fortress", "pharmaceutical": "pharmacy", "menstrual": "menstruum",
  "community": "commune", "communal": "commune", "data": "datum", "agenda": "agendum",
  "metric": "meter", "democrat": "democracy", "presumption": "presume", "shelf": "shelve",
  "solitaire": "solitary", "explanatory": "explain", "woolen": "wool", "planar": "plain",
  "northeastern": "northeast", "northwestern": "northwest",
  "southeastern": "southeast", "southwestern": "southwest",
  "eastward": "east", "westward": "west", "wintry": "winter",
}


class BuildUnionDBBatch:
  def __init__(self, input_confs, output_path, core_labels, full_def_labels, gross_labels,
               surfeit_labels, top_labels, slim_labels, tran_list_labels, supplement_labels,
               phrase_prob_path, tran_prob_path, nmt_prob_path,
               tran_aux_paths, tran_aux_last_paths,
               rev_prob_path, cooc_prob_path, aoa_paths,
               keyword_path, hint_path, synonym_path, min_prob_map):
    self.input_confs = input_confs
    self.output_path = output_path
    self.core_labels = core_labels
    self.full_def_labels = full_def_labels
    self.gross_labels = gross_labels
    self.surfeit_labels = surfeit_labels
    self.top_labels = top_labels
    self.slim_labels = slim_labels
    self.tran_list_labels = tran_list_labels
    self.supplement_labels = supplement_labels
    self.phrase_prob_path = phrase_prob_path
    self.tran_prob_path = tran_prob_path
    self.nmt_prob_path = nmt_prob_path
    self.tran_aux_paths = tran_aux_paths
    self.tran_aux_last_paths = tran_aux_last_paths
    self.rev_prob_path = rev_prob_path
    self.cooc_prob_path = cooc_prob_path
    self.aoa_paths = aoa_paths
    self.keyword_path = keyword_path
    self.hint_path = hint_path
    self.synonym_path = synonym_path
    self.min_prob_map = min_prob_map
    self.tokenizer = tkrzw_tokenizer.Tokenizer()

  def Run(self):
    start_time = time.time()
    logger.info("Process started: input_confs={}, output_path={}".format(
      str(self.input_confs), self.output_path))
    word_dicts = []
    for label, input_path in self.input_confs:
      slim = label in self.slim_labels
      word_dict = self.ReadInput(input_path, slim)
      word_dicts.append((label, word_dict))
    aux_trans = {}
    for tran_aux_path in self.tran_aux_paths:
      if not tran_aux_path: continue
      self.ReadTranAuxTSV(tran_aux_path, aux_trans)
    aux_last_trans = {}
    for tran_aux_last_path in self.tran_aux_last_paths:
      if not tran_aux_last_path: continue
      self.ReadTranAuxTSV(tran_aux_last_path, aux_last_trans)
    raw_aoa_words = collections.defaultdict(list)
    for aoa_path in self.aoa_paths:
      if not aoa_path: continue
      self.ReadAOAWords(aoa_path, raw_aoa_words)
    aoa_words = {}
    for word, values in raw_aoa_words.items():
      aoa_words[word] = sum(values) / len(values)
    keywords = set()
    if self.keyword_path:
      self.ReadKeywords(self.keyword_path, keywords)
    hints = {}
    if self.hint_path:
      self.ReadHints(self.hint_path, hints)
    extra_synonyms = {}
    if self.synonym_path:
      self.ReadSynonyms(self.synonym_path, extra_synonyms)
    self.SaveWords(word_dicts, aux_trans, aux_last_trans, aoa_words,
                   keywords, hints, extra_synonyms)
    logger.info("Process done: elapsed_time={:.2f}s".format(time.time() - start_time))

  def NormalizeText(self, text):
    text = unicodedata.normalize('NFKC', text)
    text = regex.sub(r"[\u2018\u2019\u201A\u201B\u2758\u275B\u275C\u275F\uFF02]", "'", text)
    text = regex.sub(r"[\u201C\u201D\u201E\u201F]", '"', text)
    text = regex.sub(
      r"[\u00AD\u02D7\u2010\u2011\u2012\u2013\u2014\u2015\u2043\u2212\u2796\u2E3A\u2E3B" +
      r"\uFE58\uFE63\uFF0D]", "-", text)
    return text

  def ReadInput(self, input_path, slim):
    start_time = time.time()
    logger.info("Reading an input file: input_path={}".format(input_path))
    word_dict = collections.defaultdict(list)
    num_entries = 0
    with open(input_path) as input_file:
      for line in input_file:
        word = ""
        ipa = ""
        sampa = ""
        texts = []
        inflections = {}
        etymologies = {}
        alternatives = []
        mode = ""
        rel_words = {}
        for field in line.strip().split("\t"):
          columns = field.split("=", 1)
          if len(columns) < 2: continue
          name, value = columns
          value = self.NormalizeText(value)
          value = regex.sub(r"[\p{Z}\p{C}]+", " ", value).strip()
          if name == "word":
            word = value
          elif name == "pronunciation_ipa":
            ipa = value
          elif name == "pronunciation_sampa":
            sampa = value
          elif name.startswith("inflection_"):
            name = regex.sub(r"^[a-z]+_", "", name)
            inflections[name] = inflections.get(name) or value
          elif name.startswith("etymology_"):
            etymologies[name] = value
          elif name == "alternative":
            for alt_word in value.split(","):
              alt_word = alt_word.strip()
              if alt_word:
                alternatives.append(alt_word)
          elif name in poses:
            match = regex.search(r"-\] *Synonyms?: *(.*)", value)
            if match:
              syn_expr = match.group(1)
              syn_expr = regex.sub(r"\[-.*", "", syn_expr).strip()
              if regex.fullmatch(r"[-', \p{Latin}0-9]+", syn_expr):
                old_value = rel_words.get("synonym")
                if old_value:
                  rel_words["synonym"] = old_value + ", " + syn_expr
                else:
                  rel_words["synonym"] = syn_expr
            if slim:
              value = regex.sub(r" \[-+\] .*", "", value).strip()
            if value:
              texts.append((name, value))
          elif name in rel_weights:
            old_value = rel_words.get(name)
            if old_value:
              rel_words[name] = old_value + ", " + value
            else:
              rel_words[name] = value
          elif name == "mode":
            mode = value
        if not ipa and sampa:
          ipa = tkrzw_pron_util.SampaToIPA(sampa)
        if not word or len(word) > 48:
          continue
        if ipa or texts or inflections or etymologies or alternatives:
          key = tkrzw_dict.NormalizeWord(word)
          entry = {"word": word}
          if ipa:
            entry["pronunciation"] = ipa
          for name, value in inflections.items():
            entry[name] = value
          for name, value in etymologies.items():
            entry[name] = value
          if alternatives:
            entry["alternative"] = alternatives
          entry["text"] = texts
          for rel_name, rel_value in rel_words.items():
            entry[rel_name] = rel_value
          if mode:
            key += "\t" + mode
          word_dict[key].append(entry)
          num_entries += 1
        if num_entries % 10000 == 0:
          logger.info("Reading an input: num_entries={}".format(num_entries))
    logger.info("Reading an input done: num_entries={}, elapsed_time={:.2f}s".format(
      num_entries, time.time() - start_time))
    return word_dict

  def ReadTranAuxTSV(self, input_path, aux_trans):
    start_time = time.time()
    logger.info("Reading a translation aux file: input_path={}".format(input_path))
    num_entries = 0
    with open(input_path) as input_file:
      for line in input_file:
        fields = line.strip().split("\t")
        if len(fields) < 2: continue
        word = self.NormalizeText(fields[0])
        values = aux_trans.get(word) or []
        uniq_trans = set()
        for tran in fields[1:]:
          tran = self.NormalizeText(tran)
          tran = regex.sub(r"[\p{Ps}\p{Pe}\p{C}]", "", tran)
          tran = regex.sub(r"[\p{Z}\p{C}]+", " ", tran).strip()
          tran = self.tokenizer.NormalizeJaWordStyle(tran)
          norm_tran = tkrzw_dict.NormalizeWord(tran)
          if not tran or not norm_tran: continue
          if regex.search(r"\p{Latin}.*の.*(形|分詞|級)", tran): continue
          if norm_tran in uniq_trans: continue
          uniq_trans.add(norm_tran)
          values.append(tran)
        aux_trans[word] = values
        num_entries += 1
        if num_entries % 10000 == 0:
          logger.info("Reading a translation aux file: num_entries={}".format(num_entries))
    logger.info("Reading a translation aux file: num_entries={}, elapsed_time={:.2f}s".format(
      num_entries, time.time() - start_time))

  def ReadAOAWords(self, input_path, aoa_words):
    start_time = time.time()
    logger.info("Reading a AOA file: input_path={}".format(input_path))
    num_entries = 0
    with open(input_path) as input_file:
      is_first = True
      for line in input_file:
        if is_first:
          is_first = False
          continue
        fields = line.strip().split(",")
        if len(fields) != 7: continue
        word = self.NormalizeText(fields[0]).strip()
        occur = fields[3]
        mean = fields[4]
        stddev = fields[5]
        if not word or not regex.fullmatch(r"[0-9.]+", mean): continue
        if not regex.fullmatch(r"[.0-9]+", occur): continue
        mean = float(mean)
        if regex.fullmatch(r"[0-9.]+", stddev):
          mean += float(stddev)
        else:
          mean += 3.0
        aoa_words[word].append(mean)
        num_entries += 1
        if num_entries % 10000 == 0:
          logger.info("Reading a AOA file: num_entries={}".format(num_entries))
    logger.info("Reading a translation aux file: num_entries={}, elapsed_time={:.2f}s".format(
      num_entries, time.time() - start_time))

  def ReadKeywords(self, input_path, keywords):
    start_time = time.time()
    logger.info("Reading a keyword file: input_path={}".format(input_path))
    num_entries = 0
    with open(input_path) as input_file:
      for line in input_file:
        keyword = self.NormalizeText(line).strip()
        keywords.add(keyword)
        num_entries += 1
        if num_entries % 10000 == 0:
          logger.info("Reading a keyword file: num_entries={}".format(num_entries))
    logger.info("Reading a keyword file: num_entries={}, elapsed_time={:.2f}s".format(
      num_entries, time.time() - start_time))

  def ReadHints(self, input_path, hints):
    start_time = time.time()
    logger.info("Reading a hint file: input_path={}".format(input_path))
    num_entries = 0
    with open(input_path) as input_file:
      for line in input_file:
        fields = line.strip().split("\t")
        if len(fields) < 3: continue
        surface, poses, prob = fields[:3]
        hints[surface] = (poses, float(prob))
        num_entries += 1
        if num_entries % 10000 == 0:
          logger.info("Reading a hint file: num_entries={}".format(num_entries))
    logger.info("Reading a hint file: num_entries={}, elapsed_time={:.2f}s".format(
      num_entries, time.time() - start_time))

  def ReadSynonyms(self, input_path, synonyms):
    start_time = time.time()
    logger.info("Reading a synonym file: input_path={}".format(input_path))
    num_entries = 0
    with open(input_path) as input_file:
      for line in input_file:
        fields = line.strip().split("\t")
        if len(fields) < 2: continue
        word = fields[0]
        synonyms[word] = fields[1:]
        num_entries += 1
        if num_entries % 10000 == 0:
          logger.info("Reading a synonym file: num_entries={}".format(num_entries))
    logger.info("Reading a synonym file: num_entries={}, elapsed_time={:.2f}s".format(
      num_entries, time.time() - start_time))

  def SaveWords(self, word_dicts, aux_trans, aux_last_trans, aoa_words,
                keywords, hints, extra_synonyms):
    logger.info("Preparing DBMs")
    phrase_prob_dbm = None
    if self.phrase_prob_path:
      phrase_prob_dbm = tkrzw.DBM()
      phrase_prob_dbm.Open(self.phrase_prob_path, False, dbm="HashDBM").OrDie()
    tran_prob_dbm = None
    if self.tran_prob_path:
      tran_prob_dbm = tkrzw.DBM()
      tran_prob_dbm.Open(self.tran_prob_path, False, dbm="HashDBM").OrDie()
    rev_prob_dbm = None
    if self.rev_prob_path:
      rev_prob_dbm = tkrzw.DBM()
      rev_prob_dbm.Open(self.rev_prob_path, False, dbm="HashDBM").OrDie()
    cooc_prob_dbm = None
    if self.cooc_prob_path:
      cooc_prob_dbm = tkrzw.DBM()
      cooc_prob_dbm.Open(self.cooc_prob_path, False, dbm="HashDBM").OrDie()
    start_time = time.time()
    logger.info("Extracting keys")
    keys = set()
    for label, word_dict in word_dicts:
      for key in word_dict.keys():
        if key.find("\t") >= 0: continue
        keys.add(key)
    logger.info("Extracting keys done: num_keys={}, elapsed_time={:.2f}s".format(
      len(keys), time.time() - start_time))
    nmt_probs = self.ReadNMTProbs(keys)
    start_time = time.time()
    logger.info("Indexing stems")
    stem_index = collections.defaultdict(list)
    for label, word_dict in word_dicts:
      if label in self.supplement_labels: continue
      for key in keys:
        for entry in word_dict[key]:
          word = entry["word"]
          if not regex.fullmatch("[a-z]+", word): continue
          stems = self.GetDerivativeStems(label, entry, word_dict, aux_trans, phrase_prob_dbm)
          if stems:
            valid_stems = set()
            for stem in stems:
              if stem in keys:
                stem_index[stem].append(word)
                valid_stems.add(stem)
            if valid_stems:
              entry["stem"] = list(valid_stems.union(set(entry.get("stem") or [])))
    for label, word_dict in word_dicts:
      if label not in self.core_labels: continue
      for key in keys:
        for entry in word_dict[key]:
          word = entry["word"]
          children = stem_index.get(word)
          if children:
            entry["stem_child"] = list(set(children))
    logger.info("Indexing stems done: num_stems={}, elapsed_time={:.2f}s".format(
      len(stem_index), time.time() - start_time))
    start_time = time.time()
    logger.info("Checking POS of words")
    noun_words = set()
    verb_words = set()
    adj_words = set()
    adv_words = set()
    for label, word_dict in word_dicts:
      if label in self.core_labels:
        for key in keys:
          for entry in word_dict[key]:
            word = entry["word"]
            for pos, text in entry["text"]:
              if pos == "noun": noun_words.add(word)
              if pos == "verb": verb_words.add(word)
              if pos == "adjective": adj_words.add(word)
              if pos == "adverb": adv_words.add(word)
    logger.info("Checking POS of words done: elapsed_time={:.2f}s".format(
      time.time() - start_time))
    start_time = time.time()
    logger.info("Indexing base forms")
    infl_dict = collections.defaultdict(list)
    extra_word_bases = {}
    for label, word_dict in word_dicts:
      if label not in self.top_labels: continue
      base_index = collections.defaultdict(list)
      core_index = collections.defaultdict(list)
      for key, entries in word_dict.items():
        for entry in entries:
          word = entry["word"]
          if not regex.fullmatch("[a-z]+", word): continue
          for infl_name in inflection_names:
            for infl in (entry.get(infl_name) or "").split(","):
              infl = infl.strip()
              if infl and infl != word:
                infl_dict[infl].append(word)
          if word in verb_words:
            children = set()
            for part_name in ("verb_present_participle", "verb_past_participle"):
              for part in (entry.get(part_name) or "").split(","):
                part = part.strip()
                if part and part != word and (part in noun_words or part in adj_words):
                  base_index[part].append(word)
                  extra_word_bases[part] = word
                  children.add(part)
            if children:
              entry["base_child"] = list(children)
          if word in adj_words:
            children = set()
            for part_name in ("adjective_comparative", "adjective_superlative"):
              part = entry.get(part_name)
              if part and (part in noun_words or part in adj_words):
                base_index[part].append(word)
                children.add(part)
            if children:
              entry["base_child"] = list(children)
          core = entry.get("etymology_core")
          prefix = entry.get("etymology_prefix")
          suffix = entry.get("etymology_suffix")
          if core and len(core) >= 4 and not prefix and suffix:
            entry["core"] = core
            core_index[core].append(word)
      for key, entries in word_dict.items():
        for entry in entries:
          word = entry["word"]
          if not regex.fullmatch("[a-z]+", word): continue
          bases = base_index.get(word)
          if bases:
            entry["base"] = list(bases)
          children = core_index.get(word)
          if children:
            entry["core_child"] = list(children)
    logger.info("Indexing base forms done: elapsed_time={:.2f}s".format(
      time.time() - start_time))
    start_time = time.time()
    logger.info("Merging entries: num_keys={}".format(len(keys)))
    merged_entries = []
    core_word_probs = {}
    for key in keys:
      merged_entry = self.MergeRecord(
        key, word_dicts, aux_trans, aoa_words, keywords, hints,
        phrase_prob_dbm, tran_prob_dbm, rev_prob_dbm, cooc_prob_dbm)
      if not merged_entry: continue
      merged_entries.append((key, merged_entry))
      if key.find(" ") < 0:
        for word_entry in merged_entry:
          word = word_entry["word"]
          norm_word = tkrzw_dict.NormalizeWord(word)
          prob = float(word_entry.get("probability") or 0)
          if prob >= 0.000001:
            old_prob = core_word_probs.get(norm_word) or 0
            core_word_probs[norm_word] = max(prob, old_prob)
      if len(merged_entries) % 1000 == 0:
        logger.info("Merging entries:: num_entries={}".format(len(merged_entries)))
    logger.info("Making records done: num_records={}, elapsed_time={:.2f}s".format(
      len(merged_entries), time.time() - start_time))
    start_time = time.time()
    logger.info("Extracting phrases")
    live_words = tkrzw.DBM()
    live_words.Open("", True, dbm="BabyDBM").OrDie()
    rev_live_words = tkrzw.DBM()
    rev_live_words.Open("", True, dbm="BabyDBM").OrDie()
    pivot_live_words = collections.defaultdict(list)
    pivot_dead_words = collections.defaultdict(list)
    if phrase_prob_dbm:
      for key, merged_entry in merged_entries:
        for word_entry in merged_entry:
          word = word_entry["word"]
          prob = float(word_entry.get("probability") or 0)
          tokens = word.split(" ")
          value = "{:.8f}".format(prob)
          live_words.Set(word, value).OrDie()
          rev_word = " ".join(reversed(word.split(" ")))
          rev_live_words.Set(rev_word, value).OrDie()
          if len(tokens) > 1 and prob >= 0.000001:
            if (tokens[0].lower() in particles and
                regex.search(r"[A-Z]", tokens[0]) and not regex.search(r"[A-Z]", tokens[1])):
              continue
            min_token_prob = 1
            pivot_token = None
            for token in tokens:
              if token in particles or token in misc_stop_words: continue
              if not regex.fullmatch("[-'\p{Latin}]+", token): continue
              norm_token = tkrzw_dict.NormalizeWord(token)
              token_prob = core_word_probs.get(norm_token) or 0
              if token_prob <= min_token_prob:
                min_token_prob = token_prob
                pivot_token = token
            if pivot_token:
              value = "{}|{:.8f}".format(word, prob)
              pivot_live_words[pivot_token].append(value)
      for word, trans in aux_trans.items():
        key = tkrzw_dict.NormalizeWord(word)
        if key in keys: continue
        if key not in keywords: continue
        if not regex.fullmatch("[-'\p{Latin} ]+", word): continue
        tokens = word.split(" ")
        if len(tokens) < 2 or len(tokens) > 3: continue
        if (tokens[0].lower() in particles and
            regex.search(r"[A-Z]", tokens[0]) and not regex.search(r"[A-Z]", tokens[1])):
          continue
        min_token_prob = 1
        pivot_token = None
        for token in tokens:
          norm_token = tkrzw_dict.NormalizeWord(token)
          if norm_token in particles or norm_token in misc_stop_words: continue
          if norm_token in phrase_wildcards: continue
          if not regex.fullmatch("[-'\p{Latin}]+", token): continue
          token_prob = core_word_probs.get(norm_token) or 0
          norm_token = " ".join(self.tokenizer.Tokenize("en", token, False, True))
          if norm_token != token:
            norm_token_prob = core_word_probs.get(norm_token) or 0
            token_prob = max(token_prob, norm_token_prob)
          if token_prob <= min_token_prob:
            min_token_prob = token_prob
            pivot_token = token
        if not pivot_token or min_token_prob < 0.000001: continue
        prob = self.GetPhraseProb(phrase_prob_dbm, "en", word)
        alt_prob = 0
        alt_phrases = set()
        match = regex.search(r"(.*)(\W)(one's|someone's|something's)(\W)(.*)", word)
        if match:
          for pron in ["my", "our", "your", "its", "his", "her", "their"]:
            alt_phrase = match.group(1) + match.group(2) + pron + match.group(4) + match.group(5)
            alt_phrases.add(alt_phrase)
        else:
          match = regex.search(r"(.*)(\W)(someone|something)(\W)(.*)", word)
          if match:
            for pron in ["me", "us", "you", "it", "him", "her", "them"]:
              alt_phrase = match.group(1) + match.group(2) + pron + match.group(4) + match.group(5)
              alt_phrases.add(alt_phrase)
        for alt_phrase in alt_phrases:
          alt_prob += self.GetPhraseProb(phrase_prob_dbm, "en", alt_phrase)
        prob += min(alt_prob * 4, min_token_prob * 0.05)
        value = "{}|{:.8f}".format(word, prob)
        pivot_dead_words[pivot_token].append(value)
    logger.info("Extracting phrases done: num_records={}:{}:{}:{}, elapsed_time={:.2f}s".format(
      len(live_words), len(rev_live_words), len(pivot_live_words), len(pivot_dead_words),
      time.time() - start_time))
    start_time = time.time()
    logger.info("Modifying entries")
    merged_entries = sorted(merged_entries)
    num_entries = 0
    for key, merged_entry in merged_entries:
      for word_entry in merged_entry:
        word = word_entry["word"]
        entries = []
        for label, word_dict in word_dicts:
          dict_entries = word_dict.get(key)
          if not dict_entries: continue
          for entry in dict_entries:
            if entry["word"] == word:
              entries.append((label, entry))
        self.SetAOA(word_entry, entries, aoa_words, live_words, phrase_prob_dbm)
        self.SetTranslations(word_entry, aux_trans, tran_prob_dbm, rev_prob_dbm,
                             infl_dict, nmt_probs)
        self.SetRelations(word_entry, entries, word_dicts,
                          live_words, rev_live_words, pivot_live_words,
                          phrase_prob_dbm, tran_prob_dbm, cooc_prob_dbm, extra_word_bases,
                          verb_words, adj_words, adv_words, extra_synonyms)
        if phrase_prob_dbm and cooc_prob_dbm:
          self.SetCoocurrences(word_entry, entries, word_dicts, phrase_prob_dbm, cooc_prob_dbm)
      num_entries += 1
      if num_entries % 1000 == 0:
        logger.info("Modifying entries: num_records={}".format(num_entries))
    logger.info("Modifying entries done: elapsed_time={:.2f}s".format(time.time() - start_time))
    start_time = time.time()
    logger.info("Finishing entries")
    merged_dict = {}
    for key, merged_entry in merged_entries:
      merged_dict[key] = merged_entry
    num_entries = 0
    for key, merged_entry in merged_entries:
      for word_entry in merged_entry:
        self.CompensateInflections(word_entry, merged_dict, verb_words)
        self.CompensateAlternatives(word_entry, merged_dict)
        self.PropagateTranslations(word_entry, merged_dict, tran_prob_dbm, aux_last_trans)
      num_entries += 1
      if num_entries % 1000 == 0:
        logger.info("Finishing entries R1: num_records={}".format(num_entries))
    num_entries = 0
    for key, merged_entry in merged_entries:
      rival_key_trans = set()
      for word_id, word_entry in enumerate(merged_entry):
        self.RemoveDuplicatedtranslations(word_entry, rival_key_trans)
        self.SetPhraseTranslations(word_entry, merged_dict, aux_trans, aux_last_trans,
                                   tran_prob_dbm, phrase_prob_dbm, nmt_probs,
                                   noun_words, verb_words,
                                   live_words, rev_live_words, pivot_live_words, pivot_dead_words,
                                   keywords, rival_key_trans)
        self.FilterParents(word_entry, merged_dict)
        self.AbsorbInflections(word_entry, merged_dict)
        if word_id < len(merged_entry) - 1:
          for tran in word_entry.get("translation") or []:
            rival_key_trans.add(tran)
      num_entries += 1
      if num_entries % 1000 == 0:
        logger.info("Finishing entries R2: num_records={}".format(num_entries))
    logger.info("Finishing entries done: elapsed_time={:.2f}s".format(time.time() - start_time))
    rev_live_words.Close().OrDie()
    live_words.Close().OrDie()
    if cooc_prob_dbm:
      cooc_prob_dbm.Close().OrDie()
    if rev_prob_dbm:
      rev_prob_dbm.Close().OrDie()
    if tran_prob_dbm:
      tran_prob_dbm.Close().OrDie()
    if phrase_prob_dbm:
      phrase_prob_dbm.Close().OrDie()
    start_time = time.time()
    logger.info("Saving records: output_path={}".format(self.output_path))
    word_dbm = tkrzw.DBM()
    num_buckets = len(merged_entries) * 2
    word_dbm.Open(self.output_path, True, dbm="HashDBM", truncate=True,
                  align_pow=0, num_buckets=num_buckets)
    num_records = 0
    for key, merged_entry in merged_entries:
      final_entry = []
      for word_entry in merged_entry:
        if word_entry.get("deleted"):
          continue
        for attr_name in list(word_entry.keys()):
          if attr_name.startswith("_"):
            del word_entry[attr_name]
        final_entry.append(word_entry)
      if not final_entry: continue
      serialized = json.dumps(final_entry, separators=(",", ":"), ensure_ascii=False)
      word_dbm.Set(key, serialized)
      num_records += 1
      if num_records % 1000 == 0:
        logger.info("Saving records: num_records={}".format(num_records))
    word_dbm.Close().OrDie()
    logger.info("Saving records done: num_records={}, elapsed_time={:.2f}s".format(
      len(merged_entries), time.time() - start_time))

  def GetDerivativeStems(self, label, entry, word_dict, aux_trans, phrase_prob_dbm):
    word = entry["word"]
    prob = 1.0
    if phrase_prob_dbm:
      prob = self.GetPhraseProb(phrase_prob_dbm, "en", word)
    texts = entry.get("text") or []
    def NormalizeTran(tran):
      tran = tran.strip()
      han_tran = regex.sub(r"[^\p{Han}]", "", tran)
      if len(han_tran) >= 2:
        tran = han_tran
      elif regex.fullmatch(r"\p{Han}\p{Hiragana}+", tran):
        poses = self.tokenizer.GetJaPosList(tran)
        while len(poses) >= 2:
          pos = poses[-1]
          if not regex.fullmatch(r"\p{Hiragana}+", pos[0]): break
          if pos[1] not in ["助詞", "助動詞"] and pos[2] not in ["接尾", "非自立"]: break
          poses = poses[:-1]
        norm_tran = ""
        for pos in poses:
          norm_tran += pos[3]
        if len(norm_tran) >= 2:
          tran = norm_tran
      tran = regex.sub(r"^([\p{Han}]{2,})的$", "\1", tran)
      return tran
    def GetMetadata(in_entry, out_poses, out_deris, out_trans):
      in_word = in_entry["word"]
      for pos, text in in_entry["text"]:
        out_poses.add(pos)
        for part in text.split("[-]"):
          part = part.strip()
          match = regex.search(r"^\[(synonym|derivative)\]: (.*)", part)
          if match:
            expr = regex.sub(r"\[-.*", "", match.group(2))
            for deri in expr.split(","):
              deri = deri.strip()
              if regex.fullmatch("[a-z]+", deri):
                out_deris.add(deri)
          match = regex.search(r"^\[translation\]: (.*)", part)
          if match:
            expr = regex.sub(r"\[-.*", "", match.group(1))
            expr = regex.sub(r"\(.*?\)", "", expr).strip()
            for tran in expr.split(","):
              tran = NormalizeTran(tran)
              if len(tran) >= 2:
                out_trans.add(tran)
        if label in self.gross_labels:
          text = regex.sub(r"\(.*?\)", "", text)
          text = regex.sub(r"（.*?）", "", text)
          for tran in regex.split(r"[,、。]", text):
            tran = NormalizeTran(tran)
            if len(tran) >= 2:
              out_trans.add(tran)
      relation_expr = in_entry.get("relation")
      if relation_expr:
        for rel_word in relation_expr.split(","):
          check_len = max(5, len(rel_word) - 1)
          if rel_word[:check_len] == word[:check_len]:
            out_deris.add(rel_word)
      in_aux_trans = aux_trans.get(in_word)
      if in_aux_trans:
        for tran in in_aux_trans:
          tran = NormalizeTran(tran)
          if len(tran) >= 2:
            out_trans.add(tran)
    poses = set()
    deris = set()
    trans = set()
    GetMetadata(entry, poses, deris, trans)
    deri_probs = {}
    deri_trans = {}
    if phrase_prob_dbm:
      for deri in deris:
        if deri[:3] != word[:3]: continue
        deri_probs[deri] = self.GetPhraseProb(phrase_prob_dbm, "en", deri)
        for deri_entry in word_dict.get(deri) or []:
          if deri_entry["word"] == deri:
            one_deri_poses = set()
            one_deri_deris = set()
            one_deri_trans = set()
            GetMetadata(deri_entry, one_deri_poses, one_deri_deris, one_deri_trans)
            if one_deri_trans:
              deri_trans[deri] = one_deri_trans
    stems = set()
    for pos in poses:
      for rule_pos, suffixes in (
          ("noun", noun_suffixes),
          ("verb", verb_suffixes),
          ("adjective", adjective_suffixes),
          ("adverb", adverb_suffixes)):
        if pos == rule_pos:
          for suffix in suffixes:
            if word.endswith(suffix):
              stem = word[:-len(suffix)]
              if len(stem) >= 2:
                stems.add(stem)
                if len(suffix) >= 2 and stem[-1] == suffix[0]:
                  stems.add(stem + suffix[0])
                if len(suffix) >= 2 and stem[-1] == "i":
                  stems.add(stem[:-1] + "y")
                if len(suffix) >= 2 and suffix[0] == "i":
                  stems.add(stem + "e")
                if len(suffix) >= 2 and suffix[0] == "e":
                  stems.add(stem + "e")
                if len(suffix) >= 2 and suffix[0] == "t":
                  stems.add(stem + "t")
                if len(suffix) >= 3 and suffix[0] == "s":
                  stems.add(stem + "s")
                if suffix == "al" and len(stem) >= 3:
                  stems.add(stem + "es")
                  stems.add(stem + "e")
                if suffix == "y" and len(stem) >= 3:
                  stems.add(stem + "e")
                if suffix in ["tion", "sion"] and len(stem) >= 2:
                  stems.add(stem + "e")
                  stems.add(stem + "d")
                  stems.add(stem + "t")
                  stems.add(stem + "s")
                  stems.add(stem + "te")
                  stems.add(stem + "de")
                  stems.add(stem + "se")
                  stems.add(stem + "ve")
                if suffix in ["tion", "sion"] and len(stem) >= 3 and stem.endswith("a"):
                  stems.add(stem[:-1])
                  stems.add(stem[:-1] + "e")
                  stems.add(stem[:-1] + "ate")
                if suffix in ["tion", "sion"] and len(stem) >= 3 and stem.endswith("u"):
                  stems.add(stem[:-1] + "ve")
                if suffix == "sion" and len(stem) >= 3 and stem.endswith("s"):
                  stems.add(stem[:-1] + "t")
                if suffix in ["ible", "able"] and len(stem) >= 2:
                  stems.add(stem + "or")
                  stems.add(stem + "er")
                  stems.add(stem + "ify")
                  stems.add(stem + "y")
                if suffix == "ate":
                  stems.add(stem + "e")
                if suffix == "al" and len(stem) >= 3 and stem.endswith("r"):
                  stems.add(stem[:-1] + "er")
                if suffix == "ive" and len(stem) >= 3 and stem.endswith("s"):
                  stems.add(stem[:-1] + "d")
                  stems.add(stem[:-1] + "de")
                if suffix == "ic" and len(stem) >= 3:
                  stems.add(stem + "y")
                if suffix == "ize" and len(stem) >= 3:
                  stems.add(stem + "y")
                if suffix == "ity" and len(stem) >= 6 and stem.endswith("bil"):
                  stems.add(stem[:-3] + "ble")
                if suffix == "pt" and len(stem) >= 3:
                  stems.add(stem[:-1] + "ve")
                if suffix == "ce" and len(stem) >= 3:
                  stems.add(stem + "t")
                  stems.add(stem + "d")
                  stems.add(stem + "se")
                if suffix == "ian" and len(stem) >= 4:
                  stems.add(stem + "y")
                if suffix == "cy" and len(stem) >= 4:
                  stems.add(stem + "t")
                if suffix == "faction" and len(stem) >= 4:
                  stems.add(stem + "fy")
                if suffix == "ous" and len(stem) >= 4:
                  stems.add(stem + "on")
                  stems.add(stem + "y")
                  stems.add(stem + "e")
                if suffix == "ous" and len(stem) >= 5 and stem.endswith("ul"):
                  stems.add(stem[:-2] + "le")
                if suffix == "ant" and len(stem) >= 4:
                  stems.add(stem + "ate")
                  stems.add(stem + "e")
                if suffix == "ative" and len(stem) >= 4:
                  stems.add(stem + "e")
                if suffix in ["er", "or", "ive"] and len(stem) >= 5:
                  stems.add(stem + "e")
                if len(stem) >= 3 and stem.endswith("u"):
                  stems.add(stem + "e")
                if len(stem) >= 4 and stem.endswith("i"):
                  stems.add(stem[:-1] + "e")
                if len(stem) >= 4 and stem.endswith("rr"):
                  stems.add(stem[:-1])
                if len(stem) >= 5 and stem.endswith("t"):
                  stems.add(stem[:-1] + "ce")
                  stems.add(stem[:-1] + "d")
                if len(stem) >= 5 and stem.endswith("v"):
                  stems.add(stem + "e")
                if len(stem) >= 8 and stem.endswith("tic"):
                  stems.add(stem + "s")
                if len(stem) >= 4 and stem[-1] == stem[-2]:
                  stems.add(stem[:-1])
    stems.discard(word)
    valid_stems = set()
    for pos, text in texts:
      match = regex.search(
        r'^[" ]*([\p{Latin}]+)[" ]*の(複数形|三人称|動名詞|現在分詞|過去形|過去分詞)', text)
      if match:
        stem = match.group(1)
        if len(stem) >= 4 and word.startswith(stem):
          valid_stems.add(stem)
    for stem in stems:
      if phrase_prob_dbm:
        stem_prob = self.GetPhraseProb(phrase_prob_dbm, "en", stem)
      else:
        stem_prob = prob
      stem_prob_ratio = stem_prob / prob
      if label not in self.core_labels and stem_prob_ratio < 0.1: continue
      if (stem.find(" ") < 0 and len(stem) >= 8 and len(stem) < len(word) and
          stem_prob_ratio >= 0.5):
        valid_stems.add(stem)
        continue
      stem_entry = None
      for tmp_stem_entry in word_dict.get(stem) or []:
        if tmp_stem_entry["word"] == stem:
          stem_entry = tmp_stem_entry
      is_known = False
      if stem_prob_ratio >= 0.001:
        is_known = True
      if stem_entry and word in (stem_entry.get("related") or []):
        is_known = True
      if len(stem) >= 6 and stem_prob_ratio >= 0.0002:
        is_known = True
      stem_poses = set()
      stem_deris = set()
      stem_trans = set()
      if stem_entry:
        GetMetadata(stem_entry, stem_poses, stem_deris, stem_trans)
      if stem.find(" ") < 0 and len(stem) >= 4 and trans:
        hit_deri = False
        if word in stem_deris:
          hit_deri = True
        hit_tran = False
        for stem_tran in stem_trans:
          if stem_tran in trans:
            hit_tran = True
          if regex.search(r"\p{Han}", stem_tran):
            for tran in trans:
              if tran.find(stem_tran) >= 0 or stem_tran.find(tran) >= 0:
                hit_tran = True
        if ((hit_deri and hit_tran) or (stem_prob_ratio >= 0.1 and hit_deri) or
            (is_known and hit_tran)):
          valid_stems.add(stem)
      check_len = max(3, len(stem) - 2)
      for deri in deris:
        if len(word) < len(deri):
          continue
        deri_prob = deri_probs.get(deri) or 0.0
        deri_prob_ratio = deri_prob / prob
        hit_deri = False
        if deri == stem:
          hit_deri = True
        if stem[:check_len] == deri[:check_len] and len(stem) >= 4:
          prefix = deri[:len(stem)]
          if prefix == stem:
            hit_deri = True
          if len(prefix) >= 6 and tkrzw.Utility.EditDistanceLev(stem, prefix) < 2:
            hit_deri = True
        hit_tran = False
        for deri_tran in deri_trans.get(deri) or []:
          if deri_tran in trans:
            hit_tran = True
          if regex.search(r"\p{Han}", deri_tran):
            for tran in trans:
              if tran.find(deri_tran) >= 0:
                hit_tran = True
        if hit_deri and (deri_prob_ratio >= 0.1 or hit_tran):
          valid_stems.add(deri)
    force_parent = force_parents.get(word)
    if force_parent:
      valid_stems.clear()
      valid_stems.add(force_parent)
    valid_stems.discard(word)
    return list(valid_stems)

  def ReadNMTProbs(self, keys):
    nmt_probs = {}
    if not self.nmt_prob_path: return nmt_probs
    logger.info("Reading NMT probs: path={}".format(self.nmt_prob_path))
    num_probs = 0
    with open(self.nmt_prob_path) as input_file:
      for line in input_file:
        fields = line.strip().split("\t")
        if len(fields) < 3: continue
        word = fields[0]
        if word not in keys: continue
        tran_probs = {}
        for i in range(1, len(fields), 2):
          tran = fields[i]
          prob = float(fields[i + 1]) * 0.3
          if prob > 0.02:
            tran_probs[tran] = prob
        if tran_probs:
          nmt_probs[word] = tran_probs
        num_probs += 1
        if num_probs % 10000 == 0:
          logger.info("Reading NMT probs: hints={}".format(num_probs))
    logger.info("Reading NMT probs done: records={}".format(len(nmt_probs)))
    return nmt_probs

  def MergeRecord(self, key, word_dicts, aux_trans, aoa_words, keywords, hints,
                  phrase_prob_dbm, tran_prob_dbm, rev_prob_dbm, cooc_prob_dbm):
    word_entries = {}
    word_shares = collections.defaultdict(float)
    word_trans = collections.defaultdict(set)
    entry_tran_texts = collections.defaultdict(list)
    num_words = 0
    poses = collections.defaultdict(set)
    synonyms = collections.defaultdict(set)
    core = None
    for label, word_dict in word_dicts:
      dict_entries = word_dict.get(key)
      if not dict_entries: continue
      for entry in dict_entries:
        num_words += 1
        word = entry["word"]
        entries = word_entries.get(word) or []
        entries.append((label, entry))
        word_entries[word] = entries
        texts = entry.get("text")
        if texts:
          text_score = len(texts) * 1.0
          for pos, text in texts:
            poses[word].add(pos)
            trans = self.ExtractTextLabelTrans(text)
            if trans:
              text_score += 0.5
              word_trans[word].update(trans)
          word_shares[word] += math.log2(1 + text_score)
        expr = entry.get("synonym")
        if expr:
          for synonym in regex.split(r"[,;]", expr):
            synonym = synonym.strip()
            if regex.search(r"\p{Latin}", synonym) and synonym.lower() != word.lower():
              synonyms[word].add(synonym)
        if not core:
          core = entry.get("core")
      dict_entries = word_dict.get(key + "\ttranslation")
      if dict_entries:
        for entry in dict_entries:
          word = entry["word"]
          tran_texts = entry.get("text")
          if not tran_texts: continue
          for tran_pos, tran_text in tran_texts:
            tran_key = word + "\t" + label + "\t" + tran_pos
            entry_tran_texts[tran_key].append(tran_text)
            trans = self.ExtractTextLabelTrans(tran_text)
            if trans:
              word_trans[word].update(trans)
    sorted_word_shares = sorted(word_shares.items(), key=lambda x: x[1], reverse=True)
    if len(sorted_word_shares) > 1 and aux_trans and tran_prob_dbm:
      spell_ratios = {}
      if phrase_prob_dbm:
        word_probs = {}
        for word, share in sorted_word_shares:
          if word in word_probs: continue
          prob = self.GetPhraseProb(phrase_prob_dbm, "en", word)
          if not regex.search(r"\p{Lu}", word):
            prob *= 1.1
          word_probs[word] = prob
        sum_prob = sum([x[1] for x in word_probs.items()])
        for word, prob in word_probs.items():
          spell_ratios[word] = prob / sum_prob
      word_scores = []
      for word, share in sorted_word_shares:
        score = 0.0
        if word in keywords:
          score += 0.1
        cap_aux_trans = aux_trans.get(word) or []
        if cap_aux_trans:
          score += 0.1
        cap_word_trans = word_trans.get(word) or []
        cap_trans = set(cap_aux_trans).union(cap_word_trans)
        tran_score = 0.0
        if cap_trans:
          key = tkrzw_dict.NormalizeWord(word)
          tsv = tran_prob_dbm.GetStr(key)
          if tsv:
            fields = tsv.split("\t")
            max_prob = 0.0
            sum_prob = 0.0
            for i in range(0, len(fields), 3):
              src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
              if src != word:
                prob *= 0.1
              if not regex.search(r"[\p{Han}\{Hiragana}]", trg):
                prob *= 0.5
                if regex.search(r"\p{Lu}", src):
                  prob *= 0.5
              if trg in cap_trans:
                max_prob = max(max_prob, prob)
                sum_prob += prob
            tran_score += (sum_prob * max_prob) ** 0.5
        spell_score = (spell_ratios.get(word) or 0.0) * 0.5
        score += ((tran_score + 0.05) * (share + 0.05) * (spell_score + 0.05)) ** (1 / 3)
        word_scores.append((word, score))
      sorted_word_shares = sorted(word_scores, key=lambda x: x[1], reverse=True)
    share_sum = sum([x[1] for x in sorted_word_shares])
    merged_entry = []
    for word, share in sorted_word_shares:
      entries = word_entries[word]
      word_entry = {}
      word_entry["word"] = word
      stem = " ".join(self.tokenizer.Tokenize("en", word, False, True))
      effective_labels = set()
      surfaces = set([word.lower()])
      is_keyword = (word in aux_trans or word in aoa_words or word in keywords or
                    (core and core in keywords))
      has_pronunciation = False
      has_translation = False
      for label, entry in entries:
        if "pronunciation" in entry:
          has_pronunciation = True
        texts = entry.get("text")
        if texts:
          for pos, text in texts:
            if text.find("[translation]:") >= 0:
              has_translation = True
      if word.count(" ") == 0 and len(entries) >= 2:
        is_keyword = True
      word_poses = poses[word]
      for pos in word_poses:
        for rule_pos, suffixes in (
            ("noun", noun_suffixes),
            ("verb", verb_suffixes),
            ("adjective", adjective_suffixes),
            ("adverb", adverb_suffixes)):
          if pos == rule_pos:
            for suffix in suffixes:
              if word.endswith(suffix):
                pos_stem = word[:-len(suffix)]
                if len(pos_stem) >= 4:
                  pos_stems = set()
                  pos_stems.add(pos_stem)
                  pos_stems.add(regex.sub(r"i$", r"y", pos_stem))
                  for pos_stem in pos_stems:
                    if pos_stem in aux_trans or pos_stem in aoa_words or pos_stem in keywords:
                      is_keyword = True
                      break
      if not is_keyword and "verb" in word_poses and regex.fullmatch(r"[a-z ]+", word):
        tokens = self.tokenizer.Tokenize("en", word, False, False)
        if len(tokens) >= 2 and tokens[0] in keywords:
          particle_suffix = True
          for token in tokens[1:]:
            if not token in particles:
              particle_suffix = False
              break
          if particle_suffix:
            is_keyword = True
      if not is_keyword:
        for label, entry in entries:
          core_word = entry.get('etymology_core')
          if core_word and core_word in keywords:
            is_keyword = True
      is_super_keyword = is_keyword and bool(regex.fullmatch(r"\p{Latin}{3,}", word))
      for label, entry in entries:
        if label not in self.surfeit_labels or is_keyword:
          effective_labels.add(label)
        for top_name in top_names:
          if label not in self.top_labels and top_name in word_entry: continue
          value = entry.get(top_name)
          if value:
            value = unicodedata.normalize('NFKC', value)
            if top_name in inflection_names:
              infl_words = []
              for infl_word in value.split(','):
                infl_word = infl_word.strip()
                if not infl_word: continue
                if infl_word in infl_words: continue
                infl_words.append(infl_word)
              old_value = word_entry.get(top_name)
              if old_value:
                for infl_word in old_value.split(','):
                  infl_word = infl_word.strip()
                  if not infl_word: continue
                  if infl_word in infl_words: continue
                  infl_words.append(infl_word)
              value = ", ".join(infl_words)
            word_entry[top_name] = value
        for infl_name in inflection_names:
          value = entry.get(infl_name)
          if value:
            for infl_word in value.split(','):
              infl_word = infl_word.strip()
              if not infl_word: continue
              surfaces.add(infl_word.lower())
      if merged_entry and not effective_labels:
        continue
      for label, entry in entries:
        texts = entry.get("text")
        if not texts: continue
        has_good_text = False
        for pos, text in texts:
          pure_text = regex.sub(r"[^\p{Latin}\p{Han}\p{Hiragana}\p{Katakana}\d]", "", text)
          if not pure_text or pure_text == stem:
            continue
          has_good_text = True
        if not has_good_text:
          continue
        for pos, text in texts:
          items = word_entry.get("item") or []
          tran_key = word + "\t" + label + "\t" + pos
          sections = []
          for section in text.split(" [-] "):
            if not sections:
              sections.append(section)
              continue
            eg_match = regex.search(r"^e\.g\.: (.*)", section)
            if eg_match:
              eg_text = eg_match.group(1).lower()
              eg_words = regex.findall("[-\p{Latin}]+", eg_text)
              hit = False
              for surface in surfaces:
                if surface in eg_words:
                  hit = True
                  break
              if not hit: continue
            sections.append(section)
          text = " [-] ".join(sections)
          tran_texts = entry_tran_texts.get(tran_key)
          if tran_texts:
            del entry_tran_texts[tran_key]
            for tran_text in tran_texts:
              tran_item = {"label": label, "pos": pos, "text": tran_text}
              items.append(tran_item)
          item = {"label": label, "pos": pos, "text": text}
          items.append(item)
          word_entry["item"] = items
      if "item" not in word_entry:
        continue
      num_eff_items = 0
      for item in word_entry["item"]:
        text = item["text"]
        if regex.search(r" (of|for) +\"", text) and len(text) < 50:
          continue
        if (regex.search(r"\p{Latin}.*の.*(単数|複数|現在|過去|比較|最上).*(形|級|分詞)", text) and
            len(text) < 30):
          continue
        num_eff_items += 1
      if num_eff_items == 0:
        continue
      prob = None
      if phrase_prob_dbm:
        prob = self.GetPhraseProb(phrase_prob_dbm, "en", word)
        if stem.lower() != word.lower():
          if word.endswith("ics"):
            prob *= 1.1
          elif word.count(" "):
            prob *= 0.5
          else:
            prob *= 0.1
        if "verb" in word_poses:
          tokens = word.split(" ")
          if len(tokens) == 2 and tokens[-1] in particles:
            for pronoun in obj_pronouns:
              phrase = " ".join(tokens[0:-1]) + " " + pronoun + " " + tokens[-1]
              pron_prob = self.GetPhraseProb(phrase_prob_dbm, "en", phrase)
              if pron_prob > 0.0000001:
                prob += pron_prob
        hint = hints.get(word)
        if hint:
          hint_prob = hint[1]
          if hint_prob > prob:
            prob = (prob + hint_prob) / 2
        word_entry["probability"] = "{:.7f}".format(prob).replace("0.", ".")
        if self.min_prob_map:
          has_good_label = False
          for item in word_entry["item"]:
            if item["label"] not in self.min_prob_map:
              has_good_label = True
              break
          if not has_good_label:
            new_items = []
            for item in word_entry["item"]:
              is_good_item = True
              for label, min_prob in self.min_prob_map.items():
                if item["label"] == label:
                  if is_keyword:
                    min_prob *= 0.1
                  if is_super_keyword:
                    norm_text = tkrzw_dict.NormalizeWord(item["text"])
                    norm_text = regex.sub(r"^(to|a|an|the) +([\p{Latin}])", r"\2", norm_text)
                    dist = tkrzw.Utility.EditDistanceLev(key, norm_text)
                    dist /= max(len(key), len(norm_text))
                    if dist > 0.5 or word in aux_trans or (core and core in aux_trans):
                      min_prob = 0.0
                  if prob < min_prob:
                    is_good_item = False
              if is_good_item:
                new_items.append(item)
            word_entry["item"] = new_items
      if not word_entry.get("item"):
        continue
      share_ratio = share / share_sum
      if share_ratio < 1:
        word_entry["share"] = "{:.3f}".format(share_ratio).replace("0.", ".")
      uniq_alternatives = set()
      scored_alternatives = []
      for label, entry in entries:
        alternatives = entry.get("alternative")
        if alternatives:
          for alternative in alternatives:
            norm_alt = tkrzw_dict.NormalizeWord(alternative)
            if norm_alt == key: continue
            if label not in self.core_labels:
              dist = tkrzw.Utility.EditDistanceLev(key, norm_alt)
              dist_ratio = dist / max(len(key), len(norm_alt))
              if dist > 4 or dist_ratio > 0.3: continue
            if alternative not in uniq_alternatives:
              alt_prob = self.GetPhraseProb(phrase_prob_dbm, "en", alternative)
              scored_alternatives.append((alternative, alt_prob))
              uniq_alternatives.add(alternative)
      if scored_alternatives:
        scored_alternatives = sorted(scored_alternatives, key=lambda x: x[1], reverse=True)
        word_entry["alternative"] = [x[0] for x in scored_alternatives]
      word_synonyms = synonyms[word]
      if word_synonyms:
        word_entry["_synonym"] = list(word_synonyms)
      merged_entry.append(word_entry)
    if len(merged_entry) > 1:
      merged_entry = sorted(merged_entry, key=lambda x: float(x.get("share") or 0), reverse=True)
      top_entry = merged_entry[0]
      other_entries = []
      other_items = collections.defaultdict(list)
      for word_entry in merged_entry[1:]:
        labels = set()
        for item in word_entry["item"]:
          labels.add(item["label"])
        share = float(entry.get("share") or 0)
        if len(labels) < 2 and share < 0.3:
          for item in word_entry["item"]:
            label = item["label"]
            if label in self.supplement_labels: continue
            text = item["text"]
            if label.startswith("["): continue
            text = regex.sub(r"^\(.*?\)", "", text).strip()
            if text:
              item["text"] = "(" + word_entry["word"] + ") " + text
              other_items[label].append(item)
        else:
          other_entries.append(word_entry)
      if other_items:
        top_items = []
        uniq_labels = set()
        for label, _ in self.input_confs:
          if label in uniq_labels: continue
          uniq_labels.add(label)
          for item in top_entry["item"]:
            if item["label"] != label: continue
            top_items.append(item)
          for item in other_items.get(label) or []:
            top_items.append(item)
        top_entry["item"] = top_items
      merged_entry = [top_entry] + other_entries
    return merged_entry

  def GetPhraseProb(self, prob_dbm, language, word):
    base_prob = 0.000000001
    tokens = self.tokenizer.Tokenize(language, word, False, True)
    if not tokens: return base_prob
    max_ngram = min(3, len(tokens))
    fallback_penalty = 1.0
    for ngram in range(max_ngram, 0, -1):
      if len(tokens) <= ngram:
        cur_phrase = " ".join(tokens)
        prob = float(prob_dbm.GetStr(cur_phrase) or 0.0)
        if prob:
          return max(prob, base_prob)
        fallback_penalty *= 0.1
      else:
        probs = []
        index = 0
        miss = False
        while index <= len(tokens) - ngram:
          cur_phrase = " ".join(tokens[index:index + ngram])
          cur_prob = float(prob_dbm.GetStr(cur_phrase) or 0.0)
          if not cur_prob:
            miss = True
            break
          probs.append(cur_prob)
          index += 1
        if not miss:
          inv_sum = 0
          for cur_prob in probs:
            inv_sum += 1 / cur_prob
          prob = len(probs) / inv_sum
          prob *= 0.3 ** (len(tokens) - ngram)
          prob *= fallback_penalty
          return max(prob, base_prob)
        fallback_penalty *= 0.1
    return base_prob

  def SetAOA(self, word_entry, entries, aoa_words, live_words, phrase_prob_dbm):
    word = word_entry["word"]
    phrase_prob = min(float(word_entry.get("probability") or 0), 0.0000001)
    share = float(word_entry.get("share") or 1)
    share_bias = 0.0
    if share < 0.5:
      share_bias = (0.5 - share) * 4
    aoa = aoa_words.get(word)
    if aoa:
      aoa += share_bias
      word_entry["aoa"] = "{:.3f}".format(aoa)
    concepts = set()
    for label, entry in entries:
      stems = entry.get("stem")
      if stems:
        for stem in stems:
          concepts.add(stem)
      core = entry.get("core")
      if core:
        concepts.add(core)
    min_aoa = sys.maxsize
    for concept in concepts:
      if not live_words.Get(concept):
        continue
      aoa = aoa_words.get(concept)
      if aoa:
        if phrase_prob and phrase_prob_dbm:
          concept_prob = self.GetPhraseProb(phrase_prob_dbm, "en", concept)
          diff = max(math.log(concept_prob) - math.log(phrase_prob), 0.0)
          aoa += min(diff * 1.0, 1.0)
        else:
          aoa += 1.0
        min_aoa = min(min_aoa, aoa)
    if min_aoa < sys.maxsize:
      min_aoa += share_bias
      word_entry["aoa_concept"] = "{:.3f}".format(min_aoa)
    bases = set()
    for label, entry in entries:
      tmp_bases = entry.get("base")
      if tmp_bases:
        for base in tmp_bases:
          bases.add(base)
    stem = " ".join(self.tokenizer.Tokenize("en", word, False, True))
    if stem != word:
      bases.add(stem)
    min_aoa = sys.maxsize
    for base in bases:
      if not live_words.Get(base):
        continue
      aoa = aoa_words.get(base)
      if aoa:
        aoa += 1.0
        min_aoa = min(min_aoa, aoa)
    if min_aoa < sys.maxsize:
      min_aoa += share_bias
      word_entry["aoa_base"] = "{:.3f}".format(min_aoa)

  def ExtractTextLabelTrans(self, text):
    trans = []
    match = regex.search(r"\[translation\]: ", text)
    if match:
      text = text[match.end():]
      text = regex.sub(r"\[-.*", "", text)
      text = regex.sub(r"\(.*?\)", "", text)
      for tran in text.split(","):
        tran = unicodedata.normalize('NFKC', tran)
        tran = tran.strip()
        tran = regex.sub(r"[\p{S}\p{P}]+ *(が|の|を|に|へ|と|より|から|で|や)", "", tran)
        tran = regex.sub(r"[～\p{S}\p{P}]", " ", tran)
        tran = regex.sub(r" +(?=[\p{Han}\p{Hiragana}\p{Katakana}ー])", "", tran)
        tran = regex.sub(r"[\p{Z}]+", " ", tran).strip()
        if tran:
          trans.append(tran)
    return trans

  def SetTranslations(self, entry, aux_trans, tran_prob_dbm, rev_prob_dbm, infl_dict, nmt_probs):
    word = entry["word"]
    tran_probs = {}
    if tran_prob_dbm:
      key = tkrzw_dict.NormalizeWord(word)
      sum_tran_probs = collections.defaultdict(float)
      tsv = tran_prob_dbm.GetStr(key)
      if tsv:
        fields = tsv.split("\t")
        for i in range(0, len(fields), 3):
          src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
          if src != word:
            prob *= 0.1
          sum_tran_probs[trg] += prob
      nmt_tran_probs = nmt_probs.get(word)
      if nmt_tran_probs:
        for trg, prog in nmt_tran_probs.items():
          sum_tran_probs[trg] += prog
      extra_records = []
      for trg, prob in sum_tran_probs.items():
        if regex.search("[っん]$", trg) and self.tokenizer.GetJaLastPos(trg)[1] == "動詞":
          continue
        norm_trg = tkrzw_dict.NormalizeWord(trg)
        if tkrzw_dict.IsStopWord("ja", norm_trg):
          prob *= 0.7
        elif len(norm_trg) < 2:
          prob *= 0.9
        prob **= 0.8
        tran_probs[norm_trg] = max(tran_probs.get(norm_trg) or 0.0, prob)
        stem_trg = regex.sub(
          r"([\p{Han}\p{Katakana}ー]{2,})(する|すること|される|されること|をする)$",
          r"\1", norm_trg)
        if stem_trg != norm_trg:
          extra_records.append((stem_trg, prob * 0.5))
        stem_trg = self.tokenizer.CutJaWordNounParticle(norm_trg)
        if stem_trg != norm_trg:
          extra_records.append((stem_trg, prob * 0.5))
        stem_trg = regex.sub(r"([\p{Han}\p{Katakana}ー]{2,})(的|的な|的に)$", r"\1", norm_trg)
        if stem_trg != norm_trg:
          extra_records.append((stem_trg, prob * 0.5))
        if self.tokenizer.IsJaWordSahenNoun(norm_trg):
          long_trg = norm_trg + "する"
          extra_records.append((long_trg, prob * 0.5))
      for extra_trg, extra_prob in extra_records:
        tran_probs[extra_trg] = max(tran_probs.get(extra_trg) or 0.0, extra_prob)
    word_aux_trans = aux_trans.get(word)
    count_aux_trans = {}
    if word_aux_trans:
      for aux_tran in word_aux_trans:
        count = (count_aux_trans.get(aux_tran) or 0) + 1
        count_aux_trans[aux_tran] = count
      aux_weight = 1.0
      extra_records = []
      for aux_tran, count in count_aux_trans.items():
        aux_score = (0.01 ** (1 / (count + 1))) * aux_weight
        prob = (tran_probs.get(aux_tran) or 0) + aux_score
        tran_probs[aux_tran] = prob
        stem_tran = regex.sub(
          r"([\p{Han}\p{Katakana}ー]{2,})(する|すること|される|されること|をする)$",
          r"\1", aux_tran)
        if stem_tran != aux_tran:
          extra_records.append((stem_tran, aux_score * 0.5))
        stem_tran = self.tokenizer.CutJaWordNounParticle(aux_tran)
        if stem_tran != aux_tran:
          extra_records.append((stem_tran, aux_score * 0.5))
        stem_tran = regex.sub(r"([\p{Han}\p{Katakana}ー]{2,})(的|的な|的に)$", r"\1", aux_tran)
        if stem_tran != aux_tran:
          extra_records.append((stem_tran, aux_score * 0.5))
        if self.tokenizer.IsJaWordSahenNoun(aux_tran):
          long_tran = aux_tran + "する"
          extra_records.append((long_tran, aux_score * 0.5))
        aux_weight *= 0.9
      for extra_tran, extra_prob in extra_records:
        tran_probs[extra_tran] = max(tran_probs.get(extra_tran) or 0.0, extra_prob)
    translations = {}
    tran_labels = {}
    def Vote(tran, weight, label):
      if regex.search(r"^(noun|verb|adj|adv|[0-9])[^\p{Latin}]", tran):
        return
      norm_tran = tkrzw_dict.NormalizeWord(tran)
      score = 0.00001
      if rev_prob_dbm:
        prob = self.GetPhraseProb(rev_prob_dbm, "ja", tran)
        prob = max(prob, 0.0000001)
        prob = math.exp(-abs(math.log(0.001) - math.log(prob))) * 0.1
        if tkrzw_dict.IsStopWord("ja", tran) or tran in ("又は"):
          prob *= 0.5
        score += prob
      score *= weight
      old_score = translations.get(tran) or 0.0
      translations[tran] = max(old_score, score)
      if label:
        old_labels = tran_labels.get(norm_tran) or set()
        old_labels.add(label)
        tran_labels[norm_tran] = old_labels
    body_weight = 1.0
    tran_weight = 0.7
    for item in entry["item"]:
      label = item["label"]
      pos = item["pos"]
      sections = item["text"].split(" [-] ")
      text = sections[0]
      text = regex.sub(r"。 *(また|または|又は)、.*?。", r"。", text)
      if (label in self.gross_labels and
          regex.search(r"[\p{Han}\p{Hiragana}\p{Katakana}ー]", text)):
        weight = body_weight
        body_weight *= 0.9
        if regex.search(r"[\(（《〔\{\(]([^)）》〕\}\]]+[・、])?" +
                        r"(俗|俗語|スラング|卑|卑語|隠語|古|古語|廃|廃用|廃語)+[)）》〕\}\]]",
                        text):
          weight *= 0.1
        text = regex.sub(r"[\(（《〔\{\(].*?[)）》〕\}\]]", "〜", text)
        text = regex.sub(r"[･・]", "", text)
        text = regex.sub(r"\p{Z}+", " ", text).strip()
        if regex.search(
            r"の(直接法|直説法|仮定法)?(現在|過去)?(第?[一二三]人称)?[ ・･、]?" +
            r"(単数|複数|現在|過去|比較|最上|進行|完了|動名詞|単純|縮約)+[ ・･、]?" +
            r"(形|型|分詞|級|動名詞|名詞|動詞|形容詞|副詞)+", text):
          continue
        if regex.search(r"の(直接法|直説法|仮定法)(現在|過去)", text):
          continue
        if regex.search(r"の(動名詞|異綴|異体|異形|古語|略|省略|短縮|頭字語)", text):
          continue
        if regex.search(r"その他、[^。、]{12,}", text):
          continue
        text = regex.sub(r" \[-+\] .*", "", text).strip()
        text = regex.sub(r" -+ .*", "", text).strip()
        text = regex.sub(r"。$", "", text).strip()
        text_segments = regex.split("[。|、|；|,|;]", text)
        max_tokens = 5 if len(text_segments) == 1 else 4
        for tran in text_segments:
          if len(translations) > 1:
            if tran in ("また", "または", "又は", "しばしば"):
              continue
          if regex.search(r"^[ \p{Latin}]+〜", tran):
            continue
          if regex.search(r"[\p{P}]", tran):
            continue
          tran = regex.sub(r"^[\p{S}\p{P}]+ *(が|の|を|に|へ|と|より|から|で|や)", "", tran)
          tran = regex.sub(r"[～〜]", "", tran)
          tokens = self.tokenizer.Tokenize("ja", tran, False, False)
          if len(tokens) > max_tokens:
            break
          if regex.search(r"^[ \p{Latin}]+ *など", tran):
            continue
          if regex.search(r"[\p{Latin}].*の.*(詞形|綴り)$", tran):
            continue
          tran = " ".join(tokens)
          tran = regex.sub(r"([\p{Han}\p{Hiragana}\p{Katakana}ー]) +", r"\1", tran)
          tran = regex.sub(r" +([\p{Han}\p{Hiragana}\p{Katakana}ー])", r"\1", tran)
          tran = regex.sub(r"[\p{Z}]+", " ", tran).strip()
          if tran:
            Vote(tran, weight, label)
            weight *= 0.8
      if label in self.tran_list_labels:
        for section in sections:
          trans = self.ExtractTextLabelTrans(section)
          if not trans: continue
          weight = tran_weight
          tran_weight *= 0.9
          uniq_trans = set()
          for tran in trans:
            norm_tran = self.tokenizer.NormalizeJaWordForPos(pos, tran)
            if norm_tran and norm_tran not in uniq_trans:
              Vote(norm_tran, weight, label)
              weight *= 0.8
              uniq_trans.add(norm_tran)
      if label in self.supplement_labels:
        text = sections[0]
        uniq_trans = set()
        for tran in regex.split("[;,]", text):
          norm_tran = self.tokenizer.NormalizeJaWordForPos(pos, tran.strip())
          if norm_tran and norm_tran not in uniq_trans:
            Vote(norm_tran, 0.01, "")
    pos_scores = {}
    pos_base_score = 1.0
    for item in entry["item"]:
      pos = item["pos"]
      score = pos_base_score
      if item["label"] not in self.core_labels:
        score *= 0.75
      pos_scores[pos] = (pos_scores.get(pos) or 0.0) + score
      pos_base_score *= 0.9
    pos_sum_score = 0.001
    for pos, score in pos_scores.items():
      pos_sum_score += score
    pure_noun = (pos_scores.get("noun") or 0.0) / pos_sum_score >= 0.9
    pure_verb = (pos_scores.get("verb") or 0.0) / pos_sum_score >= 0.9
    pure_adjective = (pos_scores.get("adjective") or 0.0) / pos_sum_score >= 0.9
    pure_adverb = (pos_scores.get("adverb") or 0.0) / pos_sum_score >= 0.9
    bonus_translations = []
    scored_translations = set()
    for tran, score in translations.items():
      tran = unicodedata.normalize('NFKC', tran)
      norm_tran = tkrzw_dict.NormalizeWord(tran)
      prob = tran_probs.get(norm_tran)
      if prob:
        if len(norm_tran) < 2:
          prob *= 0.5
        score += prob
        del tran_probs[norm_tran]
        scored_translations.add(norm_tran)
      bonus_translations.append((tran, score))
    base_trans = set()
    for base_word in set(infl_dict.get(word) or []):
      for base_tran in aux_trans.get(base_word) or []:
        for stem in self.GetTranslationStems(base_tran):
          base_trans.add(stem)
    sorted_translations = []
    for tran, score in bonus_translations:
      norm_tran = tkrzw_dict.NormalizeWord(tran)
      if norm_tran not in scored_translations:
        bonus = 0.0
        for dict_tran, prob in tran_probs.items():
          if len(dict_tran) >= 2 and norm_tran.startswith(dict_tran):
            bonus = max(bonus, prob * 0.3)
          elif len(norm_tran) >= 2 and dict_tran.startswith(norm_tran):
            bonus = max(bonus, prob * 0.2)
          elif len(dict_tran) >= 2 and norm_tran.find(dict_tran) >= 0:
            bonus = max(bonus, prob * 0.1)
          elif len(norm_tran) >= 2 and dict_tran.find(norm_tran) >= 0:
            bonus = max(bonus, prob * 0.1)
        score += bonus
      if norm_tran in tran_labels:
        score += (len(tran_labels[norm_tran]) - 1) * 0.001
      tran_pos = self.tokenizer.GetJaLastPos(tran)
      if pure_noun:
        if tran_pos[1] == "名詞" and regex.search(r"\p{Han}", tran):
          score *= 1.2
      if pure_verb:
        if tran_pos[1] == "動詞":
          if regex.search("[うくすつぬふむゆる]$", tran):
            score *= 1.3
        elif self.tokenizer.IsJaWordSahenNoun(tran):
          score *= 1.2
      if pure_adjective:
        tran_pos = self.tokenizer.GetJaLastPos(tran)
        if tran_pos[1] == "形容詞" or self.tokenizer.IsJaWordAdjvNoun(tran):
          score *= 1.2
      if (pure_verb or pure_adjective or pure_adverb):
        if len(tran) <= 1:
          score *= 0.8
        if regex.search(r"[\p{Katakana}]", tran):
          score *= 0.7
          if regex.fullmatch(r"[\p{Katakana}ー]+", tran):
            score *= 0.7
        elif regex.fullmatch(r"[\p{Hiragana}ー]+", tran):
          score *= 0.9
        elif not regex.search(r"[\p{Han}\p{Hiragana}\p{Katakana}]+", tran):
          score *= 0.7
      else:
        if regex.search(r"[\p{Katakana}]", tran):
          score *= 0.8
          if regex.fullmatch(r"[\p{Katakana}ー]+", tran):
            score *= 0.8
        elif regex.fullmatch(r"[\p{Hiragana}ー]+", tran):
          score *= 0.95
        elif not regex.search(r"[\p{Han}\p{Hiragana}\p{Katakana}]+", tran):
          score *= 0.8
      is_dup_base = False
      for stem_tran in self.GetTranslationStems(tran):
        if stem_tran in base_trans:
          is_dup_base = True
          break
      if is_dup_base:
        score *= 0.5
      sorted_translations.append((tran, score))
    sorted_translations = sorted(sorted_translations, key=lambda x: x[1], reverse=True)
    deduped_translations = []
    for tran, score in sorted_translations:
      norm_tran = tkrzw_dict.NormalizeWord(tran)
      bias = 1.0
      for prev_tran, prev_score in deduped_translations:
        if len(prev_tran) >= 2 and norm_tran.startswith(prev_tran):
          bias = min(bias, 0.4 if len(prev_tran) >= 2 else 0.6)
        elif len(norm_tran) >= 2 and prev_tran.startswith(norm_tran):
          bias = min(bias, 0.6 if len(norm_tran) >= 2 else 0.7)
        elif len(prev_tran) >= 2 and norm_tran.find(prev_tran) >= 0:
          bias = min(bias, 0.8 if len(prev_tran) >= 2 else 0.9)
        elif len(norm_tran) >= 2 and prev_tran.find(norm_tran) >= 0:
          bias = min(bias, 0.8 if len(norm_tran) >= 2 else 0.9)
        dist = tkrzw.Utility.EditDistanceLev(norm_tran, prev_tran)
        dist /= max(len(norm_tran), len(prev_tran))
        if dist < 0.3:
          bias = min(bias, dist + 0.2)
      score *= bias
      deduped_translations.append((tran, score))
    deduped_translations = sorted(deduped_translations, key=lambda x: x[1], reverse=True)
    uniq_trans = set()
    final_translations = []
    max_elems = int(min(max(math.log2(len(entry["item"])), 2), 8) * 8)
    for tran, score in deduped_translations:
      tran = regex.sub(r"^を.*", "", tran)
      tran = regex.sub(r"・", "", tran)
      tran = self.tokenizer.NormalizeJaWordStyle(tran)
      norm_tran = tkrzw_dict.NormalizeWord(tran)
      if not norm_tran or norm_tran in uniq_trans:
        continue
      uniq_trans.add(norm_tran)
      match = regex.search("(.*)(をする|をやる|する)$", norm_tran)
      if match:
        uniq_trans.add(match.group(1) + "する")
        uniq_trans.add(match.group(1) + "をする")
        uniq_trans.add(match.group(1) + "をやる")
      if len(final_translations) < max_elems or score >= 0.001:
        final_translations.append(tran)
    sorted_aux_trans = sorted(count_aux_trans.items(), key=lambda x: -x[1])
    for aux_tran, count in sorted_aux_trans:
      aux_tran = regex.sub(r"^を.*", "", aux_tran)
      aux_tran = regex.sub(r"・", "", aux_tran)
      aux_tran = self.tokenizer.NormalizeJaWordStyle(aux_tran)
      if pure_noun:
        aux_tran = self.MakeTranNoun(aux_tran)
      if pure_verb:
        aux_tran = self.MakeTranVerb(aux_tran)
      if pure_adjective:
        aux_tran = self.MakeTranAdjective(aux_tran)
      if pure_adverb:
        aux_tran = self.MakeTranAdverb(aux_tran)
      if len(final_translations) >= max_elems: break
      norm_tran = tkrzw_dict.NormalizeWord(aux_tran)
      if not norm_tran or norm_tran in uniq_trans:
        continue
      uniq_trans.add(norm_tran)
      final_translations.append(aux_tran)
    if final_translations:
      entry["translation"] = final_translations

  def SetRelations(self, word_entry, entries, word_dicts,
                   live_words, rev_live_words, pivot_live_words,
                   phrase_prob_dbm, tran_prob_dbm, cooc_prob_dbm, extra_word_bases,
                   verb_words, adj_words, adv_words, extra_synonyms):
    word = word_entry["word"]
    norm_word = tkrzw_dict.NormalizeWord(word)
    share = float(word_entry.get("share") or 1.0)
    scores = {}
    def Vote(rel_word, label, weight):
      values = scores.get(rel_word) or []
      values.append((weight, label))
      scores[rel_word] = values
    synonyms = word_entry.get("_synonym")
    if synonyms:
      for synonym in synonyms:
        Vote(synonym, "meta", 0.1)
    parents = set()
    children = set()
    for label, entry in entries:
      stems = entry.get("stem")
      if stems:
        for stem in stems:
          parents.add(stem)
      stem_children = entry.get("stem_child")
      if stem_children:
        for child in stem_children:
          children.add(child)
      core = entry.get("core")
      if core:
        parents.add(core)
      core_children = entry.get("core_child")
      if core_children:
        for child in core_children:
          children.add(child)
      bases = entry.get("base")
      if bases:
        for base in bases:
          parents.add(base)
      base_children = entry.get("base_child")
      if base_children:
        for child in base_children:
          children.add(child)
      for rel_name, rel_weight in rel_weights.items():
        ent_rel_words = []
        expr = entry.get(rel_name)
        if expr:
          for rel_word in expr.split(","):
            rel_word = rel_word.strip()
            ent_rel_words.append(rel_word)
          if ent_rel_words:
            scored_rel_words = []
            for i, rel_word in enumerate(ent_rel_words):
              weight = 30 / (min(i, 30) + 30)
              weight *= rel_weight
              Vote(rel_word, label, weight)
        texts = entry.get("text")
        if texts:
          base_weight = 1.1
          for text in texts:
            for field in text[1].split(" [-] "):
              if not field.startswith("[" + rel_name + "]: "): continue
              field = regex.sub(r"^[^:]+: ", "", field)
              field = regex.sub(r"\(.*?\) *", "", field)
              for i, rel_word in enumerate(field.split(",")):
                rel_word = rel_word.strip()
                if rel_word:
                  weight = 30 / (min(i, 30) + 30)
                  weight *= rel_weight * base_weight
                  Vote(rel_word, label, weight)
            base_weight *= 0.95
    word_extra_synonyms = extra_synonyms.get(word)
    if word_extra_synonyms:
      synonym_match_count = 0
      for synonym in word_extra_synonyms[:6]:
        if synonym in scores:
          synonym_match_count += 1
      if share >= 0.7 or synonym_match_count >= 3:
        syn_score = 0.5
        for synonym in word_extra_synonyms:
          Vote(synonym, "meta", syn_score)
          syn_score *= 0.98
        word_entry["rephrase"] = word_extra_synonyms[:8]
    extra_word_base = extra_word_bases.get(word)
    if extra_word_base:
      parents.add(extra_word_base)
    alternatives = word_entry.get("alternative")
    if alternatives:
      for alternative in alternatives:
        if word not in force_parents:
          parents.discard(alternative)
        if alternative not in force_parents:
          children.discard(alternative)
    for variant in self.GetSpellVariants(word):
      if word not in force_parents:
        parents.discard(variant)
      if variant not in force_parents:
        children.discard(variant)
    for child in children:
      parents.discard(child)
    if word in no_parents:
      parents.clear()
    force_parent = force_parents.get(word)
    if force_parent:
      parents.clear()
      parents.add(force_parent)
    parents = set([x for x in parents if force_parents.get(x) != word])
    children = set([x for x in children if x not in no_parents])
    translations = list(word_entry.get("translation") or [])
    if tran_prob_dbm:
      tsv = tran_prob_dbm.GetStr(norm_word)
      if tsv:
        fields = tsv.split("\t")
        for i in range(0, len(fields), 3):
          src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
          translations.append(trg)
    translations = set([tkrzw_dict.NormalizeWord(x) for x in translations])
    rel_words = []
    for rel_word, votes in scores.items():
      norm_rel_word = tkrzw_dict.NormalizeWord(rel_word)
      label_weights = {}
      for weight, label in votes:
        old_weight = label_weights.get(label) or 0.0
        label_weights[label] = max(old_weight, weight)
      total_weight = 0
      for label, weight in label_weights.items():
        total_weight += weight
      if tran_prob_dbm:
        tsv = tran_prob_dbm.GetStr(norm_rel_word)
        if tsv:
          bonus = 0.0
          fields = tsv.split("\t")
          for i in range(0, len(fields), 3):
            src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
            norm_tran = tkrzw_dict.NormalizeWord(trg)
            for dict_tran in translations:
              if dict_tran == norm_tran:
                bonus = max(bonus, 1.0)
              elif len(dict_tran) >= 2 and norm_tran.startswith(dict_tran):
                bonus = max(bonus, 0.3)
              elif len(norm_tran) >= 2 and dict_tran.startswith(norm_tran):
                bonus = max(bonus, 0.2)
              elif len(dict_tran) >= 2 and norm_tran.find(dict_tran) >= 0:
                bonus = max(bonus, 0.1)
              elif len(norm_tran) >= 2 and dict_tran.find(norm_tran) >= 0:
                bonus = max(bonus, 0.1)
              dist = tkrzw.Utility.EditDistanceLev(dict_tran, norm_tran)
              dist /= max(len(dict_tran), len(norm_tran))
              if dist < 0.3:
                bonus = max(bonus, 0.3)
          total_weight += bonus
      score = 1.0
      if phrase_prob_dbm:
        prob = self.GetPhraseProb(phrase_prob_dbm, "en", rel_word)
        prob = max(prob, 0.0000001)
        score += math.exp(-abs(math.log(0.001) - math.log(prob))) * 0.1
      score *= total_weight
      if tkrzw_dict.IsStopWord("en", norm_rel_word):
        if tkrzw_dict.IsStopWord("en", norm_word):
          score *= 0.3
        else:
          score *= 0.1
      rel_words.append((rel_word, score))
    rel_words = sorted(rel_words, key=lambda x: x[1], reverse=True)
    uniq_words = set()
    final_rel_words = []
    for rel_word, score in rel_words:
      if rel_word in parents or rel_word in children:
        continue
      if not live_words.Get(rel_word) or rel_word == word:
        continue
      norm_rel_word = tkrzw_dict.NormalizeWord(rel_word)
      if not norm_rel_word: continue
      if norm_rel_word in uniq_words: continue
      uniq_words.add(norm_rel_word)
      hit = False
      for label, word_dict in word_dicts:
        if label in self.surfeit_labels: continue
        if norm_rel_word in word_dict:
          hit = True
          break
      if not hit: continue
      final_rel_words.append(rel_word)
    if final_rel_words:
      max_elems = int(min(max(math.log2(len(word_entry["item"])), 2), 6) * 6)
      word_entry["related"] = final_rel_words[:max_elems]
    scored_parents = []
    for parent in parents:
      if not live_words.Get(parent) or parent == word:
        continue
      prob = self.GetPhraseProb(phrase_prob_dbm, "en", parent)
      scored_parents.append((parent, prob))
    scored_parents = sorted(scored_parents, key=lambda x: x[1], reverse=True)
    if scored_parents:
      word_entry["parent"] = [x[0] for x in scored_parents]
    scored_children = []
    for child in children:
      if not live_words.Get(child) or child == word or child in parents:
        continue
      prob = self.GetPhraseProb(phrase_prob_dbm, "en", child)
      scored_children.append((child, prob))
    scored_children = sorted(scored_children, key=lambda x: x[1], reverse=True)
    if scored_children:
      word_entry["child"] = [x[0] for x in scored_children]
    prob = float(live_words.GetStr(word) or 0.0)
    if prob >= 0.000001 and regex.fullmatch(r"[-\p{Latin}]+", word):
      prefix = word + " "
      idioms = []
      it = live_words.MakeIterator()
      it.Jump(prefix)
      while True:
        rec = it.GetStr()
        if not rec: break
        cmp_word, cmp_prob = rec
        if not cmp_word.startswith(prefix): break
        cmp_prob = float(cmp_prob)
        cmp_score = cmp_prob / prob
        if cmp_score >= 0.001:
          has_particle = False
          for cmp_token in cmp_word.split(" ")[1:]:
            if cmp_token in particles:
              has_particle = True
              break
          if has_particle:
            cmp_score *= 3.0
          if cmp_word in verb_words or cmp_word in adj_words or cmp_word in adv_words:
            cmp_score *= 3.0
          idioms.append((cmp_word, cmp_score))
        it.Next()
      it = rev_live_words.MakeIterator()
      it.Jump(prefix)
      while True:
        rec = it.GetStr()
        if not rec: break
        cmp_word, cmp_prob = rec
        if not cmp_word.startswith(prefix): break
        cmp_word = " ".join(reversed(cmp_word.split(" ")))
        cmp_prob = float(cmp_prob)
        cmp_score = cmp_prob / prob
        if cmp_score >= 0.001:
          has_particle = False
          for cmp_token in cmp_word.split(" ")[:-1]:
            if cmp_token in particles:
              has_particle = True
              break
          if has_particle:
            cmp_score *= 3.0
          if cmp_word in verb_words or cmp_word in adj_words or cmp_word in adv_words:
            cmp_score *= 3.0
          cmp_score *= 0.9
          idioms.append((cmp_word, cmp_score))
        it.Next()
      pivot_exprs = pivot_live_words.get(word)
      if pivot_exprs:
        for pivot_expr in pivot_exprs:
          pivot_fields = pivot_expr.split("|")
          if len(pivot_fields) != 2: continue
          cmp_word = pivot_fields[0]
          cmp_prob = float(pivot_fields[1])
          cmp_score = cmp_prob / prob
          if cmp_score >= 0.001:
            has_particle = False
            for cmp_token in cmp_word.split(" "):
              if cmp_token in particles:
                has_particle = True
                break
            if has_particle:
              cmp_score *= 3.0
            if cmp_word in verb_words or cmp_word in adj_words or cmp_word in adv_words:
              cmp_score *= 3.0
            cmp_score *= 0.8
          idioms.append((cmp_word, cmp_score))
      idioms = sorted(idioms, key=lambda x: x[1], reverse=True)
      uniq_idioms = set()
      final_idioms = []
      for idiom, prob in idioms:
        if idiom in uniq_idioms: continue
        uniq_idioms.add(idiom)
        final_idioms.append(idiom)
      if final_idioms:
        max_elems = int(min(max(math.log2(len(word_entry["item"])), 2), 6) * 4)
        word_entry["idiom"] = final_idioms[:max_elems]

  def SetCoocurrences(self, word_entry, entries, word_dicts, phrase_prob_dbm, cooc_prob_dbm):
    word = word_entry["word"]
    norm_word = tkrzw_dict.NormalizeWord(word)
    tokens = self.tokenizer.Tokenize("en", word, True, True)
    cooc_words = collections.defaultdict(float)
    max_word_weight = 0.0
    for token in tokens:
      phrase_prob = self.GetPhraseProb(phrase_prob_dbm, "en", token)
      word_idf = math.log(phrase_prob) * -1
      word_weight = word_idf ** 2
      max_word_weight = max(max_word_weight, word_weight)
      tsv = cooc_prob_dbm.GetStr(token)
      if tsv:
        for field in tsv.split("\t")[:32]:
          cooc_word, cooc_prob = field.split(" ", 1)
          cooc_tokens = self.tokenizer.Tokenize("en", cooc_word, True, True)
          for cooc_token in cooc_tokens:
            if cooc_token and cooc_word not in tokens:
              cooc_words[cooc_token] += float(cooc_prob) * word_weight
    cooc_words.pop(norm_word, None)
    def_token_labels = collections.defaultdict(set)
    for item in word_entry["item"]:
      label = item["label"]
      if label not in self.full_def_labels: continue
      text = item["text"]
      text = regex.sub(r" \[-.*", "", text).strip()
      if regex.search(r"^\[-.*", text): continue
      text = regex.sub(r"\(.*?\)", "", text)
      text = regex.sub(r"\[.*?\]", "", text)
      if not text: continue
      def_tokens = self.tokenizer.Tokenize("en", text, True, True)
      for def_token in def_tokens:
        if not regex.fullmatch(r"[\p{Latin}]{2,}", def_token): continue
        if def_token in particles or def_token in misc_stop_words: continue
        if def_token in tokens: continue
        def_token_labels[def_token].add(label)
    for def_token, labels in def_token_labels.items():
      cooc_words[def_token] += 0.01 * len(labels) * max_word_weight
    is_wiki_word = "wikipedia" in cooc_words or "encyclopedia" in cooc_words
    merged_cooc_words = sorted(cooc_words.items(), key=lambda x: x[1], reverse=True)
    weighed_cooc_words = []
    for cooc_word, cooc_score in merged_cooc_words:
      cooc_prob = self.GetPhraseProb(phrase_prob_dbm, "en", cooc_word)
      cooc_idf = math.log(cooc_prob) * -1
      cooc_score *= cooc_idf ** 2
      if tkrzw_dict.IsStopWord("en", cooc_word):
        if tkrzw_dict.IsStopWord("en", norm_word):
          cooc_score *= 0.3
        else:
          cooc_score *= 0.1
      elif cooc_word in particles or cooc_word in misc_stop_words:
        cooc_score *= 0.5
      elif is_wiki_word and cooc_word in wiki_stop_words:
        cooc_score *= 0.2
      weighed_cooc_words.append((cooc_word, cooc_score))
    sorted_cooc_words = sorted(weighed_cooc_words, key=lambda x: x[1], reverse=True)
    final_cooc_words = []
    for cooc_word, cooc_score in sorted_cooc_words:
      if len(final_cooc_words) >= 16: break
      hit = False
      for label, word_dict in word_dicts:
        if label in self.surfeit_labels: continue
        if cooc_word in word_dict:
          hit = True
          break
      if not hit: continue
      final_cooc_words.append(cooc_word)
    if final_cooc_words:
      word_entry["cooccurrence"] = final_cooc_words

  def CompensateInflections(self, entry, merged_dict, verb_words):
    word = entry["word"]
    root_verb = None
    ing_value = entry.get("verb_present_participle")
    if ing_value and ing_value.endswith("<ing"):
      root_verb = ing_value[:-4]
    for infl_name in inflection_names:
      value = entry.get(infl_name)
      if not value: continue
      infl_words = []
      for infl_word in value.split(","):
        infl_word = infl_word.strip()
        if not infl_word: continue
        if not regex.fullmatch(r"[-\p{Latin}0-9', ]+", infl_word): continue
        infl_words.append(infl_word)
      if infl_words:
        entry[infl_name] = ", ".join(infl_words)
      else:
        del entry[infl_name]
    poses = set()
    for item in entry["item"]:
      poses.add(item["pos"])
    if "verb" in poses and word.find(" ") >= 0 and not regex.search(r"[A-Z]", word):
      tokens = self.tokenizer.Tokenize("en", word, False, False)
      if len(tokens) > 1:
        if not root_verb:
          for token in tokens:
            if token not in particles and token not in misc_stop_words and token in verb_words:
              root_verb = token
              break
        if not root_verb and tokens[0] in verb_words:
          root_verb = tokens[0]
        if root_verb:
          root_entry = merged_dict.get(root_verb)
          if root_entry:
            for infl_name in inflection_names:
              if not infl_name.startswith("verb_") or entry.get(infl_name):
                continue
              root_infls = root_entry[0].get(infl_name)
              if not root_infls:
                continue
              phrase_infls = []
              for root_infl in regex.split(r"[,|]", root_infls):
                root_infl = root_infl.strip()
                if not root_infl: continue
                root_infl_tokens = []
                for token in tokens:
                  if root_infl and token == root_verb:
                    root_infl_tokens.append(root_infl)
                    root_infl = None
                  else:
                    root_infl_tokens.append(token)
                phrase_infls.append(" ".join(root_infl_tokens))
              if phrase_infls:
                entry[infl_name] = ", ".join(phrase_infls)

  def CompensateAlternatives(self, word_entry, merged_dict):
    word = word_entry["word"]
    alternatives = word_entry.get("alternative") or []
    variants = self.GetSpellVariants(word)
    wn_count = 0
    for item in word_entry["item"]:
      if item["label"] != "wn": continue
      wn_count += 1
      for section in item["text"].split("[-]"):
        section = section.strip()
        match = regex.search(r"\[synonym\]: (.*)", section)
        if match:
          for synonym in match.group(1).split(","):
            synonym = synonym.strip()
            dist = tkrzw.Utility.EditDistanceLev(word, synonym)
            similar = False
            if dist == 1 and word[:3] != synonym[:3]:
              similar = True
            elif dist == 2 and word[:5] == synonym[:5] and word[-2:] == synonym[-2:]:
              similar = True
            if similar and synonym not in variants:
              variants.add(synonym)
    for variant in variants:
      if word[:2] != variant[:2]: continue
      if variant in alternatives: continue
      variant_entries = merged_dict.get(variant)
      if not variant_entries: continue
      for variant_entry in variant_entries:
        if variant_entry["word"] != variant: continue
        var_wn_count = 0
        var_wn_counts = collections.defaultdict(int)
        for item in variant_entry["item"]:
          if item["label"] != "wn": continue
          var_wn_count += 1
          for section in item["text"].split("[-]"):
            section = section.strip()
            match = regex.search(r"\[synonym\]: (.*)", section)
            if match:
              for synonym in match.group(1).split(","):
                synonym = synonym.strip()
                if synonym:
                  var_wn_counts[synonym] += 1
        hits = var_wn_counts[word]
        if (wn_count > 0 and var_wn_count == wn_count and hits == wn_count) or hits >= 4:
          alternatives.append(variant)
    if alternatives:
      word_entry["alternative"] = alternatives

  def GetSpellVariants(self, word):
    variants = set()
    suffix_pairs = [("se", "ze"), ("sing", "zing"), ("sed", "zed"), ("ser", "zer"),
                    ("sation", "zation"), ("ce", "se"),
                    ("our", "or"), ("og", "ogue"), ("re", "er"), ("l", "ll")]
    for suffix1, suffix2 in suffix_pairs:
      if word.endswith(suffix1):
        variant = word[:-len(suffix1)] + suffix2
        variants.add(variant)
      if word.endswith(suffix2):
        variant = word[:-len(suffix2)] + suffix1
        variants.add(variant)
    return variants

  def GetTranslationStems(self, text):
    stem_trans = set()
    stem_trans.add(text)
    match = regex.search(
      r"^(.*[\p{Han}\p{Katakana}ー])(する|した|している|された|されて)$", text)
    if match:
      stem = match.group(1)
      stem_trans.add(stem + "する")
      stem_trans.add(stem + "した")
      stem_trans.add(stem + "している")
      stem_trans.add(stem + "された")
      stem_trans.add(stem + "されて")
    tokens = self.tokenizer.GetJaPosList(text)
    if tokens and tokens[-1][2] == "サ変接続":
      stem_trans.add(text + "する")
      stem_trans.add(text + "した")
      stem_trans.add(text + "している")
      stem_trans.add(text + "された")
      stem_trans.add(text + "されて")
    tokens[-1][0] = tokens[-1][3]
    phrase = ""
    for token in tokens:
      phrase += token[0]
    stem_trans.add(phrase)
    while len(tokens) >= 2:
      last_token = tokens[-1]
      if last_token[1] in ("助詞", "助動詞") or last_token[2] in ("接尾", "非自立"):
        tokens = tokens[:-1]
        tokens[-1][0] = tokens[-1][3]
        phrase = ""
        for token in tokens:
          phrase += token[0]
        stem_trans.add(phrase)
      else:
        break
    return stem_trans

  def GetEntryTranslations(self, merged_dict, word, is_capital, best_pos):
    key = tkrzw_dict.NormalizeWord(word)
    entry = merged_dict.get(key)
    if not entry: return None
    scored_trans = []
    word_score = 1.0
    for word_entry in entry:
      cmp_word = word_entry["word"]
      if bool(regex.search(r"\p{Lu}", cmp_word)) != is_capital:
        continue
      item_score = 1.0
      for item in word_entry["item"]:
        pos = item["pos"]
        text = item["text"]
        trans = self.ExtractTextLabelTrans(text)
        if trans:
          score = word_score * item_score
          if pos == best_pos:
            score *= 2.0
          for tran in trans:
            scored_trans.append((tran, score))
            score *= 0.9
          item_score *= 0.9
      trans = word_entry.get("translation")
      if trans:
        score = word_score * item_score
        for tran in trans:
          scored_trans.append((tran, score))
          score *= 0.9
      word_score *= 0.5
    scored_trans = sorted(scored_trans, key=lambda x: x[1], reverse=True)
    return [x[0] for x in scored_trans]

  def PropagateTranslations(self, entry, merged_dict, tran_prob_dbm, aux_last_trans):
    old_trans = entry.get("translation") or []
    if len(old_trans) >= 8: return
    word = entry["word"]
    is_capital = bool(regex.search(r"\p{Lu}", word))
    if len(word) <= 2: return
    uniq_labels = set()
    top_exprs = []
    poses = set()
    synonyms = []
    for item in entry["item"]:
      label = item["label"]
      pos = item["pos"]
      poses.add(pos)
      if label in self.gross_labels or label in self.supplement_labels: continue
      is_first = label not in uniq_labels
      uniq_labels.add(label)
      text = item["text"]
      for field in text.split(" [-] "):
        if not field.startswith("[synonym]: "): continue
        field = regex.sub(r"^[^:]+: ", "", field)
        field = regex.sub(r"\(.*?\) *", "", field)
        for synonym in field.split(","):
          synonym = synonym.strip()
          if synonym:
            synonyms.append((synonym, pos))
      text = regex.sub(r" \[-+\] .*", "", text)
      text = regex.sub(r"\(.*?\)", "", text)
      text = regex.sub(r"\.$", "", text)
      text = regex.sub(r"([-\p{Latin}\d]{5,})\.", r"\1;", text)
      for expr in text.split(";"):
        expr = expr.strip()
        if pos == "verb":
          expr = regex.sub(r"^to +([\p{Latin}])", r"\1", expr, flags=regex.IGNORECASE)
        elif pos == "noun":
          expr = regex.sub(r"^(a|an|the) +([\p{Latin}])", r"\2", expr, flags=regex.IGNORECASE)
        if expr:
          top_exprs.append((expr, pos, is_first))
    top_words = []
    for expr, pos, is_first in top_exprs:
      manner_match = regex.search(r"^in +([-\p{Latin}].*?) +(manner|fashion|way)$",
                                  expr, regex.IGNORECASE)
      preps = ["of", "in", "at", "from", "by", "part of", "out of", "inside",
               "relating to", "related to", "associated with",
               "characterized by", "pertaining to", "derived from", "covered with",
               "resembling", "having"]
      prep_expr = None
      for prep in preps:
        if len(expr) > len(prep):
          if expr[:len(prep)].lower() == prep:
            expr_lead = expr[len(prep):]
            joint_match = regex.match(r"^,?( +or)? +", expr_lead)
            if joint_match:
              expr = expr_lead[joint_match.end():]
              prep_expr = expr
      if manner_match:
        expr = manner_match.group(1).strip()
        expr = regex.sub(r"^(a|an|the) +", "", expr, flags=regex.IGNORECASE)
        if expr:
          top_words.append((expr, "adjective", "adverb", is_first))
      elif prep_expr:
        expr = regex.sub(r"^(a|an|the) +([\p{Latin}])", r"\2", prep_expr, flags=regex.IGNORECASE)
        if expr:
          new_pos = "adverb" if pos == "adverb" else "adjective"
          top_words.append((expr, "noun", new_pos, is_first))
      else:
        expr = expr.strip()
        if expr:
          top_words.append((expr, pos, "", is_first))
    etym_prefix = entry.get("etymology_prefix")
    etym_core = entry.get("etymology_core")
    etym_suffix = entry.get("etymology_suffix")
    if ("noun" in poses and not etym_prefix and etym_core and
        etym_suffix in ("ness", "cy", "ity")):
      top_words.append((etym_core, "adjective", "noun", True))
    if ("noun" in poses and not etym_prefix and etym_core and
        etym_suffix in ("ment", "tion", "sion")):
      top_words.append((etym_core, "verb", "noun", True))
    if ("verb" in poses and not etym_prefix and etym_core and
        etym_suffix in ("ise", "ize")):
      top_words.append((etym_core, "adjective", "verb", True))
    if ("adjective" in poses and not etym_prefix and etym_core
        and etym_suffix in ("ic", "ical", "ish", "ly")):
      top_words.append((etym_core, "noun", "adjective", True))
    if ("adverb" in poses and not etym_prefix and etym_core and
        etym_suffix == "ly"):
      top_words.append((etym_core, "adjective", "adverb", True))
    parents = entry.get("parent")
    if parents:
      for parent in parents:
        if len(parent) < 5: continue
        if ("noun" in poses and
            (word.endswith("ness") or word.endswith("cy") or word.endswith("ity"))):
          top_words.append((parent, "adjective", "noun", True))
        if ("noun" in poses and
            (word.endswith("ment") or word.endswith("tion") or word.endswith("sion"))):
          top_words.append((parent, "verb", "noun", True))
        if ("verb" in poses and
            (word.endswith("ise") or word.endswith("tze"))):
          top_words.append((parent, "adjective", "verb", True))
        if ("adjective" in poses and
            (word.endswith("ic") or word.endswith("ical") or word.endswith("ish"))):
          top_words.append((parent, "noun", "adjective", True))
        if ("adverb" in poses and
            word.endswith("ly")):
          top_words.append((parent, "adjective", "adverb", True))
    ent_synonyms = entry.get("_synonym")
    if ent_synonyms:
      for synonym in ent_synonyms:
        norm_synonym = tkrzw_dict.NormalizeWord(synonym)
        syn_entries = merged_dict.get(norm_synonym)
        if syn_entries:
          syn_pos = ""
          for syn_entry in syn_entries:
            if syn_entry["word"] != synonym: continue
            for syn_item in syn_entry["item"]:
              if syn_item["pos"] in poses:
                syn_pos = syn_item["pos"]
                break
          if syn_pos:
            synonyms.append((synonym, syn_pos))
    for synonym, pos in synonyms:
      top_words.append((synonym, pos, "", False))
    trans = []
    tran_sources = set()
    for expr, pos, conversion, trustable in top_words:
      expr = regex.sub(r"^([-\p{Latin}]+), ([-\p{Latin}]+),? +or +([-\p{Latin}]+)$",
                       r"\1; \2; \3", expr)
      expr = regex.sub(r"^([-\p{Latin}]+) +or +([-\p{Latin}]+)$", r"\1; \2", expr)
      expr = regex.sub(r"^([-\p{Latin}]+), +([-\p{Latin}]+)$", r"\1; \2", expr)
      for rel_word in expr.split(";"):
        rel_word = rel_word.strip()
        if len(rel_word) <= 2: continue
        word_trans = self.GetEntryTranslations(merged_dict, rel_word, is_capital, pos)
        if not word_trans: continue
        new_pos = conversion or pos
        if new_pos == "noun":
          word_trans = [self.MakeTranNoun(x) for x in word_trans]
        elif new_pos == "verb":
          word_trans = [self.MakeTranVerb(x) for x in word_trans]
        elif new_pos == "adjective":
          word_trans = [self.MakeTranAdjective(x) for x in word_trans]
        elif new_pos == "adverb":
          word_trans = [self.MakeTranAdverb(x) for x in word_trans]
        for rank, word_tran in enumerate(word_trans):
          tran_source = (word_tran, rel_word)
          if tran_source in tran_sources: continue
          tran_sources.add(tran_source)
          trans.append((word_tran, trustable, rel_word, rank))
    prob_trans = {}
    key = tkrzw_dict.NormalizeWord(word)
    tsv = tran_prob_dbm.GetStr(key)
    if tsv:
      fields = tsv.split("\t")
      for i in range(0, len(fields), 3):
        src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
        if regex.search("[っん]$", trg) and self.tokenizer.GetJaLastPos(trg)[1] == "動詞":
          continue
        norm_trg = tkrzw_dict.NormalizeWord(trg)
        prob = float(prob)
        if src != word:
          prob *= 0.1
        prob_trans[norm_trg] = max(prob_trans.get(norm_trg) or 0.0, prob)
    scored_trans = []
    tran_counts = {}
    for tran, trustable, rel_word, rank in trans:
      tran_counts[tran] = (tran_counts.get(tran) or 0) + 1
    for tran, trustable, rel_word, rank in trans:
      norm_tran = tkrzw_dict.NormalizeWord(tran)
      max_weight = 0
      prob_hit = False
      for prob_tran, prob in prob_trans.items():
        prob **= 0.25
        dist = tkrzw.Utility.EditDistanceLev(norm_tran, prob_tran)
        dist /= max(len(norm_tran), len(prob_tran))
        weight = prob ** 0.5 + 2.0 - dist
        if norm_tran == prob_tran:
          weight *= 10
          prob_hit = True
        elif len(prob_tran) >= 2 and norm_tran.startswith(prob_tran):
          weight *= 5
          prob_hit = True
        elif len(norm_tran) >= 2 and prob_tran.startswith(norm_tran):
          weight *= 5
          prob_hit = True
        elif len(prob_tran) >= 2 and norm_tran.find(prob_tran) >= 0:
          weight *= 3
          prob_hit = True
        elif len(norm_tran) >= 2 and prob_tran.find(norm_tran) >= 0:
          weight *= 3
          prob_hit = True
        elif dist < 0.3:
          weight *= 2
          prob_hit = True
        max_weight = max(max_weight, weight)
      if not trustable and not prob_hit:
        continue
      tran_count = tran_counts[tran]
      count_score = 1 + (tran_count * 0.2)
      rank_score = 0.95 ** rank
      score = max_weight * count_score * rank_score
      scored_trans.append((tran, score, prob_hit))
    scored_trans = sorted(scored_trans, key=lambda x: x[1], reverse=True)
    rec_aux_trans = aux_last_trans.get(word)
    if rec_aux_trans:
      scored_aux_trans = []
      for aux_tran in rec_aux_trans:
        norm_trg = tkrzw_dict.NormalizeWord(aux_tran)
        prob = prob_trans.get(norm_trg) or 0.0
        prob += 0.01 / (len(aux_tran) + 1)
        scored_aux_trans.append((aux_tran, prob))
      scored_aux_trans = sorted(scored_aux_trans, key=lambda x: x[1], reverse=True)
      for aux_tran, score in scored_aux_trans:
        scored_trans.append((aux_tran, 0, False))
    final_trans = []
    uniq_trans = set()
    for tran in old_trans:
      norm_tran = tkrzw_dict.NormalizeWord(tran)
      uniq_trans.add(norm_tran)
      final_trans.append(tran)
    num_rank = 0
    for tran, score, prob_hit in scored_trans:
      if len(final_trans) >= 8: break
      norm_tran = tkrzw_dict.NormalizeWord(tran)
      if norm_tran in uniq_trans: continue
      num_rank += 1
      if not prob_hit:
        if num_rank > 3: continue
        if num_rank > 2 and len(final_trans) >= 3: continue
      uniq_trans.add(norm_tran)
      final_trans.append(tran)
    if final_trans:
      entry["translation"] = final_trans

  def MakeTranNoun(self, tran):
    pos = self.tokenizer.GetJaLastPos(tran)
    stem = self.tokenizer.CutJaWordNounParticle(tran)
    if tran.endswith("する"):
      tran = tran[:-2]
    elif tran.endswith("される"):
      tran = tran[:-3]
    elif tran.endswith("された"):
      tran = tran[:-3]
    elif tran.endswith("ような"):
      tran = tran[:-3]
    elif self.tokenizer.IsJaWordAdjvNoun(stem):
      tran = stem
    elif tran.endswith("い") and pos[1] == "形容詞":
      tran = tran[:-1] + "さ"
    elif pos[1] in "動詞" and regex.search(r"[うくすつぬふむゆる]$", tran):
      tran = tran + "こと"
    elif pos[1] in "形容詞" and regex.search(r"[きい]$", tran):
      tran = tran + "こと"
    elif pos[0] in ("た", "な") and pos[1] == "助動詞":
      tran = tran + "こと"
    return tran

  def MakeTranVerb(self, tran):
    pos = self.tokenizer.GetJaLastPos(tran)
    if self.tokenizer.IsJaWordSahenNoun(tran):
      tran = tran + "する"
    elif tran.endswith("い") and pos[1] == "形容詞":
      tran = tran[:-1] + "くする"
    elif pos[1] == "名詞" and pos[2] == "形容動詞語幹":
      tran = tran + "にする"
    return tran

  def MakeTranAdjective(self, tran):
    pos = self.tokenizer.GetJaLastPos(tran)
    stem = self.tokenizer.CutJaWordNounParticle(tran)
    is_adjv = False
    if tran.endswith("する"):
      tran = tran[:-2]
    elif tran.endswith("される"):
      tran = tran[:-3]
    elif tran.endswith("された"):
      tran = tran[:-3]
    elif tran.endswith("ような"):
      tran = tran[:-3]
    elif self.tokenizer.IsJaWordAdjvNoun(stem):
      tran = stem
      is_adjv = True
    pos = self.tokenizer.GetJaLastPos(tran)
    if self.tokenizer.IsJaWordAdjvNounOnly(tran):
      tran += "な"
    elif pos[1] == "名詞":
      if tran.endswith("的"):
        tran += "な"
      else:
        tran += "の"
    return tran

  def MakeTranAdverb(self, tran):
    pos = self.tokenizer.GetJaLastPos(tran)
    stem = self.tokenizer.CutJaWordNounParticle(tran)
    if tran.endswith("する"):
      tran = tran[:-2] + "して"
    elif tran.endswith("される"):
      tran = tran[:-3] + "されて"
    elif tran.endswith("された"):
      tran = tran[:-3] + "されて"
    elif tran.endswith("ような"):
      tran = tran[:-3] + "ように"
    elif tran.endswith("らしい"):
      tran = tran[:-3] + "らしく"
    elif tran.endswith("とした"):
      tran = tran[:-3] + "として"
    elif tran.endswith("い") and pos[1] == "形容詞":
      tran = tran[:-1] + "く"
    elif tran.endswith("的な"):
      tran = tran[:-1] + "に"
    elif self.tokenizer.IsJaWordSahenNoun(stem):
      tran = stem + "して"
    elif self.tokenizer.IsJaWordAdjvNoun(stem):
      tran = stem + "に"
    elif stem != tran or pos[1] == "名詞":
      tran = stem + "で"
    elif pos[0] == "た" and pos[1] == "助動詞":
      tran = tran[:-1] + "て"
    elif pos[1] == "動詞":
      tran = stem + "ように"
    return tran

  def SetPhraseTranslations(self, entry, merged_dict, aux_trans, aux_last_trans,
                            tran_prob_dbm, phrase_prob_dbm, nmt_probs, noun_words, verb_words,
                            live_words, rev_live_words, pivot_live_words, pivot_dead_words,
                            keywords, rival_key_trans):
    if not tran_prob_dbm or not phrase_prob_dbm:
      return
    word = entry["word"]
    if not regex.fullmatch(r"[-\p{Latin}]+", word):
      return
    if len(word) < 2 or word in ("an", "the"):
      return
    is_noun = word in noun_words
    is_verb = word in verb_words
    word_prob = float(phrase_prob_dbm.GetStr(word) or 0.0)
    min_core_prob = 0.000001 if word in keywords else 0.00001
    if word_prob < min_core_prob:
      return
    word_mod_prob = min(word_prob, 0.001)
    norm_word = " ".join(self.tokenizer.Tokenize("en", word, True, True))
    if word != norm_word:
      stem_infl_hit = False
      for stem_entry in merged_dict.get(norm_word) or []:
        for infl_name in inflection_names:
          infl_value = stem_entry.get(infl_name)
          if infl_value:
            for infl in infl_value:
              if infl == word:
                stem_infl_hit = True
      if stem_infl_hit:
        return
    colloc_words = set()
    for cooc in entry.get("cooccurrence") or []:
      if (len(cooc) > 1 and cooc != word and
          cooc not in particles and cooc not in misc_stop_words):
        colloc_words.add(cooc)
    phrases = []
    for particle in particles:
      phrase = word + " " + particle
      is_live = False
      phrase_prob = None
      phrase_entries = merged_dict.get(phrase)
      if phrase_entries:
        for phrase_entry in phrase_entries:
          if phrase_entry["word"] == phrase:
            is_live = True
            phrase_prob_expr = phrase_entry["probability"]
            if phrase_prob_expr:
              phrase_prob = float(phrase_prob_expr)
              break
      if not phrase_prob:
        phrase_prob = float(phrase_prob_dbm.GetStr(phrase) or 0.0)
        if is_verb and phrase_prob / word_mod_prob >= 0.005:
          for pron in obj_pronouns:
            pron_phrase = word + " " + pron + " " + particle
            pron_phrase_prob = float(phrase_prob_dbm.GetStr(pron_phrase) or 0.0)
            if pron_phrase_prob > 0.0:
              phrase_prob += pron_phrase_prob
      ratio = phrase_prob / word_mod_prob
      phrases.append((phrase, is_live, True, ratio, ratio, phrase_prob, particle))
      if ratio >= 0.005:
        for sub_particle in particles:
          sub_phrase = phrase + " " + sub_particle
          sub_is_live = False
          sub_phrase_prob = None
          sub_phrase_entries = merged_dict.get(sub_phrase)
          if sub_phrase_entries:
            for sub_phrase_entry in sub_phrase_entries:
              if sub_phrase_entry["word"] == sub_phrase:
                sub_is_live = True
                sub_phrase_prob_expr = sub_phrase_entry["probability"]
                if sub_phrase_prob_expr:
                  sub_phrase_prob = float(sub_phrase_prob_expr)
                  break
          if not sub_phrase_prob:
            sub_phrase_prob = float(phrase_prob_dbm.GetStr(sub_phrase) or 0.0)
          sub_ratio = max(sub_phrase_prob / phrase_prob, 0.01)
          phrases.append((sub_phrase, sub_is_live, True, max(sub_ratio, ratio),
                          ratio * (sub_ratio ** 0.005), sub_phrase_prob, particle))
    verb_prob = 0.0
    if is_verb:
      for auxverb in ("not", "will", "shall", "can", "may", "must"):
        auxverb_prob = float(phrase_prob_dbm.GetStr(auxverb + " " + word) or 0.0)
        verb_prob += auxverb_prob
      verb_prob *= 20
    for particle in particles:
      phrase = particle + " " + word
      is_live = False
      phrase_prob = None
      phrase_entries = merged_dict.get(phrase)
      if phrase_entries:
        for phrase_entry in phrase_entries:
          if phrase_entry["word"] == phrase:
            is_live = True
            phrase_prob_expr = phrase_entry["probability"]
            if phrase_prob_expr:
              phrase_prob = float(phrase_prob_expr)
              break
      if not phrase_prob:
        phrase_prob = float(phrase_prob_dbm.GetStr(phrase) or 0.0)
      if particle == "to":
        phrase_prob -= verb_prob
      ratio = phrase_prob / word_mod_prob
      phrases.append((phrase, is_live, False, ratio, ratio, phrase_prob, particle))
      if is_noun:
        for art in ("the", "a", "an"):
          sub_phrase = particle + " " + art + " " + word
          sub_is_live = False
          sub_phrase_prob = None
          sub_phrase_entries = merged_dict.get(sub_phrase)
          if sub_phrase_entries:
            for sub_phrase_entry in sub_phrase_entries:
              if sub_phrase_entry["word"] == sub_phrase:
                sub_is_live = True
                sub_phrase_prob_expr = sub_phrase_entry["probability"]
                if sub_phrase_prob_expr:
                  sub_phrase_prob = float(sub_phrase_prob_expr)
                  break
          if not sub_phrase_prob:
            sub_phrase_prob = float(phrase_prob_dbm.GetStr(sub_phrase) or 0.0)
          sub_ratio = sub_phrase_prob / word_mod_prob
          phrases.append((sub_phrase, sub_is_live, False, sub_ratio, sub_ratio,
                          sub_phrase_prob, particle))
    it = live_words.MakeIterator()
    it.Jump(word + " ")
    while True:
      rec = it.GetStr()
      if not rec: break
      phrase, phrase_prob = rec
      if not phrase.startswith(word + " "): break
      suffix = phrase[len(word)+1:]
      phrase_prob = float(phrase_prob)
      ratio = phrase_prob / word_prob
      min_phrase_ratio = 0.01 if phrase in keywords else 0.05
      is_colloc = False
      for token in phrase.split(" "):
        if token in colloc_words:
          is_colloc = True
      if is_colloc:
        min_phrase_ratio *= 0.5
      if ratio >= min_phrase_ratio:
        phrases.append((phrase, True, True, ratio, ratio, phrase_prob, suffix))
      it.Next()
    it = rev_live_words.MakeIterator()
    it.Jump(word + " ")
    while True:
      rec = it.GetStr()
      if not rec: break
      phrase, phrase_prob = rec
      if not phrase.startswith(word + " "): break
      phrase = " ".join(reversed(phrase.split(" ")))
      prefix = phrase[:-(len(word)+1)]
      phrase_prob = float(phrase_prob)
      ratio = phrase_prob / word_prob
      min_phrase_ratio = 0.01 if phrase in keywords else 0.05
      is_colloc = False
      for token in phrase.split(" "):
        if token in colloc_words:
          is_colloc = True
      if is_colloc:
        min_phrase_ratio *= 0.5
      if ratio >= min_phrase_ratio and not regex.search("^[A-Z][a-zA-Z]+ [a-z]", phrase):
        phrases.append((phrase, True, True, ratio, ratio, phrase_prob, prefix))
      it.Next()
    uniq_pivots = set()
    for pivot in [word, entry.get("verb_present_participle"), entry.get("verb_past_participle")]:
      if not pivot: continue
      pivot = regex.sub(r",.*", "", pivot).strip()
      if pivot in uniq_pivots: continue
      uniq_pivots.add(pivot)
      pivot_prob = None
      if pivot != word:
        pivot_key = tkrzw_dict.NormalizeWord(pivot)
        pivot_entries = merged_dict.get(pivot_key)
        if pivot_entries:
          for pivot_entry in pivot_entries:
            if pivot_entry["word"] != pivot: continue
            pivot_prob = float(pivot_entry.get("probability") or 0)
      if pivot_prob and pivot_prob > word_prob: continue
      if not pivot_prob or pivot_prob > word_mod_prob:
        pivot_prob = word_mod_prob
      pivot_weight = 0.9 if pivot == word else 0.8
      for corpus_weight, is_live, pivot_exprs in [(1.0, True, pivot_live_words.get(pivot)),
                                                  (0.9, False, pivot_dead_words.get(pivot))]:
        if not pivot_exprs: continue
        for pivot_expr in pivot_exprs:
          pivot_fields = pivot_expr.split("|")
          if len(pivot_fields) != 2: continue
          phrase = pivot_fields[0]
          phrase_prob = float(pivot_fields[1])
          if phrase.lower().startswith("to "):
            phrase_prob -= verb_prob
          ratio = phrase_prob / pivot_prob
          min_phrase_ratio = 0.01 if phrase in keywords else 0.05
          is_colloc = False
          for token in phrase.split(" "):
            if token in colloc_words:
              is_colloc = True
          if is_colloc:
            min_phrase_ratio *= 0.5
          if ratio >= min_phrase_ratio:
            score = ratio * pivot_weight * corpus_weight
            affix = ""
            if phrase.startswith(pivot + " "):
              affix = phrase[len(pivot)+1:]
            elif phrase.endswith(" " + pivot):
              affix = phrase[:-len(pivot)-1:]
            phrases.append((phrase, is_live, False, ratio, score, phrase_prob, affix))
    if not phrases:
      return
    orig_trans = collections.defaultdict(float)
    tsv = tran_prob_dbm.GetStr(word)
    if tsv:
      fields = tsv.split("\t")
      for i in range(0, len(fields), 3):
        src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
        trg = regex.sub(r"[～〜]", "", trg)
        trg, trg_prefix, trg_suffix = self.tokenizer.StripJaParticles(trg)
        if src == word and prob >= 0.06:
          orig_trans[trg] = prob
    nmt_tran_probs = nmt_probs.get(word)
    if nmt_tran_probs:
      for trg, prog in nmt_tran_probs.items():
        orig_trans[trg] += prog
    aux_orig_trans = (aux_trans.get(word) or []) + (aux_last_trans.get(word) or [])
    if aux_orig_trans:
      for trg in set(aux_orig_trans):
        trg = regex.sub(r"[～〜]", "", trg)
        trg, trg_prefix, trg_suffix = self.tokenizer.StripJaParticles(trg)
        orig_trans[trg] = float(orig_trans.get(trg) or 0) + 0.1
    dedup_trans = []
    ent_orig_trans = entry.get("translation")
    if ent_orig_trans:
      base_score = 0.1
      for ent_orig_tran in ent_orig_trans:
        orig_trans[ent_orig_tran] = float(orig_trans.get(ent_orig_tran) or 0) + base_score
        base_score *= 0.9
      dedup_trans.extend(ent_orig_trans)
    dedup_trans.extend(rival_key_trans)
    final_phrases = []
    uniq_phrases = set()
    for phrase, is_live, is_suffix, mod_prob, phrase_score, raw_prob, affix in phrases:
      if regex.search(r"^([b-z]|am|are|is|was|were|has|gets|becomes) ", phrase): continue
      if phrase in uniq_phrases: continue
      uniq_phrases.add(phrase)
      phrase_trans = {}
      phrase_prefixes = {}
      pos_match = False
      if is_live:
        pos_match = is_verb if is_suffix else is_noun
      if mod_prob >= 0.02 and pos_match:
        sum_tran_probs = collections.defaultdict(float)
        tsv = tran_prob_dbm.GetStr(phrase)
        if tsv:
          fields = tsv.split("\t")
          for i in range(0, len(fields), 3):
            src, trg, prob = fields[i], fields[i + 1], float(fields[i + 2])
            if src != phrase:
              continue
            sum_tran_probs[trg] += prob
        nmt_tran_probs = nmt_probs.get(phrase)
        if nmt_tran_probs:
          phrase_aux_trans = aux_trans.get(phrase) or []
          for trg, prog in nmt_tran_probs.items():
            if sum_tran_probs[trg] > 0.04 or trg in phrase_aux_trans:
              sum_tran_probs[trg] += prog
        for trg, prob in sum_tran_probs.items():
          if regex.search("[っん]$", trg) and self.tokenizer.GetJaLastPos(trg)[1] == "動詞":
            continue
          if (is_verb and regex.search("[いきしちにひみり]$", trg) and
              self.tokenizer.GetJaLastPos(trg)[1] == "動詞"):
            continue
          trg = regex.sub(r"[～〜]", "", trg)
          trg, trg_prefix, trg_suffix = self.tokenizer.StripJaParticles(trg)
          if not trg or regex.fullmatch(r"[\p{Katakana}ー]+", trg):
            continue
          pos = self.tokenizer.GetJaLastPos(trg)
          if (is_noun and is_suffix and pos[1] == "名詞" and
              not self.tokenizer.IsJaWordSahenNoun(trg)):
            continue
          if is_noun and is_suffix and trg in ("ある", "いる", "です", "ます"):
            continue
          orig_prob = orig_trans.get(trg) or 0.0
          if is_verb:
            if self.tokenizer.IsJaWordSahenNoun(trg):
              orig_prob = max(orig_prob, orig_trans.get(trg + "する") or 0.0)
            for ext_suffix in ("する", "した", "して", "される", "された", "されて"):
              if len(trg) >= len(ext_suffix) + 2 and trg.endswith(ext_suffix):
                orig_prob = max(orig_prob, orig_trans.get(trg[:-len(ext_suffix)]) or 0.0)
          if (is_suffix and is_verb and not trg_prefix and trg_suffix and
              (pos[1] == "動詞" or self.tokenizer.IsJaWordSahenNoun(trg))):
            trg_prefix = trg_suffix
            trg_suffix = ""
          elif is_suffix and is_noun and not trg_prefix:
            if trg_suffix == "のため":
              trg_suffix = "ための"
            trg_prefix = trg_suffix
            trg_suffix = ""
          elif not trg_suffix and trg_prefix in ("ための", "のため"):
            if trg.endswith("する"):
              trg += "ための"
            else:
              trg += "のため"
            trg_prefix = ""
          elif trg_suffix:
            trg += trg_suffix
          sum_prob = orig_prob + prob
          if sum_prob >= 0.1:
            if is_verb and pos[1] == "動詞":
              sum_prob += 0.1
            phrase_trans[trg] = float(phrase_trans.get(trg) or 0.0) + sum_prob
            if trg_prefix and not trg_suffix:
              part_key = trg + ":" + trg_prefix
              phrase_prefixes[part_key] = float(phrase_trans.get(part_key) or 0.0) + sum_prob
      for aux_phrase_trans in (aux_trans.get(phrase), aux_last_trans.get(phrase)):
        if aux_phrase_trans:
          for trg in aux_phrase_trans:
            trg = regex.sub(r"[～〜]", "", trg)
            trg, trg_prefix, trg_suffix = self.tokenizer.StripJaParticles(trg)
            if is_noun and is_suffix and trg in ("ある", "いる", "です", "ます"):
              continue
            phrase_trans[trg] = float(phrase_trans.get(trg) or 0.0) + 0.1
      if mod_prob >= 0.001:
        phrase_entries = merged_dict.get(phrase)
        if phrase_entries:
          for phrase_entry in phrase_entries:
            if phrase_entry["word"] != phrase: continue
            ent_phrase_trans = phrase_entry.get("translation")
            if ent_phrase_trans:
              base_score = 0.15
              for trg in ent_phrase_trans:
                trg, trg_prefix, trg_suffix = self.tokenizer.StripJaParticles(trg)
                phrase_trans[trg] = float(phrase_trans.get(trg) or 0.0) + base_score
                if trg_prefix and not trg_suffix:
                  part_key = trg + ":" + trg_prefix
                  phrase_prefixes[part_key] = float(phrase_trans.get(part_key) or 0.0) + base_score
                base_score *= 0.9
      if not phrase_trans:
        continue
      if raw_prob < 0.05:
        has_uniq_trans = False
        if dedup_trans:
          affix_trans = (affix and aux_trans.get(affix)) or []
          for phrase_tran, score in phrase_trans.items():
            affix_full_match = False
            affix_part_match = False
            for affix_tran in affix_trans:
              if affix_tran == phrase_tran:
                affix_full_match = True
                break
              if len(affix_tran) >= 2 and phrase_tran.find(affix_tran) >= 0:
                affix_part_match = True
                break
            if affix_full_match:
              continue
            if affix_part_match:
              orig_part_match = False
              for orig_tran in dedup_trans:
                if len(orig_tran) >= 2 and phrase_tran.find(orig_tran) >= 0:
                  orig_part_match = True
                  break
              if orig_part_match:
                continue
            if regex.search("\p{Han}", phrase_tran):
              has_uniq_trans = True
            else:
              katakana = regex.sub("[^\p{Katakana}ー]", "", phrase_tran)
              if len(katakana) >= 2:
                is_dup_tran = False
                for orig_tran in dedup_trans:
                  if orig_tran.find(katakana) >= 0:
                    is_dup_tran = True
                if is_dup_tran:
                  continue
              if phrase_tran not in dedup_trans:
                has_uniq_trans = True
        else:
          has_uniq_trans = True
        if not has_uniq_trans:
          continue
      is_base = word in phrase.split(" ")
      max_tran_prob = 0
      for _, tran_prob in phrase_trans.items():
        max_tran_prob = max(max_tran_prob, tran_prob)
      if (not is_base or raw_prob < 0.01) and not is_live and max_tran_prob < 0.2:
        continue
      for tran in list(phrase_trans.keys()):
        if not regex.search(r"[\p{Han}\p{Katakana}]", tran):
          continue
        for cmp_tran, cmp_score in list(phrase_trans.items()):
          if cmp_tran not in phrase_trans: continue
          if cmp_tran.startswith(tran):
            suffix = cmp_tran[len(tran):]
            if suffix in ("する", "される", "をする", "に", "な", "の"):
              phrase_trans[cmp_tran] = cmp_score + float(phrase_trans.get(tran) or 0)
              phrase_trans.pop(tran, None)
      mod_trans = {}
      for tran, score in phrase_trans.items():
        prefix_check = tran + ":"
        best_prefix = ""
        best_prefix_score = 0.0
        for prefix, score in phrase_prefixes.items():
          if not prefix.startswith(prefix_check): continue
          if score >= best_prefix_score:
            best_prefix = prefix[len(prefix_check):]
            best_prefix_score = score
        if regex.search(r"^[\p{Katakana}ー]", tran):
          score *= 0.5
        pos = self.tokenizer.GetJaLastPos(tran)
        if is_suffix and is_verb:
          if pos[1] == "動詞" and regex.search("[うくすつぬふむゆる]$", tran):
            score *= 1.5
          if pos[1] == "名詞" and not self.tokenizer.IsJaWordSahenNoun(tran):
            score *= 0.5
        if not is_suffix and pos[1] == "名詞" and not best_prefix:
          if self.tokenizer.IsJaWordSahenNoun(tran) or self.tokenizer.IsJaWordAdjvNoun(tran):
            score *= 0.7
          else:
            score *= 0.5
        if len(tran) <= 1:
          score *= 0.5
        if is_verb:
          orig_tran = tran
          pos = self.tokenizer.GetJaLastPos(tran)
          if self.tokenizer.IsJaWordSahenNoun(tran) and best_prefix != "の":
            tran = tran + "する"
        if best_prefix and best_prefix not in ("を", "が", "は"):
          tran = "({}){}".format(best_prefix, tran)
        mod_trans[tran] = float(mod_trans.get(tran) or 0.0) + score
      scored_trans = sorted(mod_trans.items(), key=lambda x: x[1], reverse=True)[:4]
      if scored_trans:
        final_phrases.append((phrase, phrase_score, raw_prob, [x[0] for x in scored_trans]))
    if final_phrases:
      final_phrases = sorted(final_phrases, key=lambda x: x[1], reverse=True)
      map_phrases = []
      for phrase, score, raw_prob, trans in final_phrases:
        prob_expr = "{:.6f}".format(raw_prob / word_prob).replace("0.", ".")
        map_phrase = {"w": phrase, "p": prob_expr, "x": trans}
        is_live = False
        phrase_entries = merged_dict.get(tkrzw_dict.NormalizeWord(phrase))
        if phrase_entries:
          for phrase_entry in phrase_entries:
            if phrase_entry["word"] == phrase:
              is_live = True
              break
        if is_live:
          map_phrase["i"] = "1"
        map_phrases.append(map_phrase)
      entry["phrase"] = map_phrases

  def FilterParents(self, word_entry, merged_dict):
    word = word_entry["word"]
    parents = word_entry.get("parent")
    if not parents or len(parents) < 2: return
    ancestors = parents
    while True:
      grand_ancestors = []
      for ancestor in ancestors:
        ancestor_entries = merged_dict.get(ancestor)
        if ancestor_entries:
          for ancestor_entry in ancestor_entries:
            if ancestor_entry["word"] != ancestor: continue
            for grand_ancestor in ancestor_entry.get("parent") or []:
              if grand_ancestor in ancestors and grand_ancestor not in grand_ancestors:
                grand_ancestors.append(grand_ancestor)
      if not grand_ancestors or len(grand_ancestors) == len(ancestors):
        break
      ancestors = grand_ancestors
    scored_parents = []
    for parent in parents:
      parent_prob = 0
      parent_entries = merged_dict.get(parent)
      if parent_entries:
        for parent_entry in parent_entries:
          if parent_entry["word"] != parent: continue
          parent_prob = float(parent_entry.get("probability")) or 0
      score = parent_prob + 0.000001
      if word.startswith(parent):
        score *= 2
      if parent in ancestors:
        score += 1
      else:
        is_dup = False
        for suffix in ("ing", "ed", "er", "or", "ism", "ist", "est"):
          for ancestor in ancestors:
            candidate = ancestor + suffix
            if (parent[:3] == candidate[:3] and
                tkrzw.Utility.EditDistanceLev(parent, candidate) < 2):
              is_dup = True
        if is_dup:
          continue
      scored_parents.append((parent, score))
    scored_parents = sorted(scored_parents, key=lambda x: x[1], reverse=True)
    word_entry["parent"] = [x[0] for x in scored_parents]

  def AbsorbInflections(self, word_entry, merged_dict):
    word = word_entry["word"]
    translations = word_entry.get("translation")
    is_capital = bool(regex.search(r"[A-Z]", word))
    num_word_tokens = word.count(" ") + 1
    infls = []
    for infl_name in inflection_names:
      infl_value = word_entry.get(infl_name)
      if infl_value:
        named_infls = []
        for infl in regex.split(r"[,|]", infl_value):
          infl = infl.strip()
          if not infl: continue
          if not is_capital and regex.search(r"[A-Z]", infl): continue
          if num_word_tokens == 1 and infl.find(" ") >= 0: continue
          if infl not in infls:
            infls.append(infl)
          if infl not in named_infls:
            named_infls.append(infl)
        if named_infls:
          word_entry[infl_name] = named_infls
        else:
          del word_entry[infl_name]
    phrases = []
    for infl in infls:
      if infl == word: continue
      infl_entries = merged_dict.get(infl)
      if not infl_entries: continue
      for infl_entry in infl_entries:
        if infl_entry["word"] != infl: continue
        is_core = False
        good_labels = set()
        num_good_items = 0
        for infl_item in infl_entry["item"]:
          label = infl_item["label"]
          text = infl_item["text"]
          if label in self.supplement_labels: continue
          if regex.search(r"^\[\w+]:", text): continue
          good_labels.add(label)
          if label in self.core_labels:
            is_core = True
          num_good_items += 1
        alive = True
        if len(good_labels) < 2 and not is_core and num_good_items < 3:
          infl_entry["deleted"] = True
          alive = False
        infl_trans = infl_entry.get("translation")
        if infl_trans:
          has_uniq_trans = False
          if translations:
            for infl_tran in infl_trans:
              katakana = regex.sub("[^\p{Katakana}ー]", "", infl_tran)
              if len(katakana) >= 2:
                is_dup_tran = False
                for orig_tran in translations:
                  if orig_tran.find(katakana) >= 0:
                    is_dup_tran = True
                if is_dup_tran:
                  continue
              if not infl_tran in translations:
                has_uniq_trans = True
          else:
            has_uniq_trans = True
          if has_uniq_trans:
            phrase = {"w": infl, "x": infl_trans[:4]}
            if alive:
              phrase["i"] = "1"
            phrases.append(phrase)
    phrases = phrases + (word_entry.get("phrase") or [])
    map_phrases = {}
    for phrase in phrases:
      text = phrase["w"]
      map_phrases[text] = phrase
    surfaces = {word}
    for infl_name in inflection_names:
      for infl in word_entry.get(infl_name) or []:
        surfaces.add(infl)
    new_phrases = []
    unique_phrases = set()
    for phrase in phrases:
      text = phrase["w"]
      if text in unique_phrases: continue
      unique_phrases.add(text)
      new_phrases.append(phrase)
      tokens = text.split(" ")
      if len(tokens) > num_word_tokens and tokens[0] in surfaces:
        infl_tokens = tokens.copy()
        for surface in surfaces:
          if surface == tokens[0]: continue
          infl_tokens[0] = surface
          infl_text = " ".join(infl_tokens)
          if infl_text in unique_phrases: continue
          infl_phrase = map_phrases.get(infl_text)
          if infl_phrase:
            new_phrases.append(infl_phrase)
            unique_phrases.add(infl_text)
    if new_phrases:
      word_entry["phrase"] = new_phrases

  def RemoveDuplicatedtranslations(self, word_entry, main_trans):
    if not main_trans: return
    word = word_entry["word"]
    trans = word_entry.get("translation")
    if not trans: return
    dict_trans = set()
    for item in word_entry["item"]:
      text = item["text"]
      for part in text.split("[-]"):
        part = part.strip()
        match = regex.search(r"^\[translation\]: (.*)", part)
        if match:
          expr = regex.sub(r"\[-.*", "", match.group(1))
          expr = regex.sub(r"\(.*?\)", "", expr).strip()
          for tran in expr.split(","):
            tran = tran.strip()
            if len(tran) >= 2:
              dict_trans.add(tran)
    new_trans = []
    for tran in trans:
      if tran not in dict_trans:
        is_dup = False
        for main_tran in main_trans:
          if tran == main_tran:
            is_dup = True
          elif len(main_tran) >= 2 and tran.find(main_tran) >= 0:
            is_dup = True
        if is_dup: continue
      new_trans.append(tran)
    if new_trans:
      word_entry["translation"] = new_trans
    else:
      del word_entry["translation"]


def main():
  args = sys.argv[1:]
  output_path = tkrzw_dict.GetCommandFlag(args, "--output", 1) or "union-body.tkh"
  core_labels = set((tkrzw_dict.GetCommandFlag(args, "--core", 1) or "xa,wn").split(","))
  full_def_labels = set((tkrzw_dict.GetCommandFlag(
    args, "--full_def", 1) or "ox,wn,we").split(","))
  gross_labels = set((tkrzw_dict.GetCommandFlag(args, "--gross", 1) or "wj").split(","))
  top_labels = set((tkrzw_dict.GetCommandFlag(args, "--top", 1) or "we,lx,xa").split(","))
  slim_labels = set((tkrzw_dict.GetCommandFlag(args, "--slim", 1) or "ox,we,wj").split(","))
  surfeit_labels = set((tkrzw_dict.GetCommandFlag(args, "--surfeit", 1) or "we").split(","))
  tran_list_labels = set((tkrzw_dict.GetCommandFlag(
    args, "--tran_list", 1) or "xa,wn,we").split(","))
  supplement_labels = set((tkrzw_dict.GetCommandFlag(args, "--supplement", 1) or "xs").split(","))
  phrase_prob_path = tkrzw_dict.GetCommandFlag(args, "--phrase_prob", 1) or ""
  tran_prob_path = tkrzw_dict.GetCommandFlag(args, "--tran_prob", 1) or ""
  nmt_prob_path = tkrzw_dict.GetCommandFlag(args, "--nmt_prob", 1) or ""
  tran_aux_paths = (tkrzw_dict.GetCommandFlag(args, "--tran_aux", 1) or "").split(",")
  tran_aux_last_paths = (tkrzw_dict.GetCommandFlag(args, "--tran_aux_last", 1) or "").split(",")
  rev_prob_path = tkrzw_dict.GetCommandFlag(args, "--rev_prob", 1) or ""
  cooc_prob_path = tkrzw_dict.GetCommandFlag(args, "--cooc_prob", 1) or ""
  aoa_paths = (tkrzw_dict.GetCommandFlag(args, "--aoa", 1) or "").split(",")
  keyword_path = tkrzw_dict.GetCommandFlag(args, "--keyword", 1) or ""
  hint_path = tkrzw_dict.GetCommandFlag(args, "--hint", 1) or ""
  synonym_path = tkrzw_dict.GetCommandFlag(args, "--synonym", 1) or ""
  min_prob_exprs = tkrzw_dict.GetCommandFlag(args, "--min_prob", 1) or ""
  min_prob_map = {}
  for min_prob_expr in min_prob_exprs.split(","):
    columns = min_prob_expr.split(":")
    if len(columns) == 2:
      min_prob_map[columns[0]] = float(columns[1])
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  unused_flag = tkrzw_dict.GetUnusedFlag(args)
  if unused_flag:
    raise RuntimeError("Unknow flag: " + unused_flag)
  inputs = tkrzw_dict.GetArguments(args)
  if not inputs:
    raise RuntimeError("inputs are required")
  input_confs = []
  for input in inputs:
    input_conf = input.split(":", 1)
    if len(input_conf) != 2:
      raise RuntimeError("invalid input: " + input)
    input_confs.append(input_conf)
  BuildUnionDBBatch(input_confs, output_path, core_labels, full_def_labels, gross_labels,
                    surfeit_labels, top_labels, slim_labels, tran_list_labels, supplement_labels,
                    phrase_prob_path, tran_prob_path, nmt_prob_path,
                    tran_aux_paths, tran_aux_last_paths,
                    rev_prob_path, cooc_prob_path, aoa_paths,
                    keyword_path, hint_path, synonym_path, min_prob_map).Run()


if __name__=="__main__":
  main()
