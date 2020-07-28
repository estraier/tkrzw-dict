#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to parse Wikipedia XML stream and export raw text TSV of articles.
#
# Usage:
# $ bzcat enwiki-20200701-pages-articles-multistream.xml.bz2 |
#   ./parse_wikipedia.py 0.11 | bzip2 -c > enwiki-raw.tsv.bz2
# $ bzcat jawiki-20200701-pages-articles-multistream.xml.bz2 |
#   ./parse_wikipedia.py 0.61 | bzip2 -c > jawiki-raw.tsv.bz2
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
import xml.sax
import xml.sax.handler


random.seed(19780211)
log_format = "%(levelname)s\t%(message)s"
logging.basicConfig(format=log_format, stream=sys.stderr)
logger = logging.getLogger("parse_wikipedia")
logger.setLevel(logging.INFO)


class XMLHandler(xml.sax.handler.ContentHandler):
  def __init__(self, sampling_ratio, max_outputs):
    self.sampling_ratio = sampling_ratio
    self.max_outputs = max_outputs
    self.num_articles = 0
    self.num_outputs = 0
    self.tags = []
    self.is_redirect = False
    self.has_restrictions = False
    self.model = None
    self.format = None
    self.text = None

  def startDocument(self):
    logger.info("Start the document")

  def endDocument(self):
    logger.info("End the document")

  def startElement(self, name, attrs):
    self.tags.append(name)
    if self.tags == ['mediawiki', 'page']:
      self.is_redirect = False
      self.has_restrictions = False
    if self.tags == ['mediawiki', 'page', 'redirect']:
      self.is_redirect = True
    if self.tags == ['mediawiki', 'page', 'restrictions']:
      self.has_restrictions = True
    if self.tags == ['mediawiki', 'page', 'revision', 'model']:
      self.model = ""
    if self.tags == ['mediawiki', 'page', 'revision', 'model']:
      self.text = ""
    if self.tags == ['mediawiki', 'page', 'revision', 'format']:
      self.format = ""

  def endElement(self, name):
    if self.tags == ['mediawiki', 'page', 'revision']:
      if (not self.is_redirect and not self.has_restrictions and
          self.model == 'wikitext' and self.format == 'text/x-wiki' and self.text):
        self.num_articles += 1
        if self.num_articles % 1000 == 0:
          logger.info("Article {}".format(self.num_articles))
        if random.random() <= self.sampling_ratio:
          sentences = self.getSentences(self.text)
          if sentences:
            self.num_outputs += 1
            if self.num_outputs % 100 == 0:
              logger.info("Output {}".format(self.num_outputs))
            print('\t'.join(sentences))
      self.model = None
      self.format = None
    self.tags.pop()
    if self.num_outputs >= self.max_outputs:
      logger.info("reached max outputs ({})".format(self.max_outputs))
      raise xml.sax.SAXException("reached max articles")

  def characters(self, content):
    if self.tags == ['mediawiki', 'page', 'revision', 'model']:
      self.model += content
    if self.tags == ['mediawiki', 'page', 'revision', 'format']:
      self.format += content
    if self.tags == ['mediawiki', 'page', 'revision', 'text']:
      self.text += content

  def getSentences(self, text):
    text = regex.sub(r'<!--(.*?)-->', '', text)
    text = regex.sub(r'</?[a-z]+[^>]*>', '', text)
    text = regex.sub(r'\[\[Image:(.*?)\]\]', '', text)
    text = regex.sub(r'\[\[File:(.*?)\]\]', '', text)
    text = regex.sub(r'\[\[Category:(.*?)\]\]', '', text)
    text = regex.sub(r'\[\[(.*?)(\|.*?)?\]\]', r'\1', text)
    text = regex.sub(r'\[http(.*?)\]', r'', text)
    text = regex.sub(r'{{.*?}}', '', text)
    text = regex.sub(r'\|.*?}}', '', text)
    text = regex.sub(r"''+", '', text)
    text = regex.sub(r'\]\]', '', text)
    text = regex.sub(r'}}', '', text)
    text = regex.sub(r'&[a-zA-Z0-9]+;', '', text)
    text = regex.sub(r'[\r]', '@@@', text)
    sentences = []
    for line in text.split('\n'):
      line = line.strip()
      if regex.search('^[{|;!]', line): continue
      if regex.search('^:*(Image|File|Category):', line): continue
      line = regex.sub(r'^==+', '', line)
      line = regex.sub(r'==+$', '', line)
      line = regex.sub(r'^[#*:]+', '', line)
      line = regex.sub(r'\s+', ' ', line)
      line = line.strip()
      if line:
        sentences.append(line)
    return sentences


def main():
  sampling_ratio = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
  max_outputs = int(sys.argv[2]) if len(sys.argv) > 2 else sys.maxsize
  if sampling_ratio <= 0 or sampling_ratio > 1:
    raise ValueError("invalid sampling ratio")
  if max_outputs < 0:
    raise ValueError("invalid max articles")
  logger.info("Process started")
  parser = xml.sax.make_parser()
  handler = XMLHandler(sampling_ratio, max_outputs)
  parser.setContentHandler(handler)
  try:
    parser.parse(sys.stdin)
  except xml.sax.SAXException:
    pass
  logger.info("Process done")


if __name__=="__main__":
  main()
