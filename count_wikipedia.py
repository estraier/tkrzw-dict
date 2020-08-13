#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Script to count the number of articles in a Wikipedia XML stream
#
# Usage:
#   count_wikipedia.py [--quiet]
#   (It reads the standard input and prints the result on the standard output.)
# 
# Example:
#   $ bzcat enwiki-20200701-pages-articles-multistream.xml.bz2 |
#     ./count_wikipedia.py
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
import sys
import tkrzw_dict
import xml.sax
import xml.sax.handler


logger = tkrzw_dict.GetLogger()


class XMLHandler(xml.sax.handler.ContentHandler):
  def __init__(self):
    self.count = 0
    self.tags = []
    self.is_redirect = False
    self.model = None
    self.format = None
    
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
    if self.tags == ['mediawiki', 'page', 'revision', 'format']:
      self.format = ""

  def endElement(self, name):
    if self.tags == ['mediawiki', 'page', 'revision']:
      if (not self.is_redirect and not self.has_restrictions and
          self.model == 'wikitext' and self.format == 'text/x-wiki'):
        self.count += 1
        if self.count % 1000 == 0:
          logger.info("Progress: {}".format(self.count))
      self.model = None
      self.format = None
    self.tags.pop()

  def characters(self, content):
    if self.tags == ['mediawiki', 'page', 'revision', 'model']:
      self.model += content
    if self.tags == ['mediawiki', 'page', 'revision', 'format']:
      self.format += content

  def getCount(self):
    return self.count
	

def main():
  args = sys.argv[1:]
  if tkrzw_dict.GetCommandFlag(args, "--quiet", 0):
    logger.setLevel(logging.ERROR)
  if args:
    raise RuntimeError("unknown arguments: {}".format(str(args)))
  logger.info("Process started")
  parser = xml.sax.make_parser()
  handler = XMLHandler()
  parser.setContentHandler(handler)
  parser.parse(sys.stdin)
  print(handler.getCount(), flush=True)
  logger.info("Process done")


if __name__=="__main__":
  main()
