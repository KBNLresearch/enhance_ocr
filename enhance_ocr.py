#!/usr/bin/python
# -*- coding: utf-8 -*-

##
## enhance_ocr.py - generate new OCR for given DDD-identifier,
##                  also generate some comparison indicators.
##
## copyright (c) 2016 Koninklijke Bibliotheek -
##                    National library of the Netherlands.
##                    author: Willem Jan Faber.
##
## this program is free software: you can redistribute it and/or modify
## it under the terms of the gnu general public license as published by
## the free software foundation, either version 3 of the license, or
## (at your option) any later version.
##
## this program is distributed in the hope that it will be useful,
## but without any warranty; without even the implied warranty of
## merchantability or fitness for a particular purpose. see the
## gnu general public license for more details.
##
## you should have received a copy of the gnu general public license
## along with this program. if not, see <http://www.gnu.org/licenses/>.
##

import lxml
import os
import sys

from flask import Flask, request, Response
from kb.nl.api import oai


# Add the current path to the python lib-path.
sys.path.append(os.path.dirname(__file__))
import parse_mpeg21


records = oai.list_records("DDD")

sys.path.append(os.path.dirname(__file__))
application = Flask(__name__)
application.debug = True

IMG_URL = ("http://imageviewer.kb.nl/ImagingService/imagingService" +
           "?id=%s:image&coords=%s:alto&w=%i&colour=fefe56&s=1&x=%i&h=%i&y=%i")
OCR_URL = "http://ocr.kbresearch.nl/?imageurl="

DEBUG = False

DEBUG_MAX = 8

USAGE = "Usage: ?identifier= for example ?identifier=ddd:110564088:mpeg21:a001"


def alto_to_text(alto):
    xml = lxml.etree.fromstring(alto)
    return "\n".join([f.text for f in xml.iter() if f.text]).strip()


@application.route('/')
def generate_result(identifier="ddd:010168412:mpeg21"):
    if not request.args.get("identifier"):
        return USAGE

    identifier = request.args.get("identifier")

    if not identifier.split(':')[-1].startswith('a'):
        return "Error, identifier does not end with :a00[1-9]"

    #record = oai.get("DDD:" + ":".join(identifier.split(':')[:-1]))
    #data = record.record_data

    response_json = parse_mpeg21.webwrapper(identifier)

    resp = Response(response_json, status=200, mimetype="application/json")
    return resp
