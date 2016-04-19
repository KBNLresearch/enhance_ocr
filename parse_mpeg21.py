#!/usr/bin/python
# -*- coding: utf-8 -*-

##
## parse_mpeg21.py - generate new OCR for given DDD-identifier,
##                   also generate some comparison indicators.
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

import json
import os
import Queue
import string
import sys
import threading
import urllib

from lxml import etree

OAI_DDD_BASEURL = "http://services.kb.nl/mdo/oai"
OAI_DDD_BASEURL += "?verb=GetRecord&&metadataPrefix=didl&identifier=DDD:"

# Parameters: ddd_identifier, ddd_identifier, w, h, x, y.
IMAGE_SERVICE = 'http://imageviewer.kb.nl/ImagingService/imagingService'
IMAGE_SERVICE += '?id=%s:image&coords=%s:alto&w=%s&s=1&h=%s&x=%s&y=%s'

DC_IDENTIFIER = '{http://purl.org/dc/elements/1.1/}identifier'

# Any page that has confidence > 0.8 will be skipped.
CONFIDENCE_TRASHOLD = 0.8

# Nr of threads for retrieving new and old OCR.
MAX_NUM_THREAD = 20

# Nr of articles to process at max.
MAX_NUM_ARTICLES = 6

# OCR service baseurl.
OCR_BASEURL = 'http://kbresearch.nl/ocr/?lang=nld&imageurl='

# Working with a global that is filled from threads,
# this requires some locking.
lock = threading.Lock()

# Global variable to recieve results, used in several threads.
global result
result = {}


def zone_to_url(imagename, zone):
    """ Transform a zone dict to a image URL"""
    url = IMAGE_SERVICE % (imagename, imagename.replace(':image', ':alto'),
                           zone.get('width'), zone.get('height'),
                           zone.get('hpos'), zone.get('vpos'))
    return url


def get_articles_avail(data):
    """ Fetch all availaible ocr files from the DIDL"""
    articles = []
    count = 0
    for f in data.iter():
        if f.attrib.get('ref') and f.attrib.get('ref').endswith(':ocr'):
            articles.append(f.attrib.get('ref'))
            count += 1
    return articles


def get_zones_avail(data):
    """ Fetch all availaible (article) zones from a DIDL"""
    zones = {}
    count = -1
    ocr_confidencelevel = ''

    for f in data.iter():
        if (f.text and f.tag and f.tag.endswith("OCRConfidencelevel")):
            ocr_confidencelevel = f.text

        identifier = f.attrib.get(DC_IDENTIFIER)

        if (identifier and
                identifier.endswith(':zoning') and not
                identifier.find(':p0') > -1):
                count += 1
                zones[count] = {"images": []}
                zone = {}

        if count >= 0:
            if f.attrib.get('pageid'):
                current_pageid = f.attrib.get('pageid')
            if f.attrib.get('width'):
                zone['width'] = f.attrib.get('width')
            if f.attrib.get('height'):
                zone['height'] = f.attrib.get('height')
            if f.attrib.get('vpos'):
                zone['vpos'] = f.attrib.get('vpos')
            if f.attrib.get('hpos'):
                zone['hpos'] = f.attrib.get('hpos')

            if (zone.get('width') and zone.get('height') and zone.get('vpos')
                    and zone.get('hpos')):
                if (not zone_to_url(current_pageid, zone) in
                        zones[count]["images"]):
                    zones[count]["images"].append(zone_to_url(
                                                  current_pageid, zone))
                    zone = {}

    return zones, ocr_confidencelevel


def fetch_new_ocr(ocr_url, image_url, order, max_count):
    """ Retrieve the new OCR data, add it to global result (with lock)"""
    global result

    if not ocr_url in result:
        with lock:
            result[ocr_url] = {}
            result[ocr_url]["new_ocr"] = [''] * max_count
            result[ocr_url]["zones"] = [''] * max_count
            result[ocr_url]["new_ocr_stats"] = {}

        old_ocr_data = urllib.urlopen(ocr_url).read()
        old_ocr_data = etree.fromstring(old_ocr_data)
        old_ocr_data = "\n".join([e.text for e in old_ocr_data if e.text])
        old_ocr_stats = count_char(old_ocr_data)

        new_ocr_data = urllib.urlopen(image_url).read()
        new_ocr_stats = count_char(new_ocr_data)

        for item in new_ocr_stats:
            if item in result[ocr_url]["new_ocr_stats"]:
                result[ocr_url]["new_ocr_stats"][item] += new_ocr_stats[item]
            else:
                result[ocr_url]["new_ocr_stats"][item] = new_ocr_stats[item]

        with lock:
            result[ocr_url]["new_ocr"][order] = new_ocr_data
            result[ocr_url]["old_ocr"] = old_ocr_data
            result[ocr_url]["old_ocr_stats"] = old_ocr_stats
            result[ocr_url]["zones"][order] = image_url

            result[ocr_url]["new_ocr_stats"] = new_ocr_stats
    else:
        new_ocr_data = urllib.urlopen(image_url).read()
        new_ocr_stats = count_char(new_ocr_data)
        with lock:
            for item in new_ocr_stats:
                if item in result[ocr_url]["new_ocr_stats"]:
                    result[ocr_url]["new_ocr_stats"][item] += \
                        new_ocr_stats[item]
                else:
                    result[ocr_url]["new_ocr_stats"][item] = \
                        new_ocr_stats[item]

            result[ocr_url]["new_ocr"][order] = new_ocr_data
            result[ocr_url]["zones"][order] = image_url


def parse_didl(raw_data):
    """ Parse the DIDL file, extract zones, articles and OCR confidence"""

    if isinstance(raw_data, str):
        xml = etree.fromstring(raw_data)
    else:
        xml = raw_data

    zones, ocr_confidencelevel = get_zones_avail(xml)
    articles = get_articles_avail(xml)
    combined = {}

    for counter, zone in enumerate(zones):
        if counter >= len(articles):
            continue
        combined[articles[counter]] = zones.get(zone)

    if float(ocr_confidencelevel) > CONFIDENCE_TRASHOLD:
        return {"error": "OCR confidencelevel > 0.8"}

    return combined


def process_threads(thread_pool, current_nr_threads):
    """ Processing running threads, and close them when done"""
    for (thread_nr, thread) in enumerate(thread_pool):
        if not thread.is_alive():
            current_nr_threads -= 1
            thread.join()
            thread_pool.pop(thread_nr)

    return current_nr_threads


def count_char(raw_data):
    """ Do some basic measurements on the string"""
    stats = {}
    stats["nr_char"] = len(raw_data)

    stats["nr_ascii_letters"] = len([l for l in raw_data if
                                    l in string.ascii_letters])

    stats["nr_punction"] = len([l for l in raw_data if
                                l in string.punctuation])

    stats["nr_digits"] = len([l for l in raw_data if
                              l in string.digits])
    return stats


def main(data, article_start, article_end):
    didl = parse_didl(data)

    processing_que = Queue.Queue()

    article_range = range(article_start, article_end)

    for ocr in didl:
        if ocr == "error":
            #print json.dumps(didl)
            #sys.exit(-1)
            return json.dumps(didl)

        max_count = len(didl.get(ocr).get("images"))

        for counter, image in enumerate(didl.get(ocr).get("images")):
            if not ocr.split(':')[-2] in ["a%04i" % i for i in article_range]:
                continue
            new_ocr_url = OCR_BASEURL + image
            processing_que.put([ocr, new_ocr_url, counter, max_count])

    current_nr_threads = 0
    thread_pool = []

    while processing_que.qsize() > 0:
        if current_nr_threads < MAX_NUM_THREAD:
            t = threading.Thread(target=fetch_new_ocr,
                                 args=(processing_que.get()))
            processing_que.task_done()
            t.daemon = True
            t.start()
            thread_pool.append(t)
            current_nr_threads += 1

        current_nr_threads = process_threads(thread_pool,
                                             current_nr_threads)

    while current_nr_threads > 0:
        current_nr_threads = process_threads(thread_pool,
                                             current_nr_threads)

    processing_que.join()
    return json.dumps(sorted(result.items(), key=lambda x: x[1]))


def webwrapper(identifier):
    json = ""

    article_start = int(identifier.split(':')[-1][1:])
    article_end = article_start + MAX_NUM_ARTICLES

    identifier = OAI_DDD_BASEURL + ":".join(identifier.split(':')[:-1])

    data = urllib.urlopen(identifier).read()
    json = main(data, article_start, article_end)
    return json

if __name__ == "__main__":
    path = os.path.join(os.sep.join(
                        os.path.abspath(__file__).split(os.sep)[:-1]))

    article_start = 1
    article_end = article_start + MAX_NUM_ARTICLES

    if len(sys.argv) > 1:
        didl = sys.argv[1]
        if didl.startswith('ddd:'):
            if didl.split(':')[-1].startswith('a'):
                article_start = int(didl.split(':')[-1][1:])
                article_end = article_start + MAX_NUM_ARTICLES
                #article_end = int(didl.split(':')[-1][1:]) + 20
                didl = ":".join(didl.split(':')[:-1])
                #print("PARSING from " + str(article_start) +
                #" to " + str(article_end) + " with didl " + didl)
            didl = OAI_DDD_BASEURL + didl

        try:
            data = urllib.urlopen(didl).read()
        except:
            print("Could not read data from: " + sys.argv[1], didl)
            print("Try something like: " + __file__ +
                  " 'http://services.kb.nl/mdo/oai?verb=GetRecord&" +
                  "identifier=DDD:ddd:010128511:mpeg21&metadataPrefix" +
                  "=didl' or ddd:110564088:mpeg21:a0001")
            sys.exit(-1)
    else:
        fh = open(path + os.sep + 'didl_voorbeeld.mpg21', 'r')
        data = fh.read()
        fh.close()
        print("No DIDL identifier specified, \
                using example file from disk.\n\n")

    main(data, article_start, article_end)
