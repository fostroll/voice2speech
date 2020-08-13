#!/usr/bin/python3
# -*- coding: utf-8 -*-

import re, sys

log_fn = 'make_chunks.log'

rx_ok = re.compile('OK: saved \d+ chunks$')
with open(log_fn, 'rt') as f:
    for line in f:
        if not rx_ok.search(line):
            print(line)
