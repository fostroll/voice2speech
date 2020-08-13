#!/bin/sh
unbuffer python ./decode_chunks.py 2>&1 > chunks_1.txt &
