#!/bin/sh

unbuffer python ./decode_chunks.py 2>&1 > decode_chunks.log &
