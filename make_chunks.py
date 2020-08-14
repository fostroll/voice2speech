#!/usr/bin/python3
# -*- coding: utf-8 -*-

import glob
import multiprocessing as mp
import os
import re
import subprocess as sp
import sys

procs = int(sys.argv[1]) if len(sys.argv) > 1 else 1
if procs == 0: procs = mp.cpu_count() + 1

INCLUDE_PATH = False

input_file_masks = ['wav/*.wav']
output_dir = 'chunks'
tmp_dir = './tmp'
map_fn = sys.argv[0].replace('.py', '.map')
log_fn = sys.argv[0].replace('.py', '.log')
first_chunks_qnt = None
silence_threshold = -32 #dB
min_silence_len = 1.0 # sec
keep_silence = 0.5 # sec
min_chunk_len = 2.0 # sec (w/o keep_silence)

rx_silence_start = \
    re.compile('^\[silencedetect \@ 0x[0-9a-f]+\] silence_start: (\d+\.\d+)')
rx_silence_end = \
    re.compile('^\[silencedetect \@ 0x[0-9a-f]+\] silence_end: (\d+\.\d+)')
def get_first_chunks(sound_file):
    map_line = None
    log_line = sound_file + ': '
    path, sound_file_for_log = os.path.split(sound_file)
    sound_file_for_log = sound_file_for_log.rsplit(sep='.', maxsplit=1)[0]
    if INCLUDE_PATH:
        drive, path = os.path.splitdrive(path)
        path = path.replace(os.path.sep, '|')
        if drive:
            path = drive + ':' + path
        sound_file_for_log = path + '|' + sound_file_for_log
    sound_file_map_fn = output_dir + '/' + sound_file_for_log + '.map'

    print(sound_file_for_log)
    if not os.path.isfile(sound_file_map_fn):
        map_data = []
        chunk_fn = output_dir + '/' + sound_file_for_log + '_{}_{}.wav'
        pid = os.getpid()
        #stdoutdata = sp.getoutput('command')
        #print('stdoutdata: ' + stdoutdata.split()[0])
        fn = '{}/{}'.format(tmp_dir, pid)
        fns_wav = {'FR': fn + '_FR.wav', 'FL': fn + '_FL.wav'}
        fn_log = fn + '.log'
        sp.call(['ffmpeg',
            '-hide_banner', '-i', sound_file,
            '-filter_complex', 'channelsplit=channel_layout=stereo[FR][FL]',
            '-map', '[FR]', fns_wav['FR'], '-map', '[FL]', fns_wav['FL']
        ])#, stderr=sp.DEVNULL)
        for i, (channel, fn_wav) in enumerate(fns_wav.items()):
            if i != 0:
                log_line += '; '
            log_line += channel + ' - '
            if os.path.isfile(fn_wav):
                with open(fn_log, 'wt') as f:
                    sp.call(['ffmpeg',
                        '-hide_banner',
                        '-i', fn_wav,
                        '-af', 'silencedetect=noise={}dB:d={}'
                                   .format(silence_threshold,
                                           min_silence_len),
                        '-f', 'null',
                        '-'
                    ], stderr=f)
                if os.path.isfile(fn_log):
                    with open(fn_log, 'rt') as f:
                        chunk_no = 0
                        silence_start = silence_end = 0
                        for line in f:
                            silence_start_ = rx_silence_start.search(line)
                            if silence_start_:
                                silence_start = float(silence_start_.group(1))
                                if silence_start > silence_end:
                                    sp.call(['ffmpeg',
                                        '-hide_banner',
                                        '-ss', str(silence_end
                                                 - keep_silence),
                                        '-t', str(silence_start
                                                - silence_end
                                                + keep_silence
                                                + keep_silence),
                                        '-i', fn_wav,
                                        chunk_fn.format(channel, chunk_no)
                                    ])#, stderr=sp.DEVNULL)
                                    map_data.append(
                                        (silence_end, silence_start,
                                         channel, chunk_no)
                                    )
                                    chunk_no += 1
                                    if first_chunks_qnt \
                                   and chunk_no >= first_chunks_qnt:
                                        break
                            else:
                                silence_end_ = rx_silence_end.search(line)
                                if silence_end_:
                                    silence_start = silence_end = \
                                        float(silence_end_.group(1))
                                elif line.startswith('Output file is empty'):
                                    break
                        else:
                            if silence_start == silence_end:
                                sp.call(['ffmpeg',
                                    '-hide_banner',
                                    '-ss', str(silence_end - keep_silence),
                                    '-i', sound_file,
                                    chunk_fn.format(channel, chunk_no)
                                ])#, stderr=sp.DEVNULL)
                                map_data.append((silence_end, float('inf'),
                                                 channel, chunk_no))
                                chunk_no += 1
                    log_line += 'OK: saved ' + str(chunk_no) + ' chunks' \
                                    if chunk_no > 0 else \
                                'no chunks found'
                    os.remove(fn_log)

                else:
                    log_line += 'silencedetect error'

                os.remove(fn_wav)
            else:
                log_line += 'map_channel error'
        if map_data:
            map_line = sound_file_for_log + '\n'
            for data in sorted(map_data):
                map_line += '\t{} {} {} {}\n'.format(*data)
            with open(sound_file_map_fn, 'wt') as f:
                print(map_line, file=f)
    else:
        log_line += 'already exists'

    return map_line, log_line

if __name__ == '__main__':
    with open(map_fn, 'at') as f_map, open(log_fn, 'at') as f_log:
        for input_file_mask in input_file_masks:
            with mp.Pool(procs) as pool:
                for map_line, log_line in pool.imap(
                    get_first_chunks,
                    glob.glob(input_file_mask, recursive=False)
                ):
                    if map_line:
                        print(map_line, file=f_map)
                    print(log_line, flush=True, file=f_log)
