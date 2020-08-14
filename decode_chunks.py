#!/usr/bin/python3
# -*- coding: utf-8 -*-

import multiprocessing as mp
import os
import re
import subprocess as sp
import sys
from uuid import uuid4

USE_CLOUD = False

servers = ['192.168.0.1:7071', '192.168.0.1:7072', '192.168.0.1:7073']
cloud = {'address': 'asr.yandex.net',
         'uuid': 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
         'key': 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'}
backup_dirs = ['chunks_0', 'chunks_1', 'chunks_2']

chunks_dir = 'chunks'
chunks_ext = 'wav'
tmp_dir = 'tmp'
map_file = 'make_chunks.map'
res_file = 'chunks.txt'
rx = re.compile('\s*<variant confidence="(.+?)">(.+?)</variant>')
rx_fname = re.compile('.*chunks_')
def get_chunk_text (lock, server_id, files_to_process):
    backup_dir, files = backup_dirs[server_id], files_to_process[server_id]
    assert os.path.isdir(backup_dir), \
        "ERROR: directory {} doesn't exist".format(backup_dir)
    #pid = os.getpid()
    pid_fn = '{}/{}.pid'.format(tmp_dir, server_id)
    if os.path.isfile(pid_fn):
        os.remove(pid_fn)
    res_fn = '{}/{}_{}'.format(tmp_dir, server_id, res_file)
    #lock.acquire()
    # ... synchronous operations ...
    #lock.release()
    file_no = 0
    assert not os.path.isfile(res_fn), \
           'ERROR: file {} is already exist'.format(res_fn)
    with open(res_fn, 'wt', encoding='utf-8') as f_res:
        for fn in files:
            map_fn = fn + '.map'
            sp.call(['mv', os.path.join(chunks_dir, map_fn), tmp_dir])
            files, files_meta = [], []
            with open(os.path.join(tmp_dir, map_fn), 'rt') as f_map:
                for line in f_map:
                    line = line.rstrip()
                    if line and line[0] == '\t':
                        meta = line.lstrip().split()
                        sound_file = '{}_{}_{}.{}' \
                                         .format(fn, meta[2], meta[3], chunks_ext)
                        files.append(sound_file)
                        files_meta.append(meta)
                        sp.call(['mv', os.path.join(chunks_dir, sound_file), tmp_dir])

            chunks_text = ''
            for sound_file, meta in zip(files, files_meta):
                uuid = "%032x" % file_no
                output = ''
                with sp.Popen(
                    args=[
                        'curl',
                        '-X', 'POST',
                        ('http://{}/asr_xml?uuid={}&key={}&topic=freeform'
                                          '&lang=ru-RU&disableAntimat=true')
                            .format(*((cloud['address'], cloud['uuid'],
                                       cloud['key']) if USE_CLOUD else
                                      (servers[server_id], uuid4().hex,
                                       'internal'))),
                        '-H', 'Content-Type: audio/x-wav',
                        '--data-binary', '@' + os.path.join(tmp_dir,
                                                            sound_file)
                    ],
                    stdout=sp.PIPE,
                    stderr=sp.STDOUT
                ) as p:
                    for line in p.stdout.readlines():
                        output += line.decode('utf8',
                                              errors='backslashreplace') \
                                      .replace('\r', '')

                success = False
                chunk_text = ''
                confidence = None
                for line in output.split('\n'):
                    if success:
                        result = rx.match(line)
                        if result:
                            res_conf = float(result.group(1))
                            if result and (confidence == None
                                        or res_conf > confidence):
                                chunk_text = result.group(2)
                                confidence = res_conf
                    if line.startswith('<recognitionResults success="1">'):
                        success = True
                chunks_text += fn + '\t' + '\t'.join(meta) + '\t' \
                             + chunk_text + '\n'
                file_no += 1
            print(chunks_text, file=f_res)
            sp.call(['mv', os.path.join(tmp_dir, map_fn), backup_dir])
            for sound_file in files:
                sp.call(['mv', os.path.join(tmp_dir, sound_file), backup_dir])
    with open(pid_fn, 'wt') as f:
        pass

if __name__ == '__main__':
    files = []
    with open(map_file, 'rt') as f:
        for line in f:
            line = line.rstrip()
            if line and line[0] != '\t':
                if os.path.isfile(os.path.join(chunks_dir, line + '.map')):
                    files.append(line)
    files.sort()
    files_to_process = []
    for rate in range(len(servers), 0, -1):
        cnt = len(files) // rate
        files_to_process.append(files[:cnt])
        files = files[cnt:]

    lock = mp.Lock()
    procs = []
    server_ids = list(range(len(backup_dirs if USE_CLOUD else servers)))

    for server_id in server_ids:
        p = mp.Process(target=get_chunk_text,
                       args=(lock, server_id, files_to_process))
        p.start()
        procs.append(p)
    for p in procs:
        p.join()

    err = False
    for server_id in server_ids:
        pid_fn = '{}/{}.pid'.format(tmp_dir, server_id)
        if not os.path.isfile(pid_fn):
            print('ERROR: process {} was terminated'.format(server_id),
                  file=sys.stderr)
            err = True
    if not err:
        with open(res_file, 'at', encoding='utf-8') as f_res:
            for server_id in server_ids:
                res_fn = '{}/{}_{}'.format(tmp_dir, server_id, res_file)
                with open(res_fn, 'rt', encoding='utf-8') as f:
                    for line in f:
                        print(line, end='', file=f_res)
