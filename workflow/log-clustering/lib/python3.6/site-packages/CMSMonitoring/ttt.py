#!/usr/bin/env python
import subprocess

cmd='/Users/vk/Work/Languages/Go/gopath/bin/nats-pub'
subject = 'hello'
server = 'nats://127.0.0.1:4222'
def send(cmd, server, subject, msg):
    pcmd = '{} -s {} {} {}'.format(cmd, server, subject, msg)
    print('pcmd', pcmd)
    proc = subprocess.Popen(pcmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    proc.wait()
for i in range(1000):
    msg = 'msg-{}'.format(i)
    send(cmd, server, subject, msg)
