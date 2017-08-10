#!/bin/bash
#
# Run send/receiver to generate session clash
#
SUFFIX=j

# wireshark catches 4000 frames, dumpcap takes a while to start up so delay
dumpcap -i any -B 1000 -w /home/chug/Work/adverb/STR-t1t2-$SUFFIX.pcapng -c 4000 &
pidof_dumpcap=$!
sleep 2

# 10 large, 500 small
./send "" amqp t1 10 560000 t2 10 280000 > STR-t1t2-send2-$SUFFIX.csv &
pidof_send=$!

ls -al

# n millisecond delay 
./receive "" amqp t1 10  1000 &
pidof_receivelarge=$!

# sleep so the adverb colors are the same on each run
ls -al

./receive "" amqp t2 10   100 &
pidof_receivesmall=$!

echo "pid dumpcap       = $pidof_dumpcap"
echo "pid send          = $pidof_send"
echo "pid receivelarge  = $pidof_receivelarge"
echo "pid receivesmall  = $pidof_receivesmall"
