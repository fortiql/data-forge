#!/bin/bash
minio server /data --console-address ":9001" &
/usr/local/bin/setup-minio.sh
wait
