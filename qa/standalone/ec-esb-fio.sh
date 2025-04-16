#!/bin/bash
set -ex

# Install FIO
if [[ -f /etc/debian_version ]]; then
    sudo apt-get update
    sudo apt-get install -y fio
elif [[ -f /etc/redhat-release ]]; then
    sudo yum install -y fio
else
    echo "Unsupported OS"
    exit 1
fi

# Configure EC pool
ceph osd erasure-code-profile set myecprofile k=2 m=1
ceph osd pool create ecpool 16 16 erasure myecprofile
ceph osd pool set ecpool allow_ec_overwrites true

# FIO configuration
fio_file=$(mktemp -t ec-esb-fio-XXXX)
cat > $fio_file <<EOF
[global]
ioengine=rados
pool=ecpool
clientname=admin
conf=/etc/ceph/ceph.conf
time_based=1
runtime=3600
invalidate=0
direct=1
iodepth=64
numjobs=8
group_reporting=1
rw=randwrite
file_service_type=pareto:0.20:0

[variable-writes]
bssplit=4k/16:8k/10:12k/9:16k/8:20k/7:24k/7:28k/6:32k/6:36k/5:40k/5:44k/4:48k/4:52k/4:56k/3:60k/3:64k/3
size=50G
nrfiles=12500
filename_format=stress_obj.\$jobnum.\$filenum
EOF

status_log() {
    echo "Cluster status on failure:"
    ceph -s
    ceph health detail
    ceph osd tree
    ceph osd df
}

echo "[ec-esb-fio] Starting FIO test..."

# Run FIO in background
fio $fio_file  &
FIO_PID=$!

# Monitor ceph health for OSD down states
TIMEOUT=3600
START_TIME=$(date +%s)
while true; do
    if ! ps -p $FIO_PID > /dev/null; then
        echo "FIO process terminated early, checking cluster status"
        status_log
        exit 1
    fi
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED -ge $TIMEOUT ]; then
        echo "Reached 1-hour timeout, stopping FIO"
        kill -9 $FIO_PID
        wait $FIO_PID 2>/dev/null
        break
    fi
    if ceph health detail | grep -i "osd.*down"; then
        echo "Detected OSD down state:"
        ceph health detail | grep -i "osd.*down"
        kill -9 $FIO_PID
        wait $FIO_PID 2>/dev/null
        status_log
        exit 1
    fi
    sleep 10
done

echo "[ec-esb-fio] FIO test completed, log checks to follow"
