#!/bin/bash -ex

# Cross-namespace copy test using the NVMe Simple Copy command (TP 4065).
#
# The test connects to the NVMe-oF gateway, discovers the second NVMe device
# (sorted by namespace index), then issues an `nvme copy` command that copies
# two LBA ranges from NSID 1 into that device's destination LBA offset.
#
# Expected outcome: the nvme-cli reports "NVMe Copy: success".

source /etc/ceph/nvmeof.env

SPDK_CONTROLLER="Ceph bdev Controller"

# Pick the second device (index 2) sorted by namespace number.
# nvmeof_namespaces.yaml connects all namespaces so there are many devices
# available; we just need any valid second one for the copy destination.
target_device=$(sudo nvme list --output-format=json |
    jq -r '.Devices | sort_by(.NameSpace) | .[1] | select(.ModelNumber == "'"$SPDK_CONTROLLER"'") | .DevicePath')

if [ -z "$target_device" ]; then
    echo "[nvmeof.copy] ERROR: could not find a second NVMe device to use as copy destination"
    sudo nvme list
    exit 1
fi

echo "[nvmeof.copy] Using target device: $target_device"

copy_test() {
    output=$(sudo nvme copy "$target_device" \
        --sdlba=1000 \
        --slbs=5000,9000 \
        --blocks=99,199 \
        --snsids=1,1 \
        --format=2 2>&1)
    echo "$output"
    if ! echo "$output" | grep -q "NVMe Copy: success"; then
        echo "[nvmeof.copy] copy_test FAILED — expected 'NVMe Copy: success' in output"
        sudo dmesg -T > "$TESTDIR/archive/dmesg-copy_test.log" 2>/dev/null || true
        return 1
    fi
}

echo "[nvmeof.copy] Running NVMe copy test..."
copy_test
echo "[nvmeof.copy] NVMe copy test passed!"
