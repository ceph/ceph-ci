#!/usr/bin/env bash

echo "Custom fscrypt CLI setup begin"

set -xe

# Check and print kernel encryption support
zgrep -h ENCRYPTION /proc/config.gz /boot/config-$(uname -r) | sort | uniq || true
cat /proc/mounts || true

# Update the package manager cache
if command -v apt-get &>/dev/null; then
    sudo apt-get update -y
elif command -v yum &>/dev/null; then
    sudo yum makecache --refresh -y
else
    echo "Unsupported package manager. Exiting."
    exit 1
fi

# Remove any pre-installed fscrypt packages
if command -v apt-get &>/dev/null; then
    sudo apt-get remove --purge -y fscrypt || true
elif command -v yum &>/dev/null; then
    sudo yum remove -y fscrypt || true
fi

# Check if /tmp/fscrypt exists
if [ -d "/tmp/fscrypt" ]; then
    echo "/tmp/fscrypt exists. Cleaning up..."

    # Navigate to the folder
    cd /tmp/fscrypt || { echo "Failed to navigate to /tmp/fscrypt"; exit 1; }

    # Uninstall the existing installation
    if [ -f "Makefile" ]; then
        echo "Running 'sudo make uninstall'..."
        sudo make uninstall || echo "Uninstall failed or no uninstall target defined."
    else
        echo "Makefile not found. Skipping 'make uninstall'."
    fi

    # Navigate back
    cd || { echo "Failed to navigate back to home directory"; exit 1; }

    # Delete the directory
    echo "Deleting /tmp/fscrypt..."
    rm -rf /tmp/fscrypt
    echo "/tmp/fscrypt cleanup complete."
else
    echo "/tmp/fscrypt does not exist. No action needed."
fi

# Install required dependencies for building fscrypt
if command -v apt-get &>/dev/null; then
    sudo apt-get install -y build-essential libpam0g-dev libtinfo-dev golang
elif command -v yum &>/dev/null; then
    sudo yum install -y gcc make pam-devel ncurses-devel golang
fi

# Clone the custom fscrypt repository, build, and install
git clone https://github.com/salieri11/fscrypt.git -b wip-ceph-fuse /tmp/fscrypt
cd /tmp/fscrypt
make
sudo make install PREFIX=/usr/local

# Add /usr/local/bin to secure_path in sudoers
if ! sudo grep -q "/usr/local/bin" /etc/sudoers; then
    echo "Adding /usr/local/bin to secure_path in /etc/sudoers"
    sudo sed -i 's|^\(Defaults\s*secure_path="[^"]*\)"|\1:/usr/local/bin"|' /etc/sudoers
fi

# Verify installation
sudo fscrypt --help || echo "fscrypt installation or configuration failed."

echo "Custom fscrypt CLI setup done"
