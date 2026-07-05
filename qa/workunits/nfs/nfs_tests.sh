#!/bin/sh

set -ex

HERE=$(dirname "$0")
PY=${PYTHON:-python3}
if [ "${NFS_REUSE_VENV}" ]; then
    VENV="${NFS_REUSE_VENV}"
else
    VENV=${HERE}/"_nfs_tests_$$"
fi

cleanup() {
    if [ "${NFS_REUSE_VENV}" ]; then
        return
    fi
    rm -rf "${VENV}"
}

if ! [ -d "${VENV}" ]; then
    $PY -m venv "${VENV}"
fi
trap cleanup EXIT

cd "${HERE}"
"${VENV}/bin/${PY}" -m pip install pytest pyyaml
"${VENV}/bin/${PY}" -m pytest -v "$@"
