#! /bin/bash

# Script to generate IO workload for a local dir.
#
# Better to copy and run this script on the node where IO workload has to be
# generated than to generate IO workload via a remote node.

set -e

# WR_TH_LIM=12
# WR_TH_FILE_SIZE=1024
# WR_TH_FILE_LIM=20000
#
# WR_TH_SLEEP=1
#
# DIR_PFX="dir1"
# FILE_PFX="file-"
#
# DEPTH=4
# BR_FACTOR=3

test -v WR_TH_LIM
test -v WR_TH_FILE_SIZE
test -v WR_TH_FILE_LIM

test -v WR_TH_SLEEP

test -v DIR_PFX
test -v FILE_PFX

test -v DEPTH
test -v BR_FACTOR

WR_TH_STR=$(python -c "print('a' * $WR_TH_FILE_SIZE)")

echo WR_TH_LIM = $WR_TH_LIM
echo WR_TH_FILE_SIZE = $WR_TH_FILE_SIZE
echo WR_TH_FILE_LIM = $WR_TH_FILE_LIM

echo WR_TH_STR = $WR_TH_STR
echo WR_TH_SLEEP = $WR_TH_SLEEP

echo DIR_PFX = $DIR_PFX
echo FILE_PFX = $FILE_PFX

echo DEPTH = $DEPTH
echo BR_FACTOR = $BR_FACTOR


declare -A tree_paths


function mkcd() {
    mkdir $1 && cd $1
}


function _create_dir_hierarchy() {
    local tree_num=$1
    local curr_depth=$2
    local num=0

    for ((num=1; num<=$BR_FACTOR; ++num)); do
        mkcd "$DIR_PFX-$tree_num-$curr_depth-$num"

        if test $curr_depth -lt $DEPTH; then
            ((++curr_depth))
            _create_dir_hierarchy $tree_num $curr_depth
            ((--curr_depth))
        fi

        cd ..
    done
}


function create_dir_hierarchy() {
    local i=0
    for ((i=1; i <= $WR_TH_LIM; ++i)); do
        mkcd "$DIR_PFX-$i"

        tree_paths[$i]+=" $(pwd)"
        _create_dir_hierarchy $i 1

        cd ..
    done
}


function write_files() {
    local tree_num=$1
    local wp=$2
    local dir_pfx="$DIR_PFX-$tree_num"
    local SFX=$(basename $wp| grep --colour=never --only-matching [0-9-]*)
    local file_pfx="$FILE_PFX=$SFX"
    local num=0

    cd $wp
    for ((d=1; d <= $DEPTH; ++d)); do
        for ((b=1; b <= $BR_FACTOR; ++b)); do
            for ((num=1; num<=$WR_TH_FILE_LIM; ++num)); do
                cd $wp/"$dir_pfx-$d-$b"
                echo $WR_TH_STR > "$file_pfx-$num"
                sleep $WR_TH_SLEEP
                cd ..
            done
        done
    done
}


function launch_writer_threads() {
    echo launching write threads...
    local i=0
    for ((i=1; i <= $WR_TH_LIM; ++i)); do
        write_files $i ${tree_paths[$i]} &
    done
    echo finished launching all write threads
}


write_paths=""
create_dir_hierarchy
launch_writer_threads

# don't let this script quit since killing it is a convenient way to kill all
# the writer threads launched it
wait
