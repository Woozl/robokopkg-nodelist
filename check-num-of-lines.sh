#!/bin/bash

# sum lines of every file in a directory

# `chmod +x check-num-of-lines.sh`
# `./check-num-of-lines <path to dir>`

total=0
find $1 -type f -print0 | xargs -0 awk '{ total += length } END { print total }'