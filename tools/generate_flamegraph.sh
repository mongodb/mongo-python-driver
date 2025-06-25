#!/bin/bash
set -eu

sudo py-spy record -o ${1:-profile}.svg -r ${2:-2000} -- python $3
