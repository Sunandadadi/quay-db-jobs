#!/bin/bash
set -exv

BASE_IMG="quayio-migration-jobs"

IMG="${BASE_IMG}:pr-check"

docker build -t "${IMG}" -f Dockerfile .
