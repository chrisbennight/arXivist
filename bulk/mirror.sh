#!/bin/bash


if [ ! -d "venv" ]
then
    echo "Creating venv and installing s3cmd."
    python3 -m venv venv
    source venv/bin/activate
    pip install -U pip setuptools
    pip install s3cmd
    s3cmd --configure
fi

s3cmd sync --requester-pays s3://arxiv s3://arxivist