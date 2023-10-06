#!/bin/bash

if [ -d dist/ ]; then rm -r dist/; fi
python -m pip install --upgrade build twine
python -m build