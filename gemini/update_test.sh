#!/bin/bash

sudo python setup.py install 

python droptables.py
gemini load -v test/test.comp_het.vcf --skip-cadd --skip-gerp-bp --test-mode test/my.db

