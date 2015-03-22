#!/bin/bash

sudo python setup.py install 

python droptables.py
gemini load -v test/test.comp_het.vcf -p test/test.comp_het.ped --cores 4 --skip-cadd --skip-gerp-bp test/my.db

