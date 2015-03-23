#!/bin/bash

sudo python setup.py install 


python droptables.py
gemini load --skip-gene-tables --test-mode -v test/test.query.vcf --cores 5 --skip-cadd --skip-gerp-bp -t snpEff test/my.db

