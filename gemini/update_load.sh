#!/bin/bash

sudo python setup.py install 

clear

python droptables.py
#gemini load --skip-gene-tables --test-mode -ks 'test_query' -v test/test.query.vcf --cores 4 --skip-cadd --skip-gerp-bp -t snpEff

gemini load --skip-gene-tables --test-mode -ks load_test_keyspace -v test/test.comp_het.vcf --cores 3 --skip-gerp-bp --skip-cadd -t snpEff -p test/test.comp_het.ped
