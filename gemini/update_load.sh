#!/bin/bash

sudo python setup.py install 


#python droptables.py
#gemini load --skip-gene-tables --test-mode -v test/test.query.vcf --cores 5 --skip-cadd --skip-gerp-bp -t snpEff test/my.db

gemini load --skip-gene-tables --test-mode -v test/test.comp_het.vcf --skip-gerp-bp --skip-cadd -t snpEff -p test/test.comp_het.ped test/test.comp_het.db
