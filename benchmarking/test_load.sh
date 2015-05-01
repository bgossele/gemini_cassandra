#!/bin/bash

export n_cores=3
export db_ip="127.0.0.1"
export iterations=1
touch res/load_measurements.csv

c=1
while [ $c -le $iterations ]
do
	geminicassandra load -v ExAC.r0.3.sites.vep.vcf.gz --skip-gerp-bp --skip-cadd -db $db_ip -ks test_dink_db --cores $n_cores

        #geminicassandra load -v ../test/test.query.vcf --skip-gerp-bp --skip-cadd -t snpEff -db $db_ip -ks test_query_db --cores $n_cores --timing-log res/load_measurements.csv --exp-id "dinkie"
	(( c++ ))
done
 
