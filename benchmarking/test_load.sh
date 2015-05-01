#!/bin/bash

export n_cores=2
export db_ip="127.0.0.1"
export iterations=1
touch res/load_res.csv

c=1
while [ $c -le $iterations ]
do
	geminicassandra load -v /data/bgossele/1KG/1KG_1_2GB.vcf.gz --skip-gerp-bp --skip-cadd -db $db_ip -ks 1_2GB_db --cores $n_cores --timing-log res/load_res.csv --exp-id "1KG_1.2GB"

	geminicassandra load -v /data/bgossele/1KG/1KG_3GB.vcf.gz --skip-gerp-bp --skip-cadd -db $db_ip -ks 3GB_db --cores $n_cores --timing-log res/load_res.csv --exp-id "1KG_3.0GB"

	geminicassandra load -v /data/bgossele/1KG/1KG_6_7GB.vcf.gz --skip-gerp-bp --skip-cadd -db $db_ip -ks 6_7GB_db --cores $n_cores --timing-log res/load_res.csv --exp-id "1KG_6.7GB"

	geminicassandra load -v /data/bgossele/1KG/1KG_11_8GB.vcf.gz --skip-gerp-bp --skip-cadd -db $db_ip -ks 11_8GB_db --cores $n_cores --timing-log res/load_res.csv --exp-id "1KG_11.8GB"

	(( c++ ))
done
 
