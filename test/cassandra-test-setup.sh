
export n_cores=3
export buffer_size=50

geminicassandra load --skip-gene-tables --test-mode -v test.query.vcf --skip-gerp-bp --skip-cadd -t snpEff -db $cassandra_ips -ks test_query_db --cores $n_cores --buffer-size $buffer_size 
geminicassandra load --skip-gene-tables --test-mode -p test_extended_ped.ped -v test4.vep.snpeff.vcf  --skip-gerp-bp --skip-cadd -t snpEff -db $cassandra_ips -ks extended_ped_db --cores $n_cores
geminicassandra load --skip-gene-tables --test-mode -v test.snpeff.vcf --skip-gerp-bp --skip-cadd -t snpEff -db $cassandra_ips -ks test_snpeff_vcf_db --cores $n_cores
geminicassandra load --skip-gene-tables --test-mode -v test.exac.vcf --skip-gerp-bp --skip-cadd -db $cassandra_ips -ks test_exac_db --cores $n_cores 
geminicassandra load --skip-gene-tables --test-mode -v test3.snpeff.vcf --skip-gerp-bp --skip-cadd -db $cassandra_ips -ks test3_snpeff_db --cores $n_cores
geminicassandra load --skip-gene-tables --test-mode -v test.vcf_id.snpeff.vcf  --skip-gerp-bp --skip-cadd -t snpEff -db $cassandra_ips -ks test_vcf_id_snpeff_vcf_db --cores $n_cores
geminicassandra load --skip-gene-tables --test-mode -v test2.snpeff.vcf --skip-gerp-bp --skip-cadd -db $cassandra_ips -ks test2_snpeff_db --cores $n_cores
geminicassandra load --skip-gene-tables --test-mode -v test.clinvar.vcf --skip-gerp-bp --skip-cadd -db $cassandra_ips -ks test_clinvar_db
geminicassandra load --skip-gene-tables --test-mode -p test.de_novo.ped -v test.family.vcf  --skip-gerp-bp --skip-cadd -t snpEff -db $cassandra_ips -ks test_family_db
geminicassandra load --skip-gene-tables --test-mode -v test4.vep.snpeff.vcf --skip-gerp-bp --skip-cadd -t snpEff -db $cassandra_ips -ks test4_snpeff_db --cores $n_cores
geminicassandra load --skip-gene-tables --test-mode -p test4.snpeff.ped -v test4.vep.snpeff.vcf --skip-gerp-bp --skip-cadd -t snpEff -db $cassandra_ips -ks test4_snpeff_ped_db
