gemini load --test-mode -v test.query.vcf --skip-gerp-bp --skip-cadd -t snpEff -ks test_query_db --cores 3 
gemini load --skip-gene-tables --test-mode -p test_extended_ped.ped -v test4.vep.snpeff.vcf  --skip-gerp-bp --skip-cadd -t snpEff -ks extended_ped_db --cores 3
gemini load --skip-gene-tables --test-mode -v test.snpeff.vcf --skip-gerp-bp --skip-cadd     -t snpEff -ks test_snpeff_vcf_db --cores 3

