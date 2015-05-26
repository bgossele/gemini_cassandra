#!/bin/bash

export db_ips=ly-1-00,ly-1-07,ly-1-11,ly-1-12,ly-1-14,ly-2-07,ly-2-10,ly-2-11,ly-2-12
export keyspace=s_db

c=1
n=2
while [ $c -le $n ] 
do
	geminicassandra query -q "select chrom,start,end,pi from variants \
               	where sub_type = 'ts' \
        	and call_rate >= 0.95" -db $db_ips -ks $keyspace --cores 24 --exp_id exp1 \
		-batch_size 50
	rm -r exp1_results
	
	geminicassandra query -q "select chrom,start from variants \
			where chrom = 'chr22' and start > 16000000 \
			and start <= 18000000" \
        	 -db $db_ips -ks $keyspace --cores 24 --exp_id exp8 \
	
	rm -r exp8_results


	geminicassandra query -q "select chrom, start, end, ref, alt, gene from variants" \
	               --gt-filter "gt_types.HG00239 == HET || gt_types.NA19377 == HOM_REF" \
	               -db $db_ips -ks $keyspace --exp_id exp2 --cores 24
	
	rm -r exp2_results

	geminicassandra query -q "select chrom, start, end, ref, alt, gene from variants" \
	               --gt-filter "gt_types.HG00239 == HET && gt_types.NA19377 == HOM_REF" \
	               -db $db_ips -ks $keyspace --exp_id exp3 --cores 24
	
	rm -r exp3_results

	geminicassandra query -q "select chrom, start, end, ref, alt, gene from variants" \
	               --gt-filter "gt_types.HG00239 != HET" \
	                -db $db_ips -ks $keyspace --exp_id exp4 --cores 24
	
	rm -r exp4_results

		
	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_types].[phenotype=='2'].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace --exp_id exp5 \
			--cores 24

	rm -r exp5_results
		
	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_types].[phenotype=='2'].[==HOM_REF].[all] &&\
	                            [gt_depths].[phenotype=='2'].[>=20].[all]" -db $db_ips -ks $keyspace --exp_id exp6 \
			--cores 24
	
	rm -r exp6_results
		
	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_depths].[phenotype=='2'].[<10].[count <= 20] && \
	               				[gt_types].[phenotype=='2'].[!=HOM_REF].[all]" \
	               				 -db $db_ips -ks $keyspace --exp_id exp7 \
			--cores 24
	
	rm -r exp7_results
	
	((c++))
	
done
