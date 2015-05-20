#!/bin/bash

export db_ips=ly-1-00,ly-1-07,ly-2-10,ly-1-12,ly-1-14
export keyspace=s_db

c=1
n=5
while [ $c -le $n ] 
do
	#geminicassandra query -q "select chrom,start,end,pi from variants \
        #       	where sub_type = 'ts' \
        #	and call_rate >= 0.95" -db $db_ips -ks $keyspace --cores 24 --exp_id exp1_24c \
	#	-batch_size 50
	#rm -r exp1_24C_results

	geminicassandra query -q "select chrom, start, end, ref, alt, gene from variants" \
	               --gt-filter "gt_types.HG00239 == HET || gt_types.NA19377 == HOM_REF" \
	               -db $db_ips -ks $keyspace --exp_id exp2 --cores 24
	
	rm -r exp2_results

	geminicassandra query -q "select chrom, start, end, ref, alt, gene from variants" \
	               --gt-filter "gt_types.HG00239 == HET && gt_types.NA19377 == HOM_REF" \
	               -db $db_ips -ks $keyspace --exp_id exp3 --cores 24
	
	rm -r exp3_results

#	geminicassandra query -q "select chrom, start, end, ref, alt, gene from variants" \
#	               --gt-filter "gt_types.HG00239 != HET" \
#	                -db $db_ips -ks $keyspace --exp_id exp4 --cores 24
	
#	rm -r exp4_results

#	geminicassandra query \
#	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
#	               --gt-filter "[gt_types].[phenotype=='2'].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace --exp_id exp5_4 \
#			--cores 4

#	rm -r exp5_4_results

#	geminicassandra query \
#	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
#	               --gt-filter "[gt_types].[phenotype=='2'].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace --exp_id exp5_6 \
#					--cores 6

#	rm -r exp5_6_results
	
#	geminicassandra query \
#	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
#	               --gt-filter "[gt_types].[phenotype=='2'].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace --exp_id exp5_8 \
#					--cores 8

#	rm -r exp5_8_results
	
#	geminicassandra query \
#	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
#	               --gt-filter "[gt_types].[phenotype=='2'].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace --exp_id exp5_10 \
#			--cores 10

#	rm -r exp5_10_results
	
#	geminicassandra query \
#	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
#	               --gt-filter "[gt_types].[phenotype=='2'].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace --exp_id exp5_12 \
#			--cores 12

#	rm -r exp5_12_results
	
#	geminicassandra query \
#	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
#	               --gt-filter "[gt_types].[phenotype=='2'].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace --exp_id exp5_16 \
#			--cores 16

#	rm -r exp5_16_results
			
#	geminicassandra query \
#	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
#	               --gt-filter "[gt_types].[phenotype=='2'].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace --exp_id exp5_24 \
#			--cores 24

#	rm -r exp5_24_results
		
	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_types].[phenotype=='2'].[==HOM_REF].[all] && \
	                            [gt_depths].[phenotype=='2'].[>=20].[all]" -db $db_ips -ks $keyspace --exp_id exp6_4 \
			--cores 4
	
	rm -r exp6_4_results

	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_types].[phenotype=='2'].[==HOM_REF].[all] && \
	                            [gt_depths].[phenotype=='2'].[>=20].[all]" -db $db_ips -ks $keyspace --exp_id exp6_6 \
			--cores 6
	
	rm -r exp6_6_results
	
	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_types].[phenotype=='2'].[==HOM_REF].[all] && \
	                            [gt_depths].[phenotype=='2'].[>=20].[all]" -db $db_ips -ks $keyspace --exp_id exp6_8 \
			--cores 8
	
	rm -r exp6_8_results
	
	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_types].[phenotype=='2'].[==HOM_REF].[all] && \
	                            [gt_depths].[phenotype=='2'].[>=20].[all]" -db $db_ips -ks $keyspace --exp_id exp6_10 \
			--cores 10
	
	rm -r exp6_10_results
	
	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_types].[phenotype=='2'].[==HOM_REF].[all] && \
	                            [gt_depths].[phenotype=='2'].[>=20].[all]" -db $db_ips -ks $keyspace --exp_id exp6_12 \
			--cores 12
	
	rm -r exp6_12_results
	
	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_types].[phenotype=='2'].[==HOM_REF].[all] &&\
	                            [gt_depths].[phenotype=='2'].[>=20].[all]" -db $db_ips -ks $keyspace --exp_id exp6_16 \
			--cores 16
	
	rm -r exp6_16_results
			
	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_types].[phenotype=='2'].[==HOM_REF].[all] &&\
	                            [gt_depths].[phenotype=='2'].[>=20].[all]" -db $db_ips -ks $keyspace --exp_id exp6_24 \
			--cores 24
	
	rm -r exp6_24_results
	
	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_depths].[phenotype=='2'].[<10].[count <= 20] && \
	               				[gt_types].[phenotype=='2'].[!=HOM_REF].[all]" \
	               				 -db $db_ips -ks $keyspace --exp_id exp7_4 \
			--cores 4
	
	rm -r exp7_4_results

	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_depths].[phenotype=='2'].[<10].[count <= 20] && \
	               				[gt_types].[phenotype=='2'].[!=HOM_REF].[all]" \
	               				 -db $db_ips -ks $keyspace --exp_id exp7_6 \
			--cores 6
	
	rm -r exp7_6_results
	
	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_depths].[phenotype=='2'].[<10].[count <= 20] && \
	               				[gt_types].[phenotype=='2'].[!=HOM_REF].[all]" \ 
	               				-db $db_ips -ks $keyspace --exp_id exp7_8 \
			--cores 8
	
	rm -r exp7_8_results
	
	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_depths].[phenotype=='2'].[<10].[count <= 20] && \
	               				[gt_types].[phenotype=='2'].[!=HOM_REF].[all]" \
	               				 -db $db_ips -ks $keyspace --exp_id exp7_10 \
			--cores 10
	
	rm -r exp7_10_results
	
	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_types].[phenotype=='2'].[!=HOM_REF].[all] && \
	                            [gt_depths].[phenotype=='2'].[<10].[count <= 20]" -db $db_ips -ks $keyspace --exp_id exp7_12 \
			--cores 12
	
	rm -r exp7_12_results
	
	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_depths].[phenotype=='2'].[<10].[count <= 20] && \
	               				[gt_types].[phenotype=='2'].[!=HOM_REF].[all]" \
	               				 -db $db_ips -ks $keyspace --exp_id exp7_16 \
			--cores 16
	
	rm -r exp7_16_results
	
	geminicassandra query \
	               -q "SELECT chrom, start, end, ref, alt, gene FROM variants" \
	               --gt-filter "[gt_depths].[phenotype=='2'].[<10].[count <= 20] && \
	               				[gt_types].[phenotype=='2'].[!=HOM_REF].[all]" \
	               				 -db $db_ips -ks $keyspace --exp_id exp7_24 \
			--cores 24
	
	rm -r exp7_24_results
	
	((c++))
	
done
