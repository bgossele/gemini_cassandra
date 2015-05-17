#!/bin/bash

export db_ips=
export cores=1
export keyspace=

geminicassandra query -q "select * from variants \
                      where sub_type = 'ts' \
                      and call_rate >= 0.95" -db $db_ips -ks $keyspace

#geminicassandra query -q "select chrom, start, end, pi from variants" -db $db_ips -ks $keyspace

#geminicassandra query -q "select chrom, start, end, ref, alt, gene, \
#                          gts.HG00239, \
#                          gts.NA19377, \
#                          gts.HG00146, \
#                          gts.NA12286 \
#                   from variants" -db $db_ips -ks $keyspace

#geminicassandra query --header -q "select chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
#                            FROM variants LIMIT 50000" -db $db_ips -ks $keyspace

#geminicassandra query --header -q "select chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
#                            FROM variants LIMIT 100000" -db $db_ips -ks $keyspace

#geminicassandra query --header -q "select chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
#                            FROM variants LIMIT 150000" -db $db_ips -ks $keyspace

#geminicassandra query --header -q "select chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
#                            FROM variants LIMIT 200000" -db $db_ips -ks $keyspace

#geminicassandra query --header -q "select chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
#                            FROM variants LIMIT 250000" -db $db_ips -ks $keyspace

#geminicassandra query --header -q "select chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
#                            FROM variants LIMIT 300000" -db $db_ips -ks $keyspace

#geminicassandra query -q "select chrom, start, end, ref, alt, gene from variants" \
               #--gt-filter "gt_types.HG00239 == HET || gt_types.NA19377 == HOM_REF" \
               #--header -db $db_ips -ks $keyspace

#geminicassandra query -q "select chrom, start, end, ref, alt, gene from variants" \
               #--gt-filter "gt_types.HG00239 == HET && gt_types.NA19377 == HOM_REF" \
               #--header -db $db_ips -ks $keyspace

#geminicassandra query -q "select chrom, start, end, ref, alt, gene from variants" \
               #--gt-filter "gt_types.HG00239 != HET" \
               #--header -db $db_ips -ks $keyspace

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace \
		#--cores 1

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace \
		#--cores 2

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace \
		#--cores 4

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace \
		#--cores 6

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace \
		#--cores 8

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace \
		#--cores 10

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace \
		#--cores 12

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace \
		#--cores 14

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all]" -db $db_ips -ks $keyspace \
		#--cores 16

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[==HOM_REF].[all] && \
                            #[gt_depths].[phenotype==2].[>=20].[all]" -db $db_ips -ks $keyspace \
		#--cores 1

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[==HOM_REF].[all] && \
                            #[gt_depths].[phenotype==2].[>=20].[all]" -db $db_ips -ks $keyspace \
		#--cores 2

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter  -db $db_ips -ks $keyspace \
		#--cores 4

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[==HOM_REF].[all] && \
                            #[gt_depths].[phenotype==2].[>=20].[all]" -db $db_ips -ks $keyspace \
		#--cores 6

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[==HOM_REF].[all] && \
                            #[gt_depths].[phenotype==2].[>=20].[all]" -db $db_ips -ks $keyspace \
		#--cores 8

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[==HOM_REF].[all] && \
                            #[gt_depths].[phenotype==2].[>=20].[all]" -db $db_ips -ks $keyspace \
		#--cores 10

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[==HOM_REF].[all] && \
                            #[gt_depths].[phenotype==2].[>=20].[all]" -db $db_ips -ks $keyspace \
		#--cores 12

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[==HOM_REF].[all] && \
                            #[gt_depths].[phenotype==2].[>=20].[all]" -db $db_ips -ks $keyspace \
		#--cores 14

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene, (gts).(phenotype==2) \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[==HOM_REF].[all] &&\
                            #[gt_depths].[phenotype==2].[>=20].[all]" -db $db_ips -ks $keyspace \
		#--cores 16

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all] && \
                            #[gt_depths].[phenotype==2].[<10].[count <= 2]" -db $db_ips -ks $keyspace \
		#--cores 1

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all] && \
                            #[gt_depths].[phenotype==2].[<10].[count <= 2]" -db $db_ips -ks $keyspace \
		#--cores 2

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all] && \
                            #[gt_depths].[phenotype==2].[<10].[count <= 2]" -db $db_ips -ks $keyspace \
		#--cores 4

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all] && \
                            #[gt_depths].[phenotype==2].[<10].[count <= 2]" -db $db_ips -ks $keyspace \
		#--cores 6

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all] && \
                            #[gt_depths].[phenotype==2].[<10].[count <= 2]" -db $db_ips -ks $keyspace \
		#--cores 8

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all] && \
                            #[gt_depths].[phenotype==2].[<10].[count <= 2]" -db $db_ips -ks $keyspace \
		#--cores 10

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all] && \
                            #[gt_depths].[phenotype==2].[<10].[count <= 2]" -db $db_ips -ks $keyspace \
		#--cores 12

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all] && \
                            #[gt_depths].[phenotype==2].[<10].[count <= 2]" -db $db_ips -ks $keyspace \
		#--cores 14

#geminicassandra query --header \
               #-q "SELECT chrom, start, end, ref, alt, gene \
                   #FROM variants" \
               #--gt-filter "[gt_types].[phenotype==2].[!=HOM_REF].[all] && \
                            #[gt_depths].[phenotype==2].[<10].[count <= 2]" -db $db_ips -ks $keyspace \
		#--cores 16


