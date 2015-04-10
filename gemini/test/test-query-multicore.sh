####################################################################
# 1. Test the samples table
####################################################################
echo "    query_mc.t01...\c"
echo "1094PC0005	1	0	0	0	-9	-9
1094PC0009	2	0	0	0	-9	-9
1094PC0012	3	0	0	0	-9	-9
1094PC0013	4	0	0	0	-9	-9
1094PC0016	5	0	0	0	-9	-9
1094PC0017	6	0	0	0	-9	-9
1094PC0018	7	0	0	0	-9	-9
1094PC0019	8	0	0	0	-9	-9
1094PC0020	9	0	0	0	-9	-9
1094PC0021	10	0	0	0	-9	-9" > exp
gemini query -q "select * from samples" --test-mode -ks test_query_db --cores 2 | head > obs
check obs exp
rm obs exp

####################################################################
# 2. Test a basic query of the variants table
####################################################################
echo "    query_mc.t02...\c"
echo "chr1	30547	30548	T	G
chr1	30859	30860	G	C
chr1	30866	30869	CCT	C
chr1	30894	30895	T	C
chr1	30922	30923	G	T
chr1	69269	69270	A	G
chr1	69427	69428	T	G
chr1	69510	69511	A	G
chr1	69760	69761	A	T
chr1	69870	69871	G	A" > exp
gemini query -q "select chrom, start, end, ref, alt from variants" --test-mode -ks test_query_db --cores 2 | head \
       > obs
check obs exp
rm obs exp

####################################################################
# 3. Test a basic query of the variants table with a where clause
####################################################################
echo "    query_mc.t03...\c"
echo "chr1	1219381	1219382	C	G	SCNN1D
chr1	1219476	1219477	T	G	SCNN1D
chr1	1219486	1219487	T	G	SCNN1D
chr1	1219488	1219489	A	G	SCNN1D
chr1	1219494	1219496	GT	G	SCNN1D
chr1	1219502	1219505	GTT	G	SCNN1D
chr1	1219507	1219511	GTGA	G	SCNN1D
chr1	1219521	1219524	GTC	G	SCNN1D
chr1	1219533	1219536	GTT	G	SCNN1D
chr1	1219555	1219558	GTT	G	SCNN1D" > exp
gemini query -q "select chrom, start, end, ref, alt, gene \
                 from variants \
                 where gene == 'SCNN1D'" --test-mode -ks test_query_db --cores 2 | head \
	> obs
check obs exp
rm obs exp

####################################################################
# 4. Test a query of the variants table with a where clause
#    and a request for a sample's genotype
####################################################################
echo "    query_mc.t04...\c"
echo "chr1	1219381	1219382	C	G	SCNN1D	C/C
chr1	1219476	1219477	T	G	SCNN1D	T/T
chr1	1219486	1219487	T	G	SCNN1D	T/T
chr1	1219488	1219489	A	G	SCNN1D	A/A
chr1	1219494	1219496	GT	G	SCNN1D	GT/GT
chr1	1219502	1219505	GTT	G	SCNN1D	./.
chr1	1219507	1219511	GTGA	G	SCNN1D	./.
chr1	1219521	1219524	GTC	G	SCNN1D	./.
chr1	1219533	1219536	GTT	G	SCNN1D	./.
chr1	1219555	1219558	GTT	G	SCNN1D	./." > exp
gemini query -q "select chrom, start, end, ref, alt, gene, gts_1094PC0018 \
                 from variants \
                 where gene == 'SCNN1D'" --test-mode -ks test_query_db --cores 2 | head > obs
check obs exp
rm obs exp

####################################################################
# 5. Test a query of the variants table with a where clause
#    and a request for a sample's genotype and type
####################################################################
echo "    query_mc.t05...\c"
echo "chr1	1219381	1219382	C	G	SCNN1D	C/C	0
chr1	1219476	1219477	T	G	SCNN1D	T/T	0
chr1	1219486	1219487	T	G	SCNN1D	T/T	0
chr1	1219488	1219489	A	G	SCNN1D	A/A	0
chr1	1219494	1219496	GT	G	SCNN1D	GT/GT	0" > exp
gemini query -q "select chrom, start, end, ref, alt, gene, gts_1094PC0018, gt_types_1094PC0018 \
                 from variants \
                 where gene == 'SCNN1D'" --test-mode -ks test_query_db --cores 2 | head -5 > obs
check obs exp
rm obs exp

####################################################################
# 13. Test a query of the variants table with a where clause
#     and a genotype filter
####################################################################
echo "    query_mc.t13...\c"
echo "chr1	1219381	1219382	C	G	SCNN1D	C/C	0
chr1	1219476	1219477	T	G	SCNN1D	T/T	0
chr1	1219486	1219487	T	G	SCNN1D	T/T	0
chr1	1219488	1219489	A	G	SCNN1D	A/A	0
chr1	1219494	1219496	GT	G	SCNN1D	GT/GT	0" > exp
gemini query -q "select chrom, start, end, ref, alt, gene, gts_1094PC0018, gt_types_1094PC0018 \
                 from variants \
                 where gene == 'SCNN1D'" \
             --gt-filter "gt_type.1094PC0018 != HET" --test-mode -ks test_query_db --cores 2 | head -5 \
       > obs
check obs exp
rm obs exp

####################################################################
# 14. Test a query of the variants table with a where clause
#     and a more complex genotype filter
####################################################################
echo "    query_mc.t14...\c"
echo "chr1      1219381 1219382 C       G       SCNN1D  C/C     C/C
chr1    1219476 1219477 T       G       SCNN1D  T/T     T/T
chr1    1219486 1219487 T       G       SCNN1D  T/T     T/T
chr1    1219488 1219489 A       G       SCNN1D  A/A     A/A" > exp
gemini query -q "select chrom, start, end, ref, alt, gene, gts_1094PC0018, gts_1094PC0019 \
                 from variants \
                 where gene == 'SCNN1D'" \
             --gt-filter "gt_type.1094PC0018 == HET || gt_type.1094PC0019 == HOM_REF" --test-mode -ks test_query_db --cores 2 | head -5 \
       > obs
check obs exp
rm obs exp

####################################################################
# 15. Test a query of the variants table with a more complex genotype filter
####################################################################
echo "    query_mc.t15...\c"
echo "chr1	865218	865219	G	A	SAMD11	G/A	G/G
chr1	874948	874949	G	GAC	SAMD11	G/GAC	G/G
chr1	880750	880751	A	G	SAMD11	A/G	A/A
chr1	886408	886409	G	C	NOC2L	G/C	G/G
chr1	891897	891898	T	C	NOC2L	T/C	T/T
chr1	892470	892471	G	A	NOC2L	G/A	G/G
chr1	898602	898603	C	G	KLHL17	C/G	C/C
chr1	906301	906302	C	T	C1orf170	C/T	C/C
chr1	908822	908823	G	A	C1orf170	G/A	G/G
chr1	909308	909309	T	C	PLEKHN1	T/C	T/T
chr1	909418	909419	C	T	C1orf170	C/T	C/C
chr1	934796	934797	T	G	HES4	T/G	T/T
chr1	970559	970563	GGGT	G	AGRN	GGGT/G	GGGT/GGGT
chr1	970561	970563	GT	G	AGRN	GT/G	GT/GT
chr1	978761	978762	G	A	AGRN	G/A	G/G
chr1	978856	978857	T	G	AGRN	T/G	T/T
chr1	979593	979594	C	T	AGRN	C/T	C/C
chr1	982843	982844	G	C	AGRN	G/C	G/G
chr1	985444	985445	G	GT	AGRN	G/GT	G/G
chr1	985445	985446	G	T	AGRN	G/T	G/G
chr1	985446	985447	G	T	AGRN	G/T	G/G
chr1	985661	985662	C	T	AGRN	C/T	C/C
chr1	986884	986885	T	G	AGRN	T/G	T/T
chr1	987310	987311	T	C	AGRN	T/C	T/T
chr1	1119542	1119543	G	C	TTLL10	G/C	G/G
chr1	1158325	1158326	G	A	SDF4	G/A	G/G
chr1	1158356	1158357	A	G	SDF4	A/G	A/A
chr1	1158440	1158443	GCA	G	SDF4	GCA/G	GCA/GCA
chr1	1158533	1158534	G	GAC	SDF4	G/GAC	G/G
chr1	1158561	1158564	AAC	A	SDF4	AAC/A	AAC/AAC
chr1	1158566	1158567	A	G	SDF4	A/G	A/A
chr1	1158947	1158948	A	G	SDF4	A/G	A/A
chr1	1158972	1158973	A	T	SDF4	A/T	A/A
chr1	1158974	1158975	A	C	SDF4	A/C	A/A
chr1	1159484	1159485	C	T	SDF4	C/T	C/C
chr1	1163803	1163804	C	T	SDF4	C/T	C/C
chr1	1179415	1179416	A	C	FAM132A	A/C	A/A
chr1	1181371	1181372	C	T	FAM132A	C/T	C/C
chr1	1192771	1192773	CA	C	UBE2J2	CA/C	CA/CA" > exp
gemini query -q "select chrom, start, end, ref, alt, gene, gts_1094PC0018, gts_1094PC0019 \
                 from variants" \
             --gt-filter "gt_type.1094PC0018 == HET && gt_type.1094PC0019 == HOM_REF" --test-mode -ks test_query_db --cores 2 > obs
check obs exp
rm obs exp

#########################################################################
# 16. Test a basic query of the variants table with show-variant-samples
#########################################################################
echo "    query_mc.t16...\c"
echo "chrom	start	end	ref	alt	variant_samples	HET_samples	HOM_ALT_samples
chr1	30547	30548	T	G	1719PC0007,1478PC0016,1719PC0009		1719PC0007,1719PC0009,1478PC0016
chr1	30859	30860	G	C	1719PC0005,1478PC0017B	1719PC0005	1478PC0017B
chr1	30866	30869	CCT	C	1478PC0011,1719PC0005,1478PC0014B,1094PC0012,1094PC0021	1478PC0011,1719PC0005,1094PC0012,1094PC0021	1478PC0014B
chr1	30894	30895	T	C	1094PC0005,1094PC0009,1478PC0017B	1094PC0005,1094PC0009	1478PC0017B
chr1	30922	30923	G	T	1719PC0007,1478PC0014B,1478PC0015B,1478PC0016,1478PC0008B,1719PC0001,1478PC0006B,1478PC0020,1719PC0010,1478PC0025,1478PC0013B,1719PC0015,1719PC0016,1478PC0018,1719PC0009		1719PC0010,1478PC0014B,1478PC0015B,1478PC0008B,1478PC0016,1478PC0006B,1719PC0016,1478PC0025,1478PC0020,1719PC0007,1478PC0018,1478PC0013B,1719PC0015,1719PC0001,1719PC0009" > exp

gemini query --header --show-samples -q "select chrom, start, end, ref, alt \
                                        from variants" --test-mode -ks test_query_db --cores 2 | head -6 > obs
check obs exp
rm obs exp

####################################################################
# 18. Test tokenizing a SELECT list using spaces
####################################################################

echo "    query_mc.t18...\c"
echo "chr1	30547	30548	T	G
chr1	30859	30860	G	C
chr1	30866	30869	CCT	C
chr1	30894	30895	T	C
chr1	30922	30923	G	T
chr1	69269	69270	A	G
chr1	69427	69428	T	G
chr1	69510	69511	A	G
chr1	69760	69761	A	T
chr1	69870	69871	G	A" > exp
gemini query -q "select chrom, start, end, ref, alt from variants" --test-mode -ks test_query_db --cores 2 | head \
       > obs
check obs exp
rm obs exp

####################################################################
# 19. Test tokenizing a SELECT list using spaces
####################################################################
echo "    query_mc.t19...\c"
echo "chr1	30547	30548	T	G
chr1	30859	30860	G	C
chr1	30866	30869	CCT	C
chr1	30894	30895	T	C
chr1	30922	30923	G	T
chr1	69269	69270	A	G
chr1	69427	69428	T	G
chr1	69510	69511	A	G
chr1	69760	69761	A	T
chr1	69870	69871	G	A" > exp
gemini query -q "select chrom,start,end,ref,alt from variants" --test-mode -ks test_query_db --cores 2 | head \
       > obs
check obs exp

####################################################################
# 20. Test tokenizing a SELECT list spaces and no spaces
####################################################################
echo "    query_mc.t20...\c"
echo "chr1	30547	30548	T	G
chr1	30859	30860	G	C
chr1	30866	30869	CCT	C
chr1	30894	30895	T	C
chr1	30922	30923	G	T
chr1	69269	69270	A	G
chr1	69427	69428	T	G
chr1	69510	69511	A	G
chr1	69760	69761	A	T
chr1	69870	69871	G	A" > exp
gemini query -q "select chrom, start,end, ref,alt from variants" --test-mode -ks test_query_db --cores 2 | head \
       > obs
check obs exp
rm obs exp

########################################################################
# 21. Test tokenizing a SELECT list spaces and no spaces and GT columns
########################################################################
echo "    query_mc.t21...\c"
echo "chr1	30547	30548	T	G	T/T
chr1	30859	30860	G	C	G/G
chr1	30866	30869	CCT	C	CCT/CCT
chr1	30894	30895	T	C	T/T
chr1	30922	30923	G	T	./.
chr1	69269	69270	A	G	G/G
chr1	69427	69428	T	G	T/T
chr1	69510	69511	A	G	A/G
chr1	69760	69761	A	T	A/A
chr1	69870	69871	G	A	G/G" > exp
gemini query -q "select chrom, start,end, ref,alt,gts_1094PC0018 from variants" --test-mode -ks test_query_db --cores 2 | head \
       > obs
check obs exp
rm obs exp

####################################################################
# 31. Test that rows are filtered based on a --gt-filter if
#     a GT* column is SELECTed.
####################################################################
echo "    query_mc.t31...\c"
echo "chr1	69269	69270	A	G	OR4F5	3
chr1	69510	69511	A	G	OR4F5	3
chr1	69760	69761	A	T	OR4F5	3
chr1	861629	861630	G	A	SAMD11	3
chr1	861807	861808	A	G	SAMD11	3
chr1	866318	866319	G	A	SAMD11	3
chr1	866510	866511	C	CCCCT	SAMD11	3
chr1	866892	866893	T	C	SAMD11	3
chr1	866919	866920	A	G	SAMD11	3
chr1	870902	870903	T	C	SAMD11	3" > exp
gemini query -q "select chrom, start, end, ref, alt, gene, gt_types_1094PC0019 \
                 from variants" \
             --gt-filter "gt_type.1094PC0019 == HOM_ALT" --test-mode -ks test_query_db --cores 2 | head \
       > obs
check obs exp
rm obs exp

####################################################################
# 32. Test that rows are filtered based on a --gt-filter if
#     a GT* column is NOT SELECTed.
####################################################################
echo "    query_mc.t32...\c"
echo "chr1	69269	69270	A	G	OR4F5
chr1	69510	69511	A	G	OR4F5
chr1	69760	69761	A	T	OR4F5
chr1	861629	861630	G	A	SAMD11
chr1	861807	861808	A	G	SAMD11
chr1	866318	866319	G	A	SAMD11
chr1	866510	866511	C	CCCCT	SAMD11
chr1	866892	866893	T	C	SAMD11
chr1	866919	866920	A	G	SAMD11
chr1	870902	870903	T	C	SAMD11" > exp
gemini query -q "select chrom, start, end, ref, alt, gene \
                 from variants" \
             --gt-filter "gt_type.1094PC0019 == HOM_ALT" --test-mode -ks test_query_db --cores 2 | head \
       > obs
check obs exp
rm obs exp

####################################################################
# 33. Test that non-genotype columns that contain the substring "gt"
# execute properly
####################################################################
echo "    query_mc.t33...\c"
echo "chrom	start	end	ref	alt	aa_length	gene
chr1	30547	30548	T	G	85	FAM138A
chr1	30859	30860	G	C	85	FAM138A
chr1	30866	30869	CCT	C	85	FAM138A
chr1	30894	30895	T	C	85	FAM138A
chr1	30922	30923	G	T	85	FAM138A
chr1	69269	69270	A	G	305	OR4F5
chr1	69427	69428	T	G	305	OR4F5
chr1	69510	69511	A	G	305	OR4F5
chr1	69760	69761	A	T	305	OR4F5" > exp
gemini query --header -q "select chrom, start, end, ref, alt, aa_length, gene \
                 from variants" --test-mode -ks test_query_db --cores 2 | head > obs
check obs exp
rm obs exp


####################################################################
# 1. Test the samples table
####################################################################
echo "    query_mc3.t01...\c"
echo "1094PC0005	1	0	0	0	-9	-9
1094PC0009	2	0	0	0	-9	-9
1094PC0012	3	0	0	0	-9	-9
1094PC0013	4	0	0	0	-9	-9
1094PC0016	5	0	0	0	-9	-9
1094PC0017	6	0	0	0	-9	-9
1094PC0018	7	0	0	0	-9	-9
1094PC0019	8	0	0	0	-9	-9
1094PC0020	9	0	0	0	-9	-9
1094PC0021	10	0	0	0	-9	-9" > exp
gemini query -q "select * from samples" --test-mode -ks test_query_db --cores 3 | head > obs
check obs exp
rm obs exp

####################################################################
# 2. Test a basic query of the variants table
####################################################################
echo "    query_mc3.t02...\c"
echo "chr1	30547	30548	T	G
chr1	30859	30860	G	C
chr1	30866	30869	CCT	C
chr1	30894	30895	T	C
chr1	30922	30923	G	T
chr1	69269	69270	A	G
chr1	69427	69428	T	G
chr1	69510	69511	A	G
chr1	69760	69761	A	T
chr1	69870	69871	G	A" > exp
gemini query -q "select chrom, start, end, ref, alt from variants" --test-mode -ks test_query_db --cores 3 | head \
       > obs
check obs exp
rm obs exp

####################################################################
# 3. Test a basic query of the variants table with a where clause
####################################################################
echo "    query_mc3.t03...\c"
echo "chr1	1219381	1219382	C	G	SCNN1D
chr1	1219476	1219477	T	G	SCNN1D
chr1	1219486	1219487	T	G	SCNN1D
chr1	1219488	1219489	A	G	SCNN1D
chr1	1219494	1219496	GT	G	SCNN1D
chr1	1219502	1219505	GTT	G	SCNN1D
chr1	1219507	1219511	GTGA	G	SCNN1D
chr1	1219521	1219524	GTC	G	SCNN1D
chr1	1219533	1219536	GTT	G	SCNN1D
chr1	1219555	1219558	GTT	G	SCNN1D" > exp
gemini query -q "select chrom, start, end, ref, alt, gene \
                 from variants \
                 where gene == 'SCNN1D'" --test-mode -ks test_query_db --cores 3 | head \
	> obs
check obs exp
rm obs exp

####################################################################
# 4. Test a query of the variants table with a where clause
#    and a request for a sample's genotype
####################################################################
echo "    query_mc3.t04...\c"
echo "chr1	1219381	1219382	C	G	SCNN1D	C/C
chr1	1219476	1219477	T	G	SCNN1D	T/T
chr1	1219486	1219487	T	G	SCNN1D	T/T
chr1	1219488	1219489	A	G	SCNN1D	A/A
chr1	1219494	1219496	GT	G	SCNN1D	GT/GT
chr1	1219502	1219505	GTT	G	SCNN1D	./.
chr1	1219507	1219511	GTGA	G	SCNN1D	./.
chr1	1219521	1219524	GTC	G	SCNN1D	./.
chr1	1219533	1219536	GTT	G	SCNN1D	./.
chr1	1219555	1219558	GTT	G	SCNN1D	./." > exp
gemini query -q "select chrom, start, end, ref, alt, gene, gts_1094PC0018 \
                 from variants \
                 where gene == 'SCNN1D'" --test-mode -ks test_query_db --cores 3 | head > obs
check obs exp
rm obs exp

####################################################################
# 5. Test a query of the variants table with a where clause
#    and a request for a sample's genotype and type
####################################################################
echo "    query_mc3.t05...\c"
echo "chr1	1219381	1219382	C	G	SCNN1D	C/C	0
chr1	1219476	1219477	T	G	SCNN1D	T/T	0
chr1	1219486	1219487	T	G	SCNN1D	T/T	0
chr1	1219488	1219489	A	G	SCNN1D	A/A	0
chr1	1219494	1219496	GT	G	SCNN1D	GT/GT	0" > exp
gemini query -q "select chrom, start, end, ref, alt, gene, gts_1094PC0018, gt_types_1094PC0018 \
                 from variants \
                 where gene == 'SCNN1D'" --test-mode -ks test_query_db --cores 3 | head -5 > obs
check obs exp
rm obs exp

####################################################################
# 13. Test a query of the variants table with a where clause
#     and a genotype filter
####################################################################
echo "    query_mc3.t13...\c"
echo "chr1	1219381	1219382	C	G	SCNN1D	C/C	0
chr1	1219476	1219477	T	G	SCNN1D	T/T	0
chr1	1219486	1219487	T	G	SCNN1D	T/T	0
chr1	1219488	1219489	A	G	SCNN1D	A/A	0
chr1	1219494	1219496	GT	G	SCNN1D	GT/GT	0" > exp
gemini query -q "select chrom, start, end, ref, alt, gene, gts_1094PC0018, gt_types_1094PC0018 \
                 from variants \
                 where gene == 'SCNN1D'" \
             --gt-filter "gt_type.1094PC0018 != HET" --test-mode -ks test_query_db --cores 3 | head -5 \
       > obs
check obs exp
rm obs exp

####################################################################
# 14. Test a query of the variants table with a where clause
#     and a more complex genotype filter
####################################################################
echo "    query_mc3.t14...\c"
echo "chr1      1219381 1219382 C       G       SCNN1D  C/C     C/C
chr1    1219476 1219477 T       G       SCNN1D  T/T     T/T
chr1    1219486 1219487 T       G       SCNN1D  T/T     T/T
chr1    1219488 1219489 A       G       SCNN1D  A/A     A/A" > exp
gemini query -q "select chrom, start, end, ref, alt, gene, gts_1094PC0018, gts_1094PC0019 \
                 from variants \
                 where gene == 'SCNN1D'" \
             --gt-filter "gt_type.1094PC0018 == HET || gt_type.1094PC0019 == HOM_REF" --test-mode -ks test_query_db --cores 3 | head -5 \
       > obs
check obs exp
rm obs exp

####################################################################
# 15. Test a query of the variants table with a more complex genotype filter
####################################################################
echo "    query_mc3.t15...\c"
echo "chr1	865218	865219	G	A	SAMD11	G/A	G/G
chr1	874948	874949	G	GAC	SAMD11	G/GAC	G/G
chr1	880750	880751	A	G	SAMD11	A/G	A/A
chr1	886408	886409	G	C	NOC2L	G/C	G/G
chr1	891897	891898	T	C	NOC2L	T/C	T/T
chr1	892470	892471	G	A	NOC2L	G/A	G/G
chr1	898602	898603	C	G	KLHL17	C/G	C/C
chr1	906301	906302	C	T	C1orf170	C/T	C/C
chr1	908822	908823	G	A	C1orf170	G/A	G/G
chr1	909308	909309	T	C	PLEKHN1	T/C	T/T
chr1	909418	909419	C	T	C1orf170	C/T	C/C
chr1	934796	934797	T	G	HES4	T/G	T/T
chr1	970559	970563	GGGT	G	AGRN	GGGT/G	GGGT/GGGT
chr1	970561	970563	GT	G	AGRN	GT/G	GT/GT
chr1	978761	978762	G	A	AGRN	G/A	G/G
chr1	978856	978857	T	G	AGRN	T/G	T/T
chr1	979593	979594	C	T	AGRN	C/T	C/C
chr1	982843	982844	G	C	AGRN	G/C	G/G
chr1	985444	985445	G	GT	AGRN	G/GT	G/G
chr1	985445	985446	G	T	AGRN	G/T	G/G
chr1	985446	985447	G	T	AGRN	G/T	G/G
chr1	985661	985662	C	T	AGRN	C/T	C/C
chr1	986884	986885	T	G	AGRN	T/G	T/T
chr1	987310	987311	T	C	AGRN	T/C	T/T
chr1	1119542	1119543	G	C	TTLL10	G/C	G/G
chr1	1158325	1158326	G	A	SDF4	G/A	G/G
chr1	1158356	1158357	A	G	SDF4	A/G	A/A
chr1	1158440	1158443	GCA	G	SDF4	GCA/G	GCA/GCA
chr1	1158533	1158534	G	GAC	SDF4	G/GAC	G/G
chr1	1158561	1158564	AAC	A	SDF4	AAC/A	AAC/AAC
chr1	1158566	1158567	A	G	SDF4	A/G	A/A
chr1	1158947	1158948	A	G	SDF4	A/G	A/A
chr1	1158972	1158973	A	T	SDF4	A/T	A/A
chr1	1158974	1158975	A	C	SDF4	A/C	A/A
chr1	1159484	1159485	C	T	SDF4	C/T	C/C
chr1	1163803	1163804	C	T	SDF4	C/T	C/C
chr1	1179415	1179416	A	C	FAM132A	A/C	A/A
chr1	1181371	1181372	C	T	FAM132A	C/T	C/C
chr1	1192771	1192773	CA	C	UBE2J2	CA/C	CA/CA" > exp
gemini query -q "select chrom, start, end, ref, alt, gene, gts_1094PC0018, gts_1094PC0019 \
                 from variants" \
             --gt-filter "gt_type.1094PC0018 == HET && gt_type.1094PC0019 == HOM_REF" --test-mode -ks test_query_db --cores 3 > obs
check obs exp
rm obs exp

#########################################################################
# 16. Test a basic query of the variants table with show-variant-samples
#########################################################################
echo "    query_mc3.t16...\c"
echo "chrom	start	end	ref	alt	variant_samples	HET_samples	HOM_ALT_samples
chr1	30547	30548	T	G	1719PC0007,1478PC0016,1719PC0009		1719PC0007,1719PC0009,1478PC0016
chr1	30859	30860	G	C	1719PC0005,1478PC0017B	1719PC0005	1478PC0017B
chr1	30866	30869	CCT	C	1478PC0011,1719PC0005,1478PC0014B,1094PC0012,1094PC0021	1478PC0011,1719PC0005,1094PC0012,1094PC0021	1478PC0014B
chr1	30894	30895	T	C	1094PC0005,1094PC0009,1478PC0017B	1094PC0005,1094PC0009	1478PC0017B
chr1	30922	30923	G	T	1719PC0007,1478PC0014B,1478PC0015B,1478PC0016,1478PC0008B,1719PC0001,1478PC0006B,1478PC0020,1719PC0010,1478PC0025,1478PC0013B,1719PC0015,1719PC0016,1478PC0018,1719PC0009		1719PC0010,1478PC0014B,1478PC0015B,1478PC0008B,1478PC0016,1478PC0006B,1719PC0016,1478PC0025,1478PC0020,1719PC0007,1478PC0018,1478PC0013B,1719PC0015,1719PC0001,1719PC0009" > exp

gemini query --header --show-samples -q "select chrom, start, end, ref, alt \
                                        from variants" --test-mode -ks test_query_db --cores 3 | head -6 > obs
check obs exp
rm obs exp

####################################################################
# 18. Test tokenizing a SELECT list using spaces
####################################################################

echo "    query_mc3.t18...\c"
echo "chr1	30547	30548	T	G
chr1	30859	30860	G	C
chr1	30866	30869	CCT	C
chr1	30894	30895	T	C
chr1	30922	30923	G	T
chr1	69269	69270	A	G
chr1	69427	69428	T	G
chr1	69510	69511	A	G
chr1	69760	69761	A	T
chr1	69870	69871	G	A" > exp
gemini query -q "select chrom, start, end, ref, alt from variants" --test-mode -ks test_query_db --cores 3 | head \
       > obs
check obs exp
rm obs exp

####################################################################
# 19. Test tokenizing a SELECT list using spaces
####################################################################
echo "    query_mc3.t19...\c"
echo "chr1	30547	30548	T	G
chr1	30859	30860	G	C
chr1	30866	30869	CCT	C
chr1	30894	30895	T	C
chr1	30922	30923	G	T
chr1	69269	69270	A	G
chr1	69427	69428	T	G
chr1	69510	69511	A	G
chr1	69760	69761	A	T
chr1	69870	69871	G	A" > exp
gemini query -q "select chrom,start,end,ref,alt from variants" --test-mode -ks test_query_db --cores 3 | head \
       > obs
check obs exp

####################################################################
# 20. Test tokenizing a SELECT list spaces and no spaces
####################################################################
echo "    query_mc3.t20...\c"
echo "chr1	30547	30548	T	G
chr1	30859	30860	G	C
chr1	30866	30869	CCT	C
chr1	30894	30895	T	C
chr1	30922	30923	G	T
chr1	69269	69270	A	G
chr1	69427	69428	T	G
chr1	69510	69511	A	G
chr1	69760	69761	A	T
chr1	69870	69871	G	A" > exp
gemini query -q "select chrom, start,end, ref,alt from variants" --test-mode -ks test_query_db --cores 3 | head \
       > obs
check obs exp
rm obs exp

########################################################################
# 21. Test tokenizing a SELECT list spaces and no spaces and GT columns
########################################################################
echo "    query_mc3.t21...\c"
echo "chr1	30547	30548	T	G	T/T
chr1	30859	30860	G	C	G/G
chr1	30866	30869	CCT	C	CCT/CCT
chr1	30894	30895	T	C	T/T
chr1	30922	30923	G	T	./.
chr1	69269	69270	A	G	G/G
chr1	69427	69428	T	G	T/T
chr1	69510	69511	A	G	A/G
chr1	69760	69761	A	T	A/A
chr1	69870	69871	G	A	G/G" > exp
gemini query -q "select chrom, start,end, ref,alt,gts_1094PC0018 from variants" --test-mode -ks test_query_db --cores 3 | head \
       > obs
check obs exp
rm obs exp

####################################################################
# 31. Test that rows are filtered based on a --gt-filter if
#     a GT* column is SELECTed.
####################################################################
echo "    query_mc3.t31...\c"
echo "chr1	69269	69270	A	G	OR4F5	3
chr1	69510	69511	A	G	OR4F5	3
chr1	69760	69761	A	T	OR4F5	3
chr1	861629	861630	G	A	SAMD11	3
chr1	861807	861808	A	G	SAMD11	3
chr1	866318	866319	G	A	SAMD11	3
chr1	866510	866511	C	CCCCT	SAMD11	3
chr1	866892	866893	T	C	SAMD11	3
chr1	866919	866920	A	G	SAMD11	3
chr1	870902	870903	T	C	SAMD11	3" > exp
gemini query -q "select chrom, start, end, ref, alt, gene, gt_types_1094PC0019 \
                 from variants" \
             --gt-filter "gt_type.1094PC0019 == HOM_ALT" --test-mode -ks test_query_db --cores 3 | head \
       > obs
check obs exp
rm obs exp

####################################################################
# 32. Test that rows are filtered based on a --gt-filter if
#     a GT* column is NOT SELECTed.
####################################################################
echo "    query_mc3.t32...\c"
echo "chr1	69269	69270	A	G	OR4F5
chr1	69510	69511	A	G	OR4F5
chr1	69760	69761	A	T	OR4F5
chr1	861629	861630	G	A	SAMD11
chr1	861807	861808	A	G	SAMD11
chr1	866318	866319	G	A	SAMD11
chr1	866510	866511	C	CCCCT	SAMD11
chr1	866892	866893	T	C	SAMD11
chr1	866919	866920	A	G	SAMD11
chr1	870902	870903	T	C	SAMD11" > exp
gemini query -q "select chrom, start, end, ref, alt, gene \
                 from variants" \
             --gt-filter "gt_type.1094PC0019 == HOM_ALT" --test-mode -ks test_query_db --cores 3 | head \
       > obs
check obs exp
rm obs exp

####################################################################
# 33. Test that non-genotype columns that contain the substring "gt"
# execute properly
####################################################################
echo "    query_mc3.t33...\c"
echo "chrom	start	end	ref	alt	aa_length	gene
chr1	30547	30548	T	G	85	FAM138A
chr1	30859	30860	G	C	85	FAM138A
chr1	30866	30869	CCT	C	85	FAM138A
chr1	30894	30895	T	C	85	FAM138A
chr1	30922	30923	G	T	85	FAM138A
chr1	69269	69270	A	G	305	OR4F5
chr1	69427	69428	T	G	305	OR4F5
chr1	69510	69511	A	G	305	OR4F5
chr1	69760	69761	A	T	305	OR4F5" > exp
gemini query --header -q "select chrom, start, end, ref, alt, aa_length, gene \
                 from variants" --test-mode -ks test_query_db --cores 3 | head > obs
check obs exp
rm obs exp


