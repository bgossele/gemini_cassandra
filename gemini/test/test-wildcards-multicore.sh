########################################################################
# 1. Test select * wildcard
########################################################################
echo "    wildcard_mc.t1...\c"
echo "G/G	G/A	G/G	G/G
C/C	C/C	C/C	C/C
T/T	T/T	T/C	T/C
T/T	C/C	C/T	C/T
./.	./.	C/C	C/C
./.	A/A	./.	A/A
T/T	T/T	C/C	C/C
C/T	C/C	C/C	C/C
./.	C/C	C/C	./." > exp
gemini query -q "select (gts).(*)from variants" -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp


########################################################################
# 2. Test select * wildcard with other columns and a header
########################################################################
echo "    wildcard_mc.t2...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	1142207	1142208	T	C	C/C	C/C	C/C	C/C
chr10	48003991	48003992	C	T	T/T	C/C	C/T	C/T
chr10	52004314	52004315	T	C	./.	C/C	C/C	./.
chr10	52497528	52497529	G	C	./.	./.	C/C	C/C
chr16	72057434	72057435	C	T	C/T	C/C	C/C	C/C
chr10	126678091	126678092	G	A	G/G	G/A	G/G	G/G
chr10	135210790	135210791	T	C	T/T	T/T	C/C	C/C
chr10	135336655	135336656	G	A	./.	A/A	./.	A/A
chr10	135369531	135369532	T	C	T/T	T/T	T/C	T/C" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*)from variants" --test-mode -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp

########################################################################
# 3. Test specific wildcard with other columns and a header
########################################################################
echo "    wildcard_mc.t3...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215
chr10	1142207	1142208	T	C	C/C	C/C
chr10	48003991	48003992	C	T	T/T	C/C
chr10	52004314	52004315	T	C	./.	C/C
chr10	52497528	52497529	G	C	./.	./.
chr16	72057434	72057435	C	T	C/T	C/C
chr10	126678091	126678092	G	A	G/G	G/A
chr10	135210790	135210791	T	C	T/T	T/T
chr10	135336655	135336656	G	A	./.	A/A
chr10	135369531	135369532	T	C	T/T	T/T" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(phenotype=='1') from variants" --test-mode -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp

########################################################################
# 4. Test specific wildcard with genotype filter
########################################################################
echo "    wildcard_mc.t4...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215
chr10	135210790	135210791	T	C	T/T	T/T
chr10	135369531	135369532	T	C	T/T	T/T" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(phenotype=='1') from variants" \
             --gt-filter "[gt_type].[phenotype=='1'].[==HOM_REF].[all]" --test-mode -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp


########################################################################
# 5. Test multiple select wildcards with genotype filter
########################################################################
echo "    wildcard_mc.t5...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gt_types_m10475	gt_types_m128215
chr10	135210790	135210791	T	C	T/T	T/T	0	0
chr10	135369531	135369532	T	C	T/T	T/T	0	0" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(phenotype=='1'), (gt_types).(phenotype=='1') from variants" \
             --gt-filter "[gt_type].[phenotype=='1'].[==HOM_REF].[all]" --test-mode -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp


########################################################################
# 6. Test a wildcard filter with a vanilla filter
########################################################################
echo "    wildcard_mc.t6...\c"
echo "chrom	start	end	ref	alt	gts_m10478	gts_m10500
chr10	135336655	135336656	G	A	A/A	./." > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(phenotype=='2') from variants" \
             --gt-filter "[gt_type].[phenotype=='2'].[!=HOM_REF].[all] && gt.M10478 =='A/A'" -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp

########################################################################
# 7. Test specific wildcard with genotype filter using ANY
########################################################################
echo "    wildcard_mc.t7...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215
chr10	48003991	48003992	C	T	T/T	C/C
chr16	72057434	72057435	C	T	C/T	C/C
chr10	126678091	126678092	G	A	G/G	G/A
chr10	135210790	135210791	T	C	T/T	T/T
chr10	135369531	135369532	T	C	T/T	T/T" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(phenotype=='1') from variants" \
             --gt-filter "[gt_type].[phenotype=='1'].[==HOM_REF].[any]" --test-mode -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp

########################################################################
# 8. Test specific wildcard with genotype filter using NONE
########################################################################
echo "    wildcard_mc.t8...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215
chr10	1142207	1142208	T	C	C/C	C/C
chr10	52004314	52004315	T	C	./.	C/C
chr10	52497528	52497529	G	C	./.	./.
chr10	135336655	135336656	G	A	./.	A/A" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(phenotype=='1') from variants" \
             --gt-filter "[gt_type].[phenotype=='1'].[==HOM_REF].[none]" --test-mode -ks  extended_ped_db --cores 2 > obs
check obs exp
rm obs exp

########################################################################
# 9. Test specific wildcard with genotype filter using ANY and *
########################################################################
echo "    wildcard_mc.t9...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	48003991	48003992	C	T	T/T	C/C	C/T	C/T
chr16	72057434	72057435	C	T	C/T	C/C	C/C	C/C
chr10	126678091	126678092	G	A	G/G	G/A	G/G	G/G
chr10	135369531	135369532	T	C	T/T	T/T	T/C	T/C" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HET].[any]" --test-mode -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp

########################################################################
# 10. Test specific wildcard with genotype filter using COUNT > 0 (should be same as any)
########################################################################
echo "    wildcard_mc.t10...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	48003991	48003992	C	T	T/T	C/C	C/T	C/T
chr16	72057434	72057435	C	T	C/T	C/C	C/C	C/C
chr10	126678091	126678092	G	A	G/G	G/A	G/G	G/G
chr10	135369531	135369532	T	C	T/T	T/T	T/C	T/C" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HET].[count>0]" --test-mode -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp

########################################################################
# 11. Test specific wildcard with genotype filter using COUNT >= 2 
########################################################################
echo "    wildcard_mc.t11...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	48003991	48003992	C	T	T/T	C/C	C/T	C/T
chr10	135369531	135369532	T	C	T/T	T/T	T/C	T/C" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HET].[count>=2]" --test-mode -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp

########################################################################
# 12. Test specific wildcard with genotype filter using COUNT == 0  
########################################################################
echo "    wildcard_mc.t12...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	1142207	1142208	T	C	C/C	C/C	C/C	C/C
chr10	52004314	52004315	T	C	./.	C/C	C/C	./.
chr10	52497528	52497529	G	C	./.	./.	C/C	C/C
chr10	135210790	135210791	T	C	T/T	T/T	C/C	C/C
chr10	135336655	135336656	G	A	./.	A/A	./.	A/A" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HET].[count==0]" --test-mode -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp


########################################################################
# 13. Test specific wildcard with genotype filter using COUNT != 2  
########################################################################
echo "    wildcard_mc.t13...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	1142207	1142208	T	C	C/C	C/C	C/C	C/C
chr10	48003991	48003992	C	T	T/T	C/C	C/T	C/T
chr16	72057434	72057435	C	T	C/T	C/C	C/C	C/C
chr10	126678091	126678092	G	A	G/G	G/A	G/G	G/G
chr10	135369531	135369532	T	C	T/T	T/T	T/C	T/C" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HOM_ALT].[count!=2]" --test-mode -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp


########################################################################
# 14. Test specific wildcard with genotype filter using COUNT != 2 and specific filter  
########################################################################
echo "    wildcard_mc.t14...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	126678091	126678092	G	A	G/G	G/A	G/G	G/G" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HOM_ALT].[count!=2] && gt.M10478 =='G/G'" --test-mode -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp

########################################################################
# 15. Test specific wildcard with genotype filter using different spacing
########################################################################
echo "    wildcard_mc.t15...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	126678091	126678092	G	A	G/G	G/A	G/G	G/G" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==    HOM_ALT].[  count   !=2] && gt.M10478 =='G/G'" -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp

########################################################################
# 16. Test syntax failure  
########################################################################
echo "    wildcard_mc.t16...\c"
echo "Unsupported wildcard operation: (). Exiting." > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HOM_ALT].[]" -ks extended_ped_db --cores 2 2> obs
check obs exp
rm obs exp

########################################################################
# 17. Test syntax failure  
########################################################################
echo "    wildcard_mc.t17...\c"
echo "Unsupported wildcard operation: (amy). Exiting." > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HOM_ALT].[amy]" -ks extended_ped_db --cores 2 2> obs
check obs exp
rm exp

########################################################################
# 18. Test syntax failure  
########################################################################
echo "    wildcard_mc.t18...\c"
echo "Wildcard filter should consist of 4 elements. Exiting." > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HOM_ALT]" -ks extended_ped_db --cores 2 2> obs
check obs exp
rm obs exp

########################################################################
# 19. Test multiple wildcard on same column
########################################################################
echo "    wildcard_mc.t19...\c"
echo "chrom	start	end	ref	alt	gene	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	1142207	1142208	T	C	WDR37	C/C	C/C	C/C	C/C
chr10	48003991	48003992	C	T	ASAH2C	T/T	C/C	C/T	C/T
chr10	52004314	52004315	T	C	ASAH2	./.	C/C	C/C	./.
chr10	52497528	52497529	G	C	ASAH2B	./.	./.	C/C	C/C
chr10	135336655	135336656	G	A	SPRN	./.	A/A	./.	A/A" > exp
gemini query --header -q "select chrom, start, end, ref, alt, gene, (gts).(*) from variants" \
             --gt-filter "([gt_type].[phenotype=='1'].[!=HOM_REF].[count>=1] && [gt_type].[phenotype=='2'].[!=HOM_REF].[count>=1])" --test-mode -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp

########################################################################
# 20. Test multiple wildcard on same column
########################################################################
echo "    wildcard_mc.t20...\c"
echo "No results!" > exp
gemini query --header -q "select chrom, start, end, ref, alt, gene, (gts).(*) from variants" \
             --gt-filter "([gt_type].[phenotype=='1'].[!=HOM_REF].[all] && [gt_type].[phenotype=='2'].[==HOM_REF].[all])" -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp

########################################################################
# 21. Test multiple wildcard on same column
########################################################################
echo "    wildcard_mc.t21...\c"
echo "chrom	start	end	ref	alt	gene	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr16	72057434	72057435	C	T	DHODH	C/T	C/C	C/C	C/C
chr10	126678091	126678092	G	A	CTBP2	G/G	G/A	G/G	G/G" > exp
gemini query --header -q "select chrom, start, end, ref, alt, gene, (gts).(*) from variants" \
             --gt-filter "([gt_type].[phenotype=='1'].[!=HOM_REF].[count>=1] && [gt_type].[phenotype=='2'].[==HOM_REF].[all])" --test-mode -ks extended_ped_db --cores 2 > obs
check obs exp
rm obs exp


########################################################################
# 1. Test select * wildcard
########################################################################
echo "    wildcard_mc3.t1...\c"
echo "G/G	G/A	G/G	G/G
C/C	C/C	C/C	C/C
T/T	T/T	T/C	T/C
T/T	C/C	C/T	C/T
./.	./.	C/C	C/C
./.	A/A	./.	A/A
T/T	T/T	C/C	C/C
C/T	C/C	C/C	C/C
./.	C/C	C/C	./." > exp
gemini query -q "select (gts).(*)from variants" -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp


########################################################################
# 2. Test select * wildcard with other columns and a header
########################################################################
echo "    wildcard_mc3.t2...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	1142207	1142208	T	C	C/C	C/C	C/C	C/C
chr10	48003991	48003992	C	T	T/T	C/C	C/T	C/T
chr10	52004314	52004315	T	C	./.	C/C	C/C	./.
chr10	52497528	52497529	G	C	./.	./.	C/C	C/C
chr16	72057434	72057435	C	T	C/T	C/C	C/C	C/C
chr10	126678091	126678092	G	A	G/G	G/A	G/G	G/G
chr10	135210790	135210791	T	C	T/T	T/T	C/C	C/C
chr10	135336655	135336656	G	A	./.	A/A	./.	A/A
chr10	135369531	135369532	T	C	T/T	T/T	T/C	T/C" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*)from variants" --test-mode -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp

########################################################################
# 3. Test specific wildcard with other columns and a header
########################################################################
echo "    wildcard_mc3.t3...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215
chr10	1142207	1142208	T	C	C/C	C/C
chr10	48003991	48003992	C	T	T/T	C/C
chr10	52004314	52004315	T	C	./.	C/C
chr10	52497528	52497529	G	C	./.	./.
chr16	72057434	72057435	C	T	C/T	C/C
chr10	126678091	126678092	G	A	G/G	G/A
chr10	135210790	135210791	T	C	T/T	T/T
chr10	135336655	135336656	G	A	./.	A/A
chr10	135369531	135369532	T	C	T/T	T/T" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(phenotype=='1') from variants" --test-mode -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp

########################################################################
# 4. Test specific wildcard with genotype filter
########################################################################
echo "    wildcard_mc3.t4...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215
chr10	135210790	135210791	T	C	T/T	T/T
chr10	135369531	135369532	T	C	T/T	T/T" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(phenotype=='1') from variants" \
             --gt-filter "[gt_type].[phenotype=='1'].[==HOM_REF].[all]" --test-mode -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp


########################################################################
# 5. Test multiple select wildcards with genotype filter
########################################################################
echo "    wildcard_mc3.t5...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gt_types_m10475	gt_types_m128215
chr10	135210790	135210791	T	C	T/T	T/T	0	0
chr10	135369531	135369532	T	C	T/T	T/T	0	0" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(phenotype=='1'), (gt_types).(phenotype=='1') from variants" \
             --gt-filter "[gt_type].[phenotype=='1'].[==HOM_REF].[all]" --test-mode -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp


########################################################################
# 6. Test a wildcard filter with a vanilla filter
########################################################################
echo "    wildcard_mc3.t6...\c"
echo "chrom	start	end	ref	alt	gts_m10478	gts_m10500
chr10	135336655	135336656	G	A	A/A	./." > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(phenotype=='2') from variants" \
             --gt-filter "[gt_type].[phenotype=='2'].[!=HOM_REF].[all] && gt.M10478 =='A/A'" -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp

########################################################################
# 7. Test specific wildcard with genotype filter using ANY
########################################################################
echo "    wildcard_mc3.t7...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215
chr10	48003991	48003992	C	T	T/T	C/C
chr16	72057434	72057435	C	T	C/T	C/C
chr10	126678091	126678092	G	A	G/G	G/A
chr10	135210790	135210791	T	C	T/T	T/T
chr10	135369531	135369532	T	C	T/T	T/T" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(phenotype=='1') from variants" \
             --gt-filter "[gt_type].[phenotype=='1'].[==HOM_REF].[any]" --test-mode -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp

########################################################################
# 8. Test specific wildcard with genotype filter using NONE
########################################################################
echo "    wildcard_mc3.t8...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215
chr10	1142207	1142208	T	C	C/C	C/C
chr10	52004314	52004315	T	C	./.	C/C
chr10	52497528	52497529	G	C	./.	./.
chr10	135336655	135336656	G	A	./.	A/A" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(phenotype=='1') from variants" \
             --gt-filter "[gt_type].[phenotype=='1'].[==HOM_REF].[none]" --test-mode -ks  extended_ped_db --cores 3 > obs
check obs exp
rm obs exp

########################################################################
# 9. Test specific wildcard with genotype filter using ANY and *
########################################################################
echo "    wildcard_mc3.t9...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	48003991	48003992	C	T	T/T	C/C	C/T	C/T
chr16	72057434	72057435	C	T	C/T	C/C	C/C	C/C
chr10	126678091	126678092	G	A	G/G	G/A	G/G	G/G
chr10	135369531	135369532	T	C	T/T	T/T	T/C	T/C" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HET].[any]" --test-mode -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp

########################################################################
# 10. Test specific wildcard with genotype filter using COUNT > 0 (should be same as any)
########################################################################
echo "    wildcard_mc3.t10...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	48003991	48003992	C	T	T/T	C/C	C/T	C/T
chr16	72057434	72057435	C	T	C/T	C/C	C/C	C/C
chr10	126678091	126678092	G	A	G/G	G/A	G/G	G/G
chr10	135369531	135369532	T	C	T/T	T/T	T/C	T/C" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HET].[count>0]" --test-mode -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp

########################################################################
# 11. Test specific wildcard with genotype filter using COUNT >= 2 
########################################################################
echo "    wildcard_mc3.t11...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	48003991	48003992	C	T	T/T	C/C	C/T	C/T
chr10	135369531	135369532	T	C	T/T	T/T	T/C	T/C" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HET].[count>=2]" --test-mode -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp

########################################################################
# 12. Test specific wildcard with genotype filter using COUNT == 0  
########################################################################
echo "    wildcard_mc3.t12...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	1142207	1142208	T	C	C/C	C/C	C/C	C/C
chr10	52004314	52004315	T	C	./.	C/C	C/C	./.
chr10	52497528	52497529	G	C	./.	./.	C/C	C/C
chr10	135210790	135210791	T	C	T/T	T/T	C/C	C/C
chr10	135336655	135336656	G	A	./.	A/A	./.	A/A" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HET].[count==0]" --test-mode -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp


########################################################################
# 13. Test specific wildcard with genotype filter using COUNT != 2  
########################################################################
echo "    wildcard_mc3.t13...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	1142207	1142208	T	C	C/C	C/C	C/C	C/C
chr10	48003991	48003992	C	T	T/T	C/C	C/T	C/T
chr16	72057434	72057435	C	T	C/T	C/C	C/C	C/C
chr10	126678091	126678092	G	A	G/G	G/A	G/G	G/G
chr10	135369531	135369532	T	C	T/T	T/T	T/C	T/C" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HOM_ALT].[count!=2]" --test-mode -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp


########################################################################
# 14. Test specific wildcard with genotype filter using COUNT != 2 and specific filter  
########################################################################
echo "    wildcard_mc3.t14...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	126678091	126678092	G	A	G/G	G/A	G/G	G/G" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HOM_ALT].[count!=2] && gt.M10478 =='G/G'" --test-mode -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp

########################################################################
# 15. Test specific wildcard with genotype filter using different spacing
########################################################################
echo "    wildcard_mc3.t15...\c"
echo "chrom	start	end	ref	alt	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	126678091	126678092	G	A	G/G	G/A	G/G	G/G" > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==    HOM_ALT].[  count   !=2] && gt.M10478 =='G/G'" -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp

########################################################################
# 16. Test syntax failure  
########################################################################
echo "    wildcard_mc3.t16...\c"
echo "Unsupported wildcard operation: (). Exiting." > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HOM_ALT].[]" -ks extended_ped_db --cores 3 2> obs
check obs exp
rm obs exp

########################################################################
# 17. Test syntax failure  
########################################################################
echo "    wildcard_mc3.t17...\c"
echo "Unsupported wildcard operation: (amy). Exiting." > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HOM_ALT].[amy]" -ks extended_ped_db --cores 3 2> obs
check obs exp
rm obs exp

########################################################################
# 18. Test syntax failure  
########################################################################
echo "    wildcard_mc3.t18...\c"
echo "Wildcard filter should consist of 4 elements. Exiting." > exp
gemini query --header -q "select chrom, start, end, ref, alt, (gts).(*) from variants" \
             --gt-filter "[gt_type].[*].[==HOM_ALT]" -ks extended_ped_db --cores 3 2> obs
check obs exp
rm obs exp

########################################################################
# 19. Test multiple wildcard on same column
########################################################################
echo "    wildcard_mc3.t19...\c"
echo "chrom	start	end	ref	alt	gene	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr10	1142207	1142208	T	C	WDR37	C/C	C/C	C/C	C/C
chr10	48003991	48003992	C	T	ASAH2C	T/T	C/C	C/T	C/T
chr10	52004314	52004315	T	C	ASAH2	./.	C/C	C/C	./.
chr10	52497528	52497529	G	C	ASAH2B	./.	./.	C/C	C/C
chr10	135336655	135336656	G	A	SPRN	./.	A/A	./.	A/A" > exp
gemini query --header -q "select chrom, start, end, ref, alt, gene, (gts).(*) from variants" \
             --gt-filter "([gt_type].[phenotype=='1'].[!=HOM_REF].[count>=1] && [gt_type].[phenotype=='2'].[!=HOM_REF].[count>=1])" --test-mode -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp

########################################################################
# 20. Test multiple wildcard on same column
########################################################################
echo "    wildcard_mc3.t20...\c"
echo "No results!" > exp
gemini query --header -q "select chrom, start, end, ref, alt, gene, (gts).(*) from variants" \
             --gt-filter "([gt_type].[phenotype=='1'].[!=HOM_REF].[all] && [gt_type].[phenotype=='2'].[==HOM_REF].[all])" -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp

########################################################################
# 21. Test multiple wildcard on same column
########################################################################
echo "    wildcard_mc3.t21...\c"
echo "chrom	start	end	ref	alt	gene	gts_m10475	gts_m128215	gts_m10500	gts_m10478
chr16	72057434	72057435	C	T	DHODH	C/T	C/C	C/C	C/C
chr10	126678091	126678092	G	A	CTBP2	G/G	G/A	G/G	G/G" > exp
gemini query --header -q "select chrom, start, end, ref, alt, gene, (gts).(*) from variants" \
             --gt-filter "([gt_type].[phenotype=='1'].[!=HOM_REF].[count>=1] && [gt_type].[phenotype=='2'].[==HOM_REF].[all])" --test-mode -ks extended_ped_db --cores 3 > obs
check obs exp
rm obs exp


