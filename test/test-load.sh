
###########################################################################################
#1. Test loading an unannotated file without genotypes
###########################################################################################
gemini load -v ALL.wgs.phase1_release_v3.20101123.snps_indels_sv.sites.snippet.vcf \
	        --skip-gene-tables --skip-gerp-bp --skip-cadd --no-genotypes -ks thousand_g_snippet_db

echo "    load.t1...\c"
echo "chr1	10582	G	A	None
chr1	10610	C	G	None
chr1	13301	C	T	None
chr1	13326	G	C	None
chr1	13956	TC	T	None
chr1	13979	T	C	None
chr1	30922	G	T	None
chr1	46401	C	CTGT	None
chr1	47189	G	GA	None
chr1	51475	T	C	None" > exp

gemini query -q "select chrom, start, ref, alt, gene from variants" \
	-ks thousand_g_snippet_db --test-mode | head > obs
check obs exp
rm obs exp


###########################################################################################
#2. Test loading an annotated file without genotypes
###########################################################################################
gemini load -v ALL.wgs.phase1_release_v3.20101123.snps_indels_sv.sites.snippet.snpEff.vcf \
    --skip-gene-tables --skip-gerp-bp --skip-cadd --no-genotypes \
    -t snpEff -ks thousand_g_snippet_snpeff_db

echo "    load.t2...\c"
echo "chr1	10582	G	A	WASH7P
chr1	10610	C	G	WASH7P
chr1	13301	C	T	WASH7P
chr1	13326	G	C	WASH7P
chr1	13956	TC	T	DDX11L1
chr1	13979	T	C	DDX11L1
chr1	30922	G	T	FAM138A
chr1	46401	C	CTGT	None
chr1	47189	G	GA	None
chr1	51475	T	C	None" > exp

gemini query -q "select chrom, start, ref, alt, gene from variants" \
	-ks thousand_g_snippet_snpeff_db --test-mode | head > obs
check obs exp
rm obs exp

###########################################################################################
#3. Test loading an extended ped file
###########################################################################################
gemini load -p test_extended_ped.ped -v test4.vep.snpeff.vcf \
--skip-gene-tables --skip-gerp-bp --skip-cadd -t snpEff -ks extended_ped_test_db

echo "    load.t3...\c"
echo "name	sample_id	ethnicity	family_id	hair_color	maternal_id	paternal_id	phenotype	sex
M10475	1	None	1	brown	0	0	1	1
M10478	2	None	1	brown	M10500	M10475	2	2
M10500	3	None	1	purple	0	0	2	2
M128215	4	None	1	blue	M10500	M10475	1	1" > exp
gemini query --header -q "select * from samples" -ks extended_ped_test_db --test-mode > obs
check obs exp
rm obs exp

###########################################################################################
#4. Test --passonly on loading
###########################################################################################
gemini load  --skip-gene-tables --passonly -v test.passonly.vcf --skip-gerp-bp --skip-cadd -t snpEff \
-ks passonly_db

echo "    load.t4...\c"
echo "chr1	1334051	CTAGAG	C" > exp
gemini query -q "select chrom, start, ref, alt from variants" -ks passonly_db --test-mode > obs
check obs exp
rm obs exp
