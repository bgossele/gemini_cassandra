
#############################################
# 1. Test ExAC allele frequencies
#############################################
echo "    exac.t01...\c"
echo "chrom	start	end	ref	alt	in_exac	aaf_exac_all	aaf_adj_exac_all	aaf_adj_exac_afr	aaf_adj_exac_amr	aaf_adj_exac_eas	aaf_adj_exac_fin	aaf_adj_exac_nfe	aaf_adj_exac_oth	aaf_adj_exac_sas
chr1	985954	985955	G	C	False	None	None	None	None	None	None	None	None	None
chr1	1199488	1199489	G	A	False	None	None	None	None	None	None	None	None	None
chr1	1959698	1959699	G	A	True	0.0170000009239	0.0175534356385	0.00481877243146	0.00973012670875	0.0	0.0410552062094	0.023726593703	0.0232843142003	0.00655049597844
chr14	105420589	105420590	C	T	True	0.00174600002356	0.00175203033723	0.000301083899103	0.000775862077717	0.0	0.000296384125249	0.00248675211333	0.00437636766583	0.00168431189377
chr9	112185055	112185056	C	G	True	1.6260000848e-05	1.62665110111e-05	0.000189000187675	0.0	0.0	0.0	0.0	0.0	0.0
chr12	121432116	121432118	GC	G	True	0.00022839999292	0.000234270410147	0.000539374304935	0.000371839356376	0.000825354887638	0.0	9.89472027868e-05	0.0	0.00030057108961
chr1	161276552	161276553	G	T	False	None	None	None	None	None	None	None	None	None
chr1	247587092	247587093	C	T	False	None	None	None	None	None	None	None	None	None" > exp

gemini query --header -q "select chrom, start, end, ref, alt, in_exac, aaf_exac_all, aaf_adj_exac_all, \
	                        aaf_adj_exac_afr, aaf_adj_exac_amr, aaf_adj_exac_eas, aaf_adj_exac_fin, \
		                        aaf_adj_exac_nfe, aaf_adj_exac_oth, aaf_adj_exac_sas from variants" --test-mode -ks test_exac_db > obs

check obs exp
rm obs exp

#############################################
