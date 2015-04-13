gemini query -q "select chrom, start, end, ref, alt, (gts).(phenotype=='1'), (gt_types).(phenotype=='1') from variants" \
             --gt-filter "[gt_type].[phenotype=='1'].[==HOM_REF].[all]" --test-mode -ks extended_ped_db --cores 1

gemini query -q "select chrom, start, end, ref, alt, (gts).(phenotype=='2') from variants" \
             --gt-filter "[gt_type].[phenotype=='2'].[!=HOM_REF].[all] && gt.M10478 =='A/A'" -ks extended_ped_db --cores 1

gemini query -q "select chrom, start, end, ref, alt, (gts).(phenotype=='1') from variants" \
             --gt-filter "[gt_type].[phenotype=='1'].[==HOM_REF].[any]" --test-mode -ks extended_ped_db --cores 1

gemini query -q "select chrom, start, end, ref, alt, (gts).(phenotype=='1') from variants" \
             --gt-filter "[gt_type].[phenotype=='1'].[==HOM_REF].[none]" --test-mode -ks  extended_ped_db --cores 1

gemini query -q "select chrom, start, end, ref, alt, (gts).(phenotype=='1'), (gt_types).(phenotype=='1') from variants" \
             --gt-filter "[gt_type].[phenotype=='1'].[==HOM_REF].[all]" --test-mode -ks extended_ped_db --cores 3

gemini query -q "select chrom, start, end, ref, alt, (gts).(phenotype=='2') from variants" \
             --gt-filter "[gt_type].[phenotype=='2'].[!=HOM_REF].[all] && gt.M10478 =='A/A'" -ks extended_ped_db --cores 3

gemini query -q "select chrom, start, end, ref, alt, (gts).(phenotype=='1') from variants" \
             --gt-filter "[gt_type].[phenotype=='1'].[==HOM_REF].[any]" --test-mode -ks extended_ped_db --cores 3

gemini query -q "select chrom, start, end, ref, alt, (gts).(phenotype=='1') from variants" \
             --gt-filter "[gt_type].[phenotype=='1'].[==HOM_REF].[none]" --test-mode -ks  extended_ped_db --cores 3

gemini query -q "select chrom, start, end, ref, alt from variants" \
	     --gt-filter "[gt_type].[*].[==HOM_REF].[count >20]" -ks test_query_db --cores 1

gemini query -q "select chrom, start, end, ref, alt from variants" \
	     --gt-filter "[gt_type].[*].[==HOM_REF].[count >20]" -ks test_query_db --cores 2 
