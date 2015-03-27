#!/bin/bash

sudo python setup.py install

#gemini query -q "select variant_id, chrom, sub_type from variants" \
#		--gt-filter "[gt_depth].[sex='1'].[>100].[all] && gt_type.child_1 = HET" \
#		--header --show-samples test/my.db

gemini query -q "select variant_id, (gt_types).(sex='1') from variants limit 1" \
		--header test/my.db
