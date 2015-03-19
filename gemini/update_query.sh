#!/bin/bash

sudo python setup.py install

gemini query -q "select variant_id from variants" \
		--gt-filter "[gt_depth].[sex = '-9'].[>100].[all] && gt_type.child_1 = HET && gt_type.mom_2 != HET" \
		test/my.db
