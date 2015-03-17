#!/bin/bash

sudo python setup.py install

gemini query -q "select variant_id from variants" \
		--gt-filter "gt_type.child_1==HET" \
		test/my.db
