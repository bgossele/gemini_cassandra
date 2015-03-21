#!/usr/bin/env python

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

answer = raw_input('Drop all gemini tables (y | n)? ')

if answer.lower().startswith(('ja', 'yes', 'oui', 'si')):
	print 'Dropping gemini tables...'
	cluster = Cluster()
	session = cluster.connect('gemini_keyspace')
	tables = ["variants", "samples", "version", "resources", "sample_genotypes", "variant_impacts",
		  "variants_by_samples_gt_type", "variants_by_samples_gt_depth", "variants_by_sub_type_call_rate", 
		  "variants_by_chrom_depth", "samples_by_phenotype", "samples_by_sex"]
	for table in tables:
		session.execute('DROP TABLE if exists %s' % table )


