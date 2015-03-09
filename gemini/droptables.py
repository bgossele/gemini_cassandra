#!/usr/bin/env python

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

answer = raw_input('Drop all gemini tables (y | n)? ')

if answer in ['yes', 'yes please', 'ja', 'ja, graag']:
	print 'Dropping gemini tables...'
	cluster = Cluster()
	session = cluster.connect('gemini_keyspace')
	tables = ["variants", "samples", "version", "resources", "sample_genotypes", "variant_impacts"]
	for table in tables:
		session.execute(SimpleStatement('DROP TABLE if exists %s' % table ))


