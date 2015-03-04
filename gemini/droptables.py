#!/usr/bin/env python

from cassandra.cluster import Cluster

answer = raw_input('Drop all gemini tables (y | n)? ')

if answer in ['yes', 'yes please', 'ja', 'ja, graag']:
	print 'Dropping gemini tables...'
	cluster = Cluster()
	session = cluster.connect()
	session.execute('DROP KEYSPACE IF EXISTS gemini_keyspace')


