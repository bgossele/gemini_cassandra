#!/usr/bin/env python

from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()
session.execute('DROP KEYSPACE IF EXISTS gemini_keyspace')


