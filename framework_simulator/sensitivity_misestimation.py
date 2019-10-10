#!/usr/bin/python
'''
Created on Sep 19, 2019

@author: mania
'''

import tpc
import plans.kariz.pig as pig
import utils.requester as req

tpch_q = tpc.graphs[0] # it is query number 22 for now
tpch_q.plans_container = pig.build_kariz_priorities(tpch_q)
print('hello')

