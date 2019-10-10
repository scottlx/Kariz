#!/usr/bin/python
# Trevor Nogues, Mania Abdi

# Graph abstraction 
from collections import defaultdict 
import random
import sched, threading, time
import copy
import random
import numpy as np
import pandas as pd
import queue
import utils.tests as tests
import plans.kariz.pig as pig
import utils.requester as requester
import ast
from utils.graph import Graph
import copy

_mrd = None

#Class to represent a graph 
class MRD: 
    def initialize_mrdtable(self, g):
        ''' compute the MRD table for every stage in the graph'''
        mrd_table = {}
        mrd_table_by_distance = {}
        pig.build_stages(g)
        for stg in g.stages:
            for j in g.stages[stg].jobs:
                for i in j.inputs:
                    if i not in mrd_table:
                        mrd_table[i] = [];
                    mrd_table[i].append(stg)
                    
                    if stg not in mrd_table_by_distance:
                        mrd_table_by_distance[stg] = []
                    mrd_table_by_distance[stg].append({i : j.inputs[i]})
        self.mrd_tables[g.dag_id] = mrd_table_by_distance
        
    def dag_from_string2(self, raw_execplan):
        raw_dag = ast.literal_eval(raw_execplan)
        nodes = raw_dag['nodes']
        n_nodes = len(nodes)
        g = Graph(n_nodes)
        g.dag_id = raw_dag['id']
        g.nodes = nodes
        for src in raw_dag['edges']:
            dests = raw_dag['edges'][src]
            for dst in dests:
                g.add_edge(src, dst, 0)
        g.inputs = raw_dag['inputs']
        g.inputSize = raw_dag['size']
        g.timeValue = raw_dag['original_runtime']
        g.cachedtimeValue = raw_dag['cached_runtime']
        self.initialize_mrdtable(g)
        self.graphs[g.dag_id] = g
    
    def gq_worker(self):
        while True:
            graph = self.gq.get()
            if graph:
                self.dag_from_string2(graph)
                self.gq.task_done()
                
    def get_cache_mrd_plan(self, dag_id):
        if 0 in self.mrd_tables[dag_id]:          
            return self.mrd_tables[dag_id][0]
        return None
    
    def get_prefetch_mrd_plan(self, dag_id):
        return self.mrd_tables[dag_id]

    def get_pinned_for_stage(self, stage_id):
        return self.constant_mrd_tables[dag_id][stage_id]

    def update_pinned_datasets(self, dag_id, stage_id, data):
        if dag_id not in self.pinned_files:
            self.pinned_files[dag_id] = {}
        self.pinned_files[dag_id][stage_id] = data

    def unpinned_completed_stage(self, dag_id, stage_id):
        if dag_id in self.pinned_files:
            return
        if stage_id -1 not in self.pinned_files:
            return 
        requester.uppined_datasets(self.pinned_files[dag_id][stage_id -1])

    def update_mrdtable(self, dag_id, stage_id):
        mrd_table_by_distance =  self.mrd_tables[dag_id]
        mrd_table_by_distance_tmp = {}
    
        distance=0         
        if distance in mrd_table_by_distance:
            del mrd_table_by_distance[distance]
            
        for k in mrd_table_by_distance:
            mrd_table_by_distance_tmp[k-1] = mrd_table_by_distance[k]
        
        self.mrd_tables[dag_id] = mrd_table_by_distance_tmp

    def online_planner(self, stage_metastr):
        stage_meta = ast.literal_eval(stage_metastr)
        if stage_meta['stage'] == -1:
            return
       
        # uppin files from previous stage 
        self.unpinned_completed_stage(stage_meta['id'], stage_meta['stage'])
        cached_files = requester.cache_mrd_plan(self.get_cache_mrd_plan(stage_meta['id']))
        self.update_pinned_datasets(stage_meta['id'], stage_meta['stage'], cached_files)
        self.update_mrdtable(stage_meta['id'], stage_meta['stage'])
        requester.prefetch_mrd_plan(self.get_prefetch_mrd_plan(stage_meta['id']))
        
    def pq_worker(self):
        while True:
            stage_meta = self.pq.get()
            if stage_meta:
                self.online_planner(stage_meta)
                self.pq.task_done()


    def __init__(self):
        global _mrd
        # a thread to process the incoming dags 
        self.gq = queue.Queue();
        self.gt = threading.Thread(target=self.gq_worker)
        self.gt.start()
        # a thread to process the incoming stage 
        self.pq = queue.Queue();
        self.pt = threading.Thread(target=self.pq_worker)
        self.pt.start()
        
        self.graphs = {}
        self.mrd_tables = {}
        self.pinned_files = {} 
        _mrd = self # mirab daemon instance

        
    def new_dag_from_string(self, dag_string):
        self.gq.put(dag_string)

    def notify_new_stage_from_string(self, stage_metastr):
        self.pq.put(stage_metastr)
