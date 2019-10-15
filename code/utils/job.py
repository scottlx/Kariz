from collections import defaultdict 
import random
import sched, threading, time
import utils.randoms
import uuid
#import utils.plan as plan
import ast

class Job:
    def __init__(self, id = -1):
        self.id = id
        self.inputs = {}
        self.est_runtime_cache = 0
        self.est_runtime_remote = 0
        self.runtime_cache = 0
        self.runtime_remote = 0
        self.runtime_partial_cached = 0
        self.final_runtime = 0
        self.num_task = 0
        self.bfs_level = 0
        self.blevel = 0
        self.tlevel = 0
        self.slevel = 0
        self.in_degree = 0
        self.out_degree = 0
        self.parents = {} # parent : distance
        self.children = {}
        
    def reset(self):
        self.est_runtime_cache = self.runtime_cache
        self.est_runtime_remote = self.runtime_remote
        self.runtime_partial_cached = self.runtime_remote
        self.final_runtime = self.runtime_remote
        self.bfs_level = 0
        self.blevel = 0
        self.tlevel = 0
        self.slevel = 0

        
    def __str__(self):
        jobstr =  '{"id" : ' + str(self.id)
        jobstr +=  ', "runtime_cache":' + str(self.runtime_cache)
        jobstr +=  ', "runtime_remote":' + str(self.runtime_remote)
        jobstr +=  ', "est_runtime_cache":' + str(self.est_runtime_cache)
        jobstr +=  ', "est_runtime_remote":' + str(self.est_runtime_remote)
        jobstr +=  ', "num_task":' + str(self.num_task)
        jobstr +=  ', "children":' + str(self.children) + ', "inputs":' + str(self.inputs) + '}'
        return jobstr 
    
    def to_str(self):
        jobstr =  '{"id" : ' + str(self.id)
        jobstr +=  ', "runtime_cache":' + str(self.runtime_cache)
        jobstr +=  ', "runtime_remote":' + str(self.runtime_remote)
        jobstr +=  ', "est_runtime_cache":' + str(self.est_runtime_cache)
        jobstr +=  ', "est_runtime_remote":' + str(self.est_runtime_remote)
        jobstr +=  ', "num_task":' + str(self.est_runtime_remote)
        jobstr +=  ', "children":' + str(self.children) + ', "inputs":' + str(self.inputs) + '}'
        return jobstr 
    

    def longer_than(self, other):
        return self.runtime_partial_cached > other.runtime_partial_cached

    def concurrent_with(self, other):
        return self.runtime_partial_cached == other.runtime_partial_cached

    def longer_than_wcache(self, other):
        return self.runtime_cache > other.runtime_cache
    
    def random_runtime(self, _min = 1, _max = 10):
        self.est_runtime_remote = self.runtime_remote = random.randint(_min, _max)
        self.est_runtime_cache = self.runtime_cache = random.randint(_min, self.est_runtime_remote)
        self.runtime_partial_cached = self.est_runtime_remote
        self.final_runtime = self.est_runtime_remote
        
    def static_runtime(self, runtime_remote, runtime_cache):
        self.est_runtime_remote = self.runtime_remote = runtime_remote
        self.est_runtime_cache = self.runtime_cache = runtime_cache
        self.runtime_partial_cached = self.est_runtime_remote
        self.final_runtime = self.est_runtime_remote
    
    def estimated_runtimes(self, runtime_remote, runtime_cache):
        self.runtime_partial_cached = self.est_runtime_remote = runtime_remote
        self.est_runtime_cache = runtime_cache
    
    def config_ntasks(self, n_tasks):
        self.num_task = n_tasks
        
    def add_child(self, child, distance = 0):
        if child not in self.children:
            self.out_degree += 1
        self.children[child] = distance

    def add_parent(self, p, distance = 0):
        if p not in self.parents:
            self.in_degree += 1
        self.parents[p] = distance
        
    def config_inputs(self, inputs):
        self.inputs = inputs
        