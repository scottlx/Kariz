#!/usr/bin/python

import time

KARIZ = 0
MRD = 1
CP = 2


class Entry:
    """Cache Entry DataClass"""
    # this should support ranges
    def __init__(self, name, size=-1, score=0):
        self.parent_id = 0
        self.name = name
        self.size = size
        self.score = score
        self.pscore = score/size
        self.access_time = time.time()
        self.refcount = 0
        self.refjobs = {}
        self.sscore = 0 #share score
        self.pin = 0 # whether it is pin or not
        self.mrd_distance = -1
        self.policy = 0 # 0: KARIZ, 1: MRD, 2: CP
        self.lru_prev = None
        self.lru_next = None

    def __lt__(self, other):
        if self.policy == MRD:
            if self.mrd_distance < 0:
                return False; 
            if other.mrd_distance < 0:
                return True;
            return self.mrd_distance < self.mrd_distance
        return self.pscore < other.pscore

    def __eq__(self, other):
        if self.policy == MRD:
            return self.mrd_distance == self.mrd_distance
        return self.name == other.name

    def __ne__(self, other):
        if self.policy == MRD:
            return self.mrd_distance != self.mrd_distance
        return self.name != other.name

    def __str__(self):
        return self.name + ":" + str(self.size)
    
    def update_pscore(self):
        if self.size > 0:
            self.pscore = self.score/self.size
            
    def touch(self):
        self.access_time = time.time()
        
    def update_scores(self):
        return 0
