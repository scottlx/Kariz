#!/usr/bin/python

# Author: Mania Abdi
import utils.plan as plan
import json

class Planner:
    def __init__(self):
        self.dags = {}
        
    def build_pig_mrd_plans(self, g):
        blevels, orderednodes = g.bLevel()
        cur_blevel = max(blevels)
        csi = max(blevels) - cur_blevel # current stage index
        cur_stage = plan.Stage(csi)
        plans_container = plan.PlansContainer(g)
        
        # build priority plans for stage 
        ci = 0 # current index
        while ci < len(blevels):    
            if blevels[ci] != cur_blevel:
                cur_stage.finish_add_jobs()
                plans_container.add_stage(cur_stage)
                plan = cur_stage.get_criticalpath_plan()
                plans_container.add_cache_plan(plan, cur_stage)
                # prepare new stage
                cur_blevel = cur_blevel - 1
                csi = max(blevels) - cur_blevel 
                cur_stage = plan.Stage(csi)
            
            # build the job
            j = plan.Job(csi)
            j.id = orderednodes[ci]
            j.original_runtime = g.timeValue[orderednodes[ci]]
            j.improved_runtime = g.timeValue[orderednodes[ci]]
            j.cached_runtime = g.cachedtimeValue[orderednodes[ci]]
            # FIXME add inputs as well
            inputs = g.inputs[orderednodes[ci]]
            inputSize = g.inputSize[orderednodes[ci]]
            for i in range(0, len(inputs)):
                j.inputs[inputs[i]] = inputSize[i]
            
            
            cur_stage.add_job(j)
            ci = ci + 1
    
        cur_stage.finish_add_jobs()
        plans_container.add_stage(cur_stage)
        plan = cur_stage.get_criticalpath_plan()
        plans_container.add_cache_plan(plan, cur_stage)
    
        # so far, the plans are build for each stage, its time to append to the prefetch stage
        plans_container.assing_prefetch_plan()
        return plans_container;

    def add_dag(self, g):
        g.plans_container = self.build_pig_mrd_plans(g)
        self.dags[g.dag_id] = g

    def get_stage_plans(self, dag_id, stage):
        return self.dags[dag_id].plans_container.get_stage_plans(stage)
