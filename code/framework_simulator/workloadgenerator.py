#!/usr/bin/python
'''
Created on Sep 7, 2019

@author: mania
'''
import pigsimulator
import colorama
from colorama import Fore, Style
import json

'''
from alibaba traces we figure it out that average # of DAGs
submitted per 30 is L. The maxumum is Y and the minimum is Z

we generate DAGs for 1 hour every 30 seconds using poisson distribution 
with average time interval of L

For alibaba traces should I randomly 
lets independently apply size and identity 

generate set of N filenames 
Usibg exponential propablity to assign sizes for filenames use zipf distribution 
I use zipf to select from a list of file names

for ali baba traces just randomly assign inputs to nodes. 


'''
import scipy.stats as sp
import numpy as np
import sched, threading, time
import numpy as np
import tpc
import utils.graph as graph
import utils.hadoop as hadoop
import ast
import pandas as pd
#import estimator.predictor as pred
import random 
import pigsimulator as pigsim
import math
import logging
import threading
import time
import colorama
from colorama import Fore, Style

class Workload: 
    def __init__(self):
        self.avg_ndags_p30s = 5
        self.interval = 5 # The resolution is seconds
        self.overall_sim_time = 30 # 1hours, the resolution is in seconds
        self.n_intervals = self.overall_sim_time//self.interval
        self.n_dags_p30s = sp.poisson.rvs(mu=self.avg_ndags_p30s, size=self.n_intervals)
        self.priority = 1
        self.elapsed_time = 0
        self.fname = '../plans/dags_pool.csv'
        self.n_datasets = 50
        self.dags = tpc.graphs_dict
        self.dags_byid = {}
        stats_fname = '../plans/job_runtime_stats.csv'
        #self.datasets = self.generate_datasets_pool()
        self.statistics = {}
        self.statistic_df = None
        #self.load_dag_pool(self.fname)
        #self.load_statistics(stats_fname)
    
    def generate_datasets_pool(self):
        datasets = ['file_'+str(i) for i in range(0, self.n_datasets)]
        #datasets_sizes = sp.expon.rvs(scale=1, loc=0, size=self.n_datasets)
        #datasets_frequency = np.random.zipf(5, size=120)
        return datasets
    
    def load_dag_pool(self, fname):
        with open(fname) as fd: 
            dags_str = fd.read()
            dagstr_ls = dags_str.split('}{')
            dagstr_ls[0] = dagstr_ls[0] + '}'
            dagstr_ls[-1] = '{' + dagstr_ls[-1]
            for i in range(1, len(dagstr_ls) - 1):
                dagstr_ls[i] = '{' + dagstr_ls[i] + '}'
            for gstr in dagstr_ls:
                raw_dag = ast.literal_eval(gstr)
                g = graph.jsonstr_to_graph(raw_dag['dagdata'])
                g.dag_id = raw_dag['DAGid']
                self.dags.append(g)
                self.dags_byid[g.dag_id] = g
    
    def load_statistics(self,fname):
        self.statistic_df = pd.read_csv(fname)
         
    def load_statistics2(self, fname):
        with open(fname) as fd:
            data1 = fd.read().split('\n')
            del data1[-1]
            df_header = data1[0].split(',')
            data = data1[1:]
            df_data = []
            for x in data:
                jobInfo = x.split(',')
                df_data.append(jobInfo)
                dag_id = jobInfo[14]
                jobId = jobInfo[0]
                if dag_id not in self.statistics:
                    self.statistics[dag_id] = {}
                type = jobInfo[12]
                if not type:
                    type = 'UDF'
                self.statistics[dag_id][jobId] = {'job_id': jobInfo[0],
                                               'n_maps':  int(jobInfo[1]),
                                               'Reduces': int(jobInfo[2]),
                                               'MaxMapTime':  math.ceil(int(jobInfo[3])),
                                               'MinMapTime': math.ceil(int(jobInfo[4])),
                                               'AvgMapTime':  math.ceil(int(jobInfo[5])),
                                               'MedianMapTime': math.ceil(int(jobInfo[6])),
                                               'MaxReduceTime':  math.ceil(int(jobInfo[7])),
                                               'MinReduceTime': math.ceil(int(jobInfo[8])),
                                               'AvgReduceTime':  math.ceil(int(jobInfo[9])),
                                               'MedianReducetime': math.ceil(int(jobInfo[10])),
                                               'Alias':  jobInfo[11],
                                               'Type': type,
                                               'Outputs':  jobInfo[13],
                                               'DagId': jobInfo[14],
                                               'runtime': math.ceil(int(jobInfo[15])//1000),
                                               'queuetime':  math.ceil(int(jobInfo[16])//1000),
                                               'maptime': math.ceil(int(jobInfo[17])//1000),
                                               'name':  jobInfo[18]}
                
        self.statistic_df = pd.DataFrame(data=df_data,columns=df_header)

        
    def select_dags(self):
        index = self.elapsed_time//self.interval
        n_dags_inpool = len(self.dags)
        n_submitted_dags = self.n_dags_p30s[index]
        submitted_dags_indexes = np.random.randint(n_dags_inpool, size=n_submitted_dags)
        print('timestamp: ', index, 
              ', \# of submitted DAGs:', n_submitted_dags, 
              'submitted DAGs:', submitted_dags_indexes)
        submitted_dags = [self.dags[i] for i in submitted_dags_indexes]
        return submitted_dags
        
    
    def schedule_timer(self):
        s = sched.scheduler(time.time, time.sleep)
        self.select_dags()
        self.elapsed_time+=self.interval
        if self.elapsed_time < self.overall_sim_time:
            s.enter(self.interval, self.priority, self.schedule_timer, argument=())
            s.run()
        
    def start_workload(self):
        global start_t
        s = sched.scheduler(time.time, time.sleep)
        start = time.time()
        start_t = start
        self.schedule_timer() 
        end = time.time()
        print("Total simulation time", int(end - start))
        
    def start_single_dag_coldcache_poolworkload(self):
        for dag in self.dags:
            if dag.dag_id in self.statistics: 
                g_stats = self.statistics[dag.dag_id]
                i = 0;
                for j in g_stats:
                    dag.static_runtime(i, int(g_stats[j]['runtime']),
                                       random.randint(int(g_stats[j]['runtime'])//10, 
                                                      int(g_stats[j]['runtime'])//3))
                    i = i + 1
                runtime = pigsim.start_pig_simulator(dag)
    
    def start_single_dag_coldcache_misestimation(self):
        stats = {"Kariz": {}, "RCP": {}, "PC": {}}
        try:
            with open('misestimation_result.json', 'r') as dumpf:
                stats = json.load(dumpf)
        except FileNotFoundError:
                print(Fore.YELLOW, "No stats available", Style.RESET_ALL)
        
        mse_factors= [-0.5, -0.4, -0.3, -0.2, -0.1, -0.05, 0, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5]    
        runtimes = stats["Kariz"]
        print('Start misestimation experiment with Kariz, number of DAGs in the workload: ', len(self.dags))
        for dag_id in self.dags:
            if not dag_id.startswith('AQ'): continue 
            dag = self.dags[dag_id]
            dag.name = dag_id
            if dag_id not in runtimes:
                runtimes[dag_id] = {}
            
            for msef in mse_factors:
                print(Fore.GREEN, '\nProcess:', dag_id, ', with mse_factor:', msef, Style.RESET_ALL)
                dag.reset()
                dag.set_misestimation_error(msef)
                runtime, rtl, dataset_inputs = pigsim.start_pig_simulator(dag)
                
                runtimes[dag_id][msef] = {'Cache': 'Kariz', 'DAG_id': dag_id, 'Runtime': runtime, 
                                'runtime list': rtl, 'datasets': dataset_inputs,
                                'lamda': msef}
            #print(Fore.GREEN, runtimes[dag_id], Style.RESET_ALL)
            print(Fore.GREEN, "We are done successfullyyyyy! We could get in!!!! Yoooohooooo", Style.RESET_ALL)
        
        with open('misestimation_result.json', 'w') as dumpf:    
            json.dump(stats, dumpf)

    
    def start_single_dag_coldcache_seqworkload(self):
        stats = {"Kariz": {}, "RCP": {}, "PC": {}, "MRD": {}}
        try:
            with open('simulation_result.json', 'r') as dumpf:
                stats = json.load(dumpf)
        except FileNotFoundError:
                print("No stats available")
            
        runtimes = stats["Kariz"]
        for dag_id in self.dags:
            if not dag_id.startswith('AQ'): continue 
            print('Process ', dag_id)
            dag = self.dags[dag_id]
            runtime, rtl, dataset_inputs = pigsim.start_pig_simulator(dag)
            runtimes[dag_id] = {'Cache': 'Kariz', 'DAG_id': dag_id, 'Runtime': runtime, 'runtime list': rtl, 'datasets': dataset_inputs}
            print(Fore.GREEN, runtimes[dag_id], Style.RESET_ALL)
        
        with open('simulation_result.json', 'w') as dumpf:    
            json.dump(stats, dumpf)
                
    
    def start_multiple_dags_workload(self):
        threads = []
        tdags = self.dags
        for i in range(len(tdags)):
            x = threading.Thread(target=pigsim.start_pig_simulator, args=(tdags[i],))
            threads.append(x)
            x.start()
            
        for th in threads:
            th.join()
            
        return 0

    def select_tpch_dags(self):
        tpch8 = self.statistic_df[self.statistic_df['name'] == 'PigLatin:Q22.pig']
        dag_id = tpch8.iloc[0]['DagId']
        
        runtime = tpch8['runtime'].tolist()
        tpch8_dag = self.dags_byid[dag_id]
        #tpch8_dag.add_edge(6, 9, 0)
        for i in range(min(len(runtime), tpch8_dag.V)):
            tpch8_dag.static_runtime(i, runtime[i], runtime[i]//3)
            
            for ins in range(len(tpch8_dag.inputSize[i])):
                tpch8_dag.inputSize[i][ins] = math.ceil(tpch8_dag.inputSize[i][ins]//(1024*1024)) 
        print(tpch8_dag.timeValue)
        print(tpch8_dag.cachedtimeValue)
        print(tpch8_dag.edges)
        print(tpch8_dag.inputSize)
        print(tpch8_dag.inputs)        
        return tpch8_dag

        
    

