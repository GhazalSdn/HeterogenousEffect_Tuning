import pandas as pd

import numpy as np
import pickle

FILE_PATH = '../'

print("### Load Data ######################")
app_data = pd.read_csv('application_data_meanPerc95.csv', delimiter=', ')

conf_units = {'spark.executor.memory': 'g',
 'spark.shuffle.service.index.cache.size': 'm',
 'spark.cleaner.periodicGC.interval': 'min',
 'spark.storage.memoryMapThreshold': 'm',
 'spark.io.compression.snappy.blockSize': 'k',
 'spark.executor.heartbeatInterval': 's',
 'spark.shuffle.file.buffer': 'k',
 'spark.kyroserializer.buffer': 'k',
 'spark.locality.wait': 's',
 'spark.speculation.interval': 'ms',
 'spark.broadcast.blocksize': 'm',
 'spark.shuffle.registration.timeout': 'ms',
 'spark.rpc.lookupTimeout': 's'}

for conf in conf_units:
    app_data.loc[:, (conf)] = [int(elem.split(conf_units[conf])[0]) for elem in app_data[conf]]

colmns = ['spark.executor.cores',
 'spark.executor.memory',
 'spark.default.parallelism',
 'spark.serializer',
 'spark.task.cpus',
 'executorDeserializeCpuTime',
 'executorRunTime',
 'executorCpuTime',
 'resultSize',
 'jvmGcTime',
 'Execution_Time'
 ]

data = app_data[colmns]
l = []
for i in range(len(data)):
    if(data.iloc[i]['spark.serializer'] == 'org.apache.spark.serializer.KryoSerializer'):
        l += [0]
    else:
        l += [1]
data['spark.serializer'] = l


data.to_csv('preprocessed_app_data.csv', index=False,)