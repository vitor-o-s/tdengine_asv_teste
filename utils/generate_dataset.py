import datetime
import pandas as pd 

# Next Step  - Get Data using request
pmu_original = pd.read_csv("data/Phasor_data.csv")

# Create sub samples datasets 
one_thousand_lines = pmu_original.iloc[0:1000,:]
five_thousand_lines = pmu_original.iloc[0:5000,:]
ten_thousand_lines = pmu_original.iloc[0:10000,:]
fifty_thousand_lines = pmu_original.iloc[0:50000,:]
hundred_thousand_lines = pmu_original.iloc[0:100000,:]

# Create over sample datasets
# Generate timestamps value
max_time = pmu_original['Timestamp'].iloc[-1]
max_range = 1000000 - pmu_original.last_valid_index() # Last dataset has 1M lines

last_time = datetime.datetime.strptime(max_time,"%Y/%m/%d %H:%M:%S.%f")
timestamp_list = [last_time + datetime.timedelta(microseconds=33500*x) for x in range(max_range)]

# print("Ultimo lista:",timestamp_list[-1])

pmu_copy = pd.read_csv("data/Phasor_data.csv")
list_concat = [pmu_copy for i in range (10)]
pmu_copy = pd.concat(list_concat, ignore_index=True)
one_million_lines = pmu_copy.iloc[0:1000000,:]
last_index = pmu_original.last_valid_index()
for i in range(max_range):
	pmu_copy.at[i+last_index, 'Timestamp'] =  timestamp_list[i]
# print("Index 1M dataset",one_million_lines.last_valid_index())
# print(one_million_lines['Timestamp'].iloc[-1])
five_hundred_lines = one_million_lines.iloc[0:500000,:]
six_hundred_lines = one_million_lines.iloc[0:648000,:]


# Save new datasets 
one_thousand_lines.to_csv("data/1klines.csv",index=False)
five_thousand_lines.to_csv("data/5klines.csv",index=False)
ten_thousand_lines.to_csv("data/10klines.csv",index=False)
fifty_thousand_lines.to_csv("data/50klines.csv",index=False)
hundred_thousand_lines.to_csv("data/100klines.csv",index=False)
five_hundred_lines.to_csv("data/500klines.csv",index=False)
six_hundred_lines.to_csv("data/648klines.csv",index=False)
one_million_lines.to_csv("data/1Mlines.csv",index=False)
