import pandas as pd 

def split_dataset(path: str):

    full_dataset = pd.read_csv(path + ".csv")
    full_dataset["Timestamp"] = full_dataset["Timestamp"].apply(lambda x: "'"+ x +"'").str.replace('/', '-')
	
    # Get all lines but only 4 columns
    # Split by location
    first_loc = full_dataset.iloc[:, [0, 1, 2, 13]]
    second_loc = full_dataset.iloc[:, [0, 3, 4, 14]]
    third_loc = full_dataset.iloc[:, [0, 5, 6, 15]]
    fourth_loc = full_dataset.iloc[:, [0, 7, 8, 16]]
    fifth_loc = full_dataset.iloc[:, [0, 9, 10, 17]]
    sixth_loc = full_dataset.iloc[:, [0, 11, 12, 18]]
	
	# Save new datasets 
    first_loc.to_csv(path + '/1_loc.csv', index=False, header=False)
    second_loc.to_csv(path + '/2_loc.csv', index=False, header=False)
    third_loc.to_csv(path + '/3_loc.csv', index=False, header=False)
    fourth_loc.to_csv(path + '/4_loc.csv', index=False, header=False)
    fifth_loc.to_csv(path + '/5_loc.csv', index=False, header=False)
    sixth_loc.to_csv(path + '/6_loc.csv', index=False, header=False)


if __name__ == "__main__":
    dataset_list = ["data/1klines",
                    "data/5klines",
                    "data/10klines",
                    "data/50klines",
                    "data/100klines",
                    "data/500klines",
                    "data/648klines",
                    "data/1Mlines"]

    for dataset in dataset_list:
        split_dataset(dataset)