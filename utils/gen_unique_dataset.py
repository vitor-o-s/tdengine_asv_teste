import pandas as pd 

def transform_dataset(path: str):
    full_dataset = pd.read_csv(path + ".csv")
    full_dataset["Timestamp"] = full_dataset["Timestamp"].str.replace('/', '-')
    # Ensure all timestamps have the microsecond part
    # full_dataset["Timestamp"] = full_dataset["Timestamp"].apply(lambda x: x if x.count('.') == 1 else x + '.000000')
    full_dataset["Timestamp"] = pd.to_datetime(full_dataset["Timestamp"], format='%Y-%m-%d %H:%M:%S.%f')

    # Define the locations and corresponding columns
    locations = [
        ([0, 1, 2, 13], '1'),
        ([0, 3, 4, 14], '2'),
        ([0, 5, 6, 15], '3'),
        ([0, 7, 8, 16], '4'),
        ([0, 9, 10, 17], '5'),
        ([0, 11, 12, 18], '6')
    ]
    
    # Create a list to store the transformed datasets
    transformed_datasets = []
    
    # Loop through each location, extract relevant columns, add location column, and append to list
    for loc_cols, loc_name in locations:
        loc_data = full_dataset.iloc[:, loc_cols].copy()
        loc_data.columns = ['Timestamp', 'Magnitude', 'Angle', 'Frequency']
        loc_data['Location'] = loc_name
        transformed_datasets.append(loc_data)
    
    # Concatenate all datasets and sort by Timestamp
    final_dataset = pd.concat(transformed_datasets, ignore_index=True)
    final_dataset = final_dataset.sort_values(by=['Timestamp', 'Location'])
    
    # Save the final dataset
    final_dataset.to_csv(path + '/final_dataset.csv', index=False, header=True)

if __name__ == "__main__":
    dataset_list = [
        "data/1klines",
        "data/5klines",
        "data/10klines",
        "data/50klines",
        "data/100klines",
        "data/500klines",
        "data/648klines",
        "data/1Mlines"
    ]

    for dataset in dataset_list:
        transform_dataset(dataset)
