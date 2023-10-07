import random
import csv
from datetime import datetime

def get_balanced_sets(number: int, path: str) -> list:
    """
    Returns a list of sets balanced by the number of lines.
    
    Parameters:
    - number (int): The number of sets to divide the file into.
    - path (str): The path to the file.
    
    Returns:
    - list: A list of sets, each containing lines from the file.
    """
    # Read the file and get all lines
    with open(path, 'r') as file:
        lines = file.readlines()
    
    # Calculate the number of lines in each set
    lines_per_set = len(lines) // number
    
    # Create the sets
    sets_list = []
    for _ in range(number):
        # Randomly select lines to form each set
        selected_lines = random.sample(lines, lines_per_set)
        sets_list.append(set(selected_lines))
        
        # Remove the selected lines to ensure they are not repeated
        for line in selected_lines:
            lines.remove(line)
    
    return sets_list

def loading_data(file_path) -> list[tuple]:
    """
    Returns a list of all sets.
    
    Parameters:
    - file_path (str): The path to the file.
    
    Returns:
    - list: A list of sets, each containing lines from the file.
    """
    data = []
    with open(file_path, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        
        for line in reader:
            try:
                converted_row = (
                    datetime.strptime(line[0], '%Y-%m-%d %H:%M:%S.%f'),
                    float(line[1]),
                    float(line[2]),
                    float(line[3]),
                    int(line[4])
                )
                # 2012-01-03 01:00:00.000, 78807.3672, 9.3185, 60.0218, 1
                data.append(tuple(converted_row))
            except ValueError as e:
                print(f"Skipping row due to error: {e}")
                print("Problematic row:", line)
                
    return data

def load_csv_data(file_path):
    """
    Load data from a CSV file.

    :param file_path: str, path to the CSV file
    :return: list of tuples, each tuple represents a row of data
    """
    data = []
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            data.append(tuple(row))
    return data
