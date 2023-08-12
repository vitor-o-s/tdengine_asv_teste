import random

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
