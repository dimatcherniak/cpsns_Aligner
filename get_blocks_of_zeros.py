import numpy as np

def find_zero_blocks(arr):
    """
    Find blocks of consecutive zeros in a numpy array.

    Parameters:
    arr (numpy.ndarray): Input array.

    Returns:
    list of tuples: Each tuple contains the start index and length of a block of consecutive zeros.
    """
    # Find the positions of zeros
    zero_positions = np.where(arr == 0)[0]

    # Initialize variables to store the results
    blocks = []
    start = None

    # Iterate through the positions of zeros
    for i in range(len(zero_positions)):
        if start is None:
            start = zero_positions[i]
        if i == len(zero_positions) - 1 or zero_positions[i] + 1 != zero_positions[i + 1]:
            end = zero_positions[i]
            blocks.append((start, end - start + 1))
            start = None

    return blocks

# Sample usage
#arr = np.array([1, 0, 0, 2, 0, 0, 0, 3, 0, 0])
#blocks = find_zero_blocks(arr)
#print("Blocks of consecutive zeros (start index, length):", blocks)
