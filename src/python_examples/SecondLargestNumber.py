# Find second largest number in a list

def find_second_largest(numbers):
    """
    Find the second largest number in a list.
    Returns None if list has less than 2 distinct elements.
    """
    if len(numbers) < 2:
        return None

    # Remove duplicates and sort in descending order
    unique_numbers = list(set(numbers))

    if len(unique_numbers) < 2:
        return None

    unique_numbers.sort(reverse=True)
    return unique_numbers[1]


def find_second_largest_optimized(numbers):
    """
    Find second largest with O(n) time complexity.
    Single pass through the list.
    """
    if len(numbers) < 2:
        return None

    largest = second_largest = float('-inf')

    for num in numbers:
        if num > largest:
            second_largest = largest
            largest = num
        elif num > second_largest and num != largest:
            second_largest = num

    return None if second_largest == float('-inf') else second_largest


# Example usage
if __name__ == "__main__":
    # Test cases
    test_lists = [
        [10, 5, 20, 8, 15],
        [1, 2, 3, 4, 5],
        [5, 5, 5, 5],
        [10, 10, 9, 9, 8],
        [42],
        [],
        [7, 7, 3, 3, 1, 1]
    ]

    print("Method 1: Using set and sort")
    print("-" * 40)
    for lst in test_lists:
        result = find_second_largest(lst)
        print(f"List: {lst}")
        print(f"Second largest: {result}\n")

    print("\nMethod 2: Optimized O(n) approach")
    print("-" * 40)
    for lst in test_lists:
        result = find_second_largest_optimized(lst)
        print(f"List: {lst}")
        print(f"Second largest: {result}\n")
