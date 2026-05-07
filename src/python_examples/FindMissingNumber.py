# Find missing number in array

def find_missing_sum(nums):
    """
    Using sum formula: n(n+1)/2
    Time: O(n), Space: O(1)
    """
    n = len(nums) + 1
    expected_sum = n * (n + 1) // 2
    actual_sum = sum(nums)
    return expected_sum - actual_sum


def find_missing_xor(nums):
    """
    Using XOR property: a ^ a = 0, a ^ 0 = a
    Time: O(n), Space: O(1)
    """
    n = len(nums) + 1
    xor_all = 0
    xor_nums = 0

    for i in range(1, n + 1):
        xor_all ^= i

    for num in nums:
        xor_nums ^= num

    return xor_all ^ xor_nums


def find_missing_set(nums):
    """
    Using set difference
    Time: O(n), Space: O(n)
    """
    n = len(nums) + 1
    expected = set(range(1, n + 1))
    actual = set(nums)
    return (expected - actual).pop()


def find_missing_binary_search(nums):
    """
    Using binary search (requires sorted array)
    Time: O(log n), Space: O(1)
    """
    nums.sort()
    left, right = 0, len(nums)

    while left < right:
        mid = (left + right) // 2
        if nums[mid] > mid + 1:
            right = mid
        else:
            left = mid + 1

    return left + 1


# Example usage
if __name__ == "__main__":
    test_cases = [
        [1, 2, 4, 5],
        [3, 7, 1, 2, 8, 4, 5],
        [1, 2, 3, 4, 5, 6, 8, 9, 10],
        [2, 3, 4, 5],
        [1]
    ]

    for arr in test_cases:
        print(f"Array: {arr}")
        print(f"Sum method:    {find_missing_sum(arr)}")
        print(f"XOR method:    {find_missing_xor(arr)}")
        print(f"Set method:    {find_missing_set(arr)}")
        print(f"Binary Search: {find_missing_binary_search(arr.copy())}")
        print("-" * 50)
