
l1=[1,0,3,0,4,0,6,7]

def move_zeros(nums):
    #Extract non-zero elements
    non_zeros = [x for x in nums if x != 0]
    #Create list of zeros
    zeros = [0] * (len(nums) - len(non_zeros))
    return non_zeros + zeros

print(move_zeros(l1))


def move_zeros(nums):
    j = 0  # position for next non-zero

    for i in range(len(nums)):
        if nums[i] != 0:
            nums[j], nums[i] = nums[i], nums[j]
            j += 1

    return nums

# Example
nums = [1, 0, 2, 0, 3, 0, 4]
print(move_zeros(nums))