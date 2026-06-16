def find_substring(string, substring):

    # Edge case 1: both empty → return 0
    if len(string) == 0 and len(substring) == 0:
        return 0

    # Edge case 2: substring empty → return 0
    if len(substring) == 0:
        return 0

    # Edge case 3: substring longer than string
    if len(substring) > len(string):
        return -1

    s_len  = len(string)
    sub_len = len(substring)

    # Slide a window of size sub_len across string
    for i in range(s_len - sub_len + 1):
        match = True

        # Compare character by character
        for j in range(sub_len):
            if string[i + j] != substring[j]:
                match = False
                break          # mismatch → stop inner loop

        if match:
            return i           # found! return starting index

    return -1                  # not found


# ── Test Cases ──────────────────────────────────
print(find_substring("Hello", "ll"))       #  2 ✅
print(find_substring("Hello", "He"))       #  0 ✅
print(find_substring("Hello", "lo"))       #  3 ✅
print(find_substring("Hello", "world"))    # -1 ✅
print(find_substring("", ""))              #  0 ✅
print(find_substring("Hello", ""))         #  0 ✅
print(find_substring("", "ll"))            # -1 ✅
print(find_substring("aaaa", "aa"))        #  0 ✅
print(find_substring("abcabc", "abc"))     #  0 ✅
print(find_substring("abcabc", "cab"))     #  2 ✅