from collections import defaultdict

def group_anagrams(words):
    anagram_map = defaultdict(list)

    for word in words:
        key = "".join(sorted(word))  # "eat" → "aet"
        anagram_map[key].append(word)

    print(f'anagram_map : {anagram_map}')
    return list(anagram_map.values())


# Test
words = ["eat", "tea", "tan", "ate", "nat", "bat"]
print(group_anagrams(words))
# [['eat', 'tea', 'ate'], ['tan', 'nat'], ['bat']]