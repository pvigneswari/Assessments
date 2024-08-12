def anagramSorting(str1,str2):
    print(sorted(str1))
    print(sorted(str2))

    return sorted(str1.lower()) == sorted(str2.lower())

print(anagramSorting('rat','art'))
print(anagramSorting('listen','silent'))
print(anagramSorting('hero','zero'))

