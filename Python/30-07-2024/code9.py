def palindrome(word):
    list1=list(word)
    reverse_word=''.join(list1)
    return word == reverse_word
print(palindrome('madam'))

# def palindrome(word):
#     return word == word[::-1]

# if palindrome('madam'):
#     print("word is palindrome")
# else:
#     print('word is not palindrome')