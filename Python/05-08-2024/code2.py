sentence = "The quick brown fox jumps over the lazy dog"
def long_word(sentence):
    words = sentence.split()
    output = []
    # get longest word from the list
    longest_word =max(words, key=len)
    # get length of longest word
    lengthOfLongestWord = len(longest_word) 
    print('length of longest word is',lengthOfLongestWord,'characters') 
    for word in words:
        # compare length of each word with max word length 
       if(len(word) == lengthOfLongestWord):
            output.append(word)
    output.append(lengthOfLongestWord) 
    return output

print(long_word(sentence))
