# write a function that takes a character and return a boolean, with a value true is the character is a vowel and false if it's anything else

def booleanFun(input):
    vowel = ['a','e','i','o','A','E','I','O','U']
    if input in vowel:
        return True
    else:
        return False
print(booleanFun('c'))
