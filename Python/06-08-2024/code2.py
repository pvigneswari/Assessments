# Write a Python function that accepts a lists of strings and returns a single string

# ["hello", "world"] it returns "hello world"

def singleString(s):
    string1 = ''
    for i in s:
        string1 =string1+i
    return string1
    
print(singleString(['Hello','World']))
