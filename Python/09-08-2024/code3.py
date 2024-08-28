# Write a function which takes a string and returns a new string with all duplicate characters removed
def newString(string1):
    output =[]
    for i in string1:
        if i not in output:
            output.append(i)
    return ''.join(output)

print(newString('aaabbbccc111222333'))