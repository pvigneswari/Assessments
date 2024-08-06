# input_string = 'Helloworld'

def W_count(str):
    vowels = 'aeioAEIOU'
    count = 0
    for char in str:
        if char in vowels:
            count += 1
    return count

str = input('enter the string:')
print(W_count(str))
    