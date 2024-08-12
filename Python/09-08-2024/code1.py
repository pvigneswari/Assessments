# Write a Python function which takes an integer that has multiple digits, and returns the sum of those digits

def sumOfDigits(num):
    sum = 0
    for i in str(num):
        sum = sum+int(i)
    return sum
print(sumOfDigits(1234))


