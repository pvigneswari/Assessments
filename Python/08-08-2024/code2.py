# Write a function that accepts an integer and which checks to see if the integer is a prime number

def isPrime(num):
    for i in range(2,num):
        if(num%i == 0):
            return False
    return True

print(isPrime(7))