input = [("London", 15), ("Mumbai", 25)]

modified_data = list(map(lambda x : (x[0],x[1]*(9/5)+32), input))
print(modified_data)