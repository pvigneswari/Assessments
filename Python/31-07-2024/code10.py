input = [("London", 15), ("Mumbai", 25)]
result=[]
def temp_degrees(n):
    for i in n:
        result.append((i[0], (i[1] * (9/5) + 32)))

    return result

print(temp_degrees(input))

# input = [("London", 15), ("Mumbai", 25)]
# results = [(city,(temp*(9/5)+32)) for city, temp in input]
# print(results)