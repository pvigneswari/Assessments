input = [("London", 15), ("Mumbai", 25)]
# results = ['ganesh']
def temp_degrees(arr):
    results = []
    for city in arr:
        far = city[1] * 9/5 + 32
        city_data = (city[0], far)
  
        results.append(city_data)
        
    return results
              
print(temp_degrees(input))
# print(results)
