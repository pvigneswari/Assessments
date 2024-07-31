list_of_lists = [[2,4,5], [6,4,8], [5,7,3]]
def middle(list):
    output = []
    for i in list:
        output.append(i[1])
    return output
print(middle(list_of_lists))
        
