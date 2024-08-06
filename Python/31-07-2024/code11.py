list_of_lists = [[2,4,5], [6,4,8], [5,7,3]]
def list(x):
    output = []
    for i in x:
        output.append(x[1])
    return output
list(list_of_lists)