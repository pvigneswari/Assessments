lst = [5, 3, 4, 4, 2, 6, 3, 4, 5, 2, 5, 6, 8, 1, 3, 2, 0, 6, 4]

def newlist(lst):
    output=[]
    for i in lst:
        if i not in output:
            output.append(i)
    return output
print(newlist(lst))
