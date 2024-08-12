lst = [4, 0, 2, 4, 5, 0, 7]

def ZeroEnd(lst):
    output=[]
    for i in lst:
        if i !=0:
            output.append(i)
    for i in lst:
        if i ==0:
            output.append(i) 
    return output
print(ZeroEnd(lst))
