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

# Code 2:
list1 =[4,0,2,4,5,0,1,7,1,2,3]

def zeroLast(list1):
    output=[]
    zeroCount=list1.count(0)
    for i in list1:
        if (i!=0):
            output.append(i)
    # for i in list1:
    #     if(i==0):
    #         output.append(i)
    output.extend([0]*zeroCount)
    return output
print(zeroLast(list1))
