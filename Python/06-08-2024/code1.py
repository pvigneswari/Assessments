def list_intersection(a,b):
    output = []
    for i in a:
        if i in b:
            output.append(i)
    return output    
print(list_intersection(a=[1,2,3,4],b=[3,4,5,6]))
          