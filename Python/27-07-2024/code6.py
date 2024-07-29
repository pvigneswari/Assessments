lst = [1,2,3,4,5,6,7,8,9]

def new_lst(lstr):
    output = []
    for i in lstr:
        output.append(i*3)  
    return output

print(new_lst(lst))