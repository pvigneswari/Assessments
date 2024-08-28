# challenge: write a function that accepts two lists and returns a list of the common elements between the input lists
lst1 = [1, 2, 3, 4]
lst2 = [3, 4, 5, 6]
output=[]
def commonElement(lst1,lst2):
    # output=[]
    for i in lst1:
        if i in lst2:
            output.append(i) 
    return output
commonElement(lst1,lst2)
print(output)
        