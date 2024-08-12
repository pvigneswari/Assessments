input_list = [[4, 5, 6], [7, 8, 9]]
output_list = []

for sublist in input_list:
    for i in sublist:
        output_list.append(i)

print("Output list:", output_list)

# def flatten(list_of_lists):
#     return list(itertools.chain.from_iterable(list_of_lists))

# output_list = flatten(input_list)