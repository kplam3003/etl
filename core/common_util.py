import os

def mkdirs_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def split_list(the_list, chunk_size):
    result_list = []
    while the_list:
        result_list.append(the_list[:chunk_size])
        the_list = the_list[chunk_size:]
    return result_list