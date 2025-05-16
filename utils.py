def divide_chunks(data, n):
    """
    Split a list into n nearly equal-sized chunks.
    :param data: List to split
    :param n: Number of chunks
    :return: A list of n chunks (sublists)
    """
    k, m = divmod(len(data), n)
    return [data[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

def scale_list_repeat(lst, n):
    """
    Scale a list to a given length by repeating its elements.

    If the list is empty, return an empty list.
    Otherwise, return a list of the given length,
    where each element is from the original list,
    repeated as necessary.

    :param lst: List to scale
    :param n: Desired length
    :return: A list of length n, where each element is from the original list
    """

    if not lst:
        return []
    if len(lst) >= n:
        return lst[:n]
    return [lst[i % len(lst)] for i in range(n)]