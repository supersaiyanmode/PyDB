
def byte_chunker(iterator, chunk_size=1):
    exhausted = False
    while not exhausted:
        res = b''
        for _ in range(chunk_size):
            try:
                res += next(iterator)
            except StopIteration:
                exhausted = True
                break
        if res:
            yield res

