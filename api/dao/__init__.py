class APIStorageException(Exception):
    pass

class APIConsistencyException(Exception):
    pass

def noop(*args, **kwargs):
    pass
