class APIStorageException(Exception):
    pass

class APIConsistencyException(Exception):
    pass

class APIConflictException(Exception):
    pass

class APINotFoundException(Exception):
    pass

def noop(*args, **kwargs): # pylint: disable=unused-argument
    pass
