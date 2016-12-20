class APIStorageException(Exception):
    pass

class APIConsistencyException(Exception):
    pass

class APIConflictException(Exception):
    pass

class APINotFoundException(Exception):
    pass

class APIPermissionException(Exception):
    pass

class APIValidationException(Exception):
    def __init__(self, errors):

        super(APIValidationException, self).__init__('API Validation Error.')
        self.errors = errors

def noop(*args, **kwargs): # pylint: disable=unused-argument
    pass
