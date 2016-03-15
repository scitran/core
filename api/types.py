from . import util

# Origin represents the different methods a request can be authenticated.
Origin = util.Enum('Origin', {
    'user':    'user',    # An authenticated user
    'device':  'device',  # A connected device (reaper, script, etc)
    'job':     'job',     # Made on behalf of a job (downloading data, uploading results, etc)
    'unknown': 'unknown', # Other or public
})
