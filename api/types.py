from . import util

# Origin represents the different methods a request can be authenticated.
Origin = util.Enum('Origin', {
    'user':    'user',    # An authenticated user
    'device':  'device',  # A connected device (reaper, script, etc)
    'job':     'job',     # Made on behalf of a job (downloading data, uploading results, etc)
    'system':  'system',  # Created by the system (a job initiated by a rule, etc)
    'unknown': 'unknown'  # Other or public
})
