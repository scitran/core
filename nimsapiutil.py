# @author:  Gunnar Schaefer

import webapp2


class NIMSRequestHandler(webapp2.RequestHandler):

    def __init__(self, request=None, response=None):
        webapp2.RequestHandler.__init__(self, request, response)
        self.response.headers['Content-Type'] = 'application/json'
