# @author:  Gunnar Schaefer

import webapp2


class NIMSRequestHandler(webapp2.RequestHandler):

    def __init__(self, request=None, response=None):
        webapp2.RequestHandler.__init__(self, request, response)
        self.request.remote_user = self.request.get('user', None) # FIXME: auth system should set REMOTE_USER
        self.userid = self.request.remote_user or '@public'
        self.user = self.app.db.users.find_one({'_id': self.userid})
        self.user_is_superuser = self.user.get('superuser')
        self.response.headers['Content-Type'] = 'application/json'
