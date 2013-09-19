# @author:  Gunnar Schaefer

import webapp2


class NIMSRequestHandler(webapp2.RequestHandler):

    def __init__(self, request=None, response=None):
        webapp2.RequestHandler.__init__(self, request, response)
        if self.request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            print '    AJAX request'
        elif self.request.get('debug', False):
            print '    debug request'
        else:
            print '    browser request'
            self.redirect('http://cni.stanford.edu', abort=True)
        self.response.headers['Content-Type'] = 'application/json'
