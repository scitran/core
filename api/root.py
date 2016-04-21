import re
import markdown

from . import base
from . import config

log = config.log

class Root(base.RequestHandler):

    def head(self):
        """
        .. http:head:: /api

            Confirm endpoint is ready for requests

            :statuscode 200: no error
        """

        pass

    def get(self):
        """
        .. http:get:: /api

            Return API documentation

            :statuscode 200: no error

            **Example request**:

            .. sourcecode:: http

                GET /api HTTP/1.1
                Host: demo.flywheel.io
                Accept: text/html

            **Example response**:

            .. sourcecode:: http

                HTTP/1.1 200 OK
                Vary: Accept-Encoding
                Content-Type: text/html
                <html>
                <head>
                <title>SciTran API</title>
                <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
                <style type="text/css">
                table {width:0%; border-width:1px; padding: 0;border-collapse: collapse;}
                table tr {border-top: 1px solid #b8b8b8; background-color: white; margin: 0; padding: 0;}
                table tr:nth-child(2n) {background-color: #f8f8f8;}
                table thead tr :last-child {width:100%;}
                table tr th {font-weight: bold; border: 1px solid #b8b8b8; background-color: #cdcdcd; margin: 0; padding: 6px 13px;}
                table tr th {font-weight: bold; border: 1px solid #b8b8b8; background-color: #cdcdcd; margin: 0; padding: 6px 13px;}
                table tr td {border: 1px solid #b8b8b8; margin: 0; padding: 6px 13px;}
                table tr th :first-child, table tr td :first-child {margin-top: 0;}
                table tr th :last-child, table tr td :last-child {margin-bottom: 0;}
                </style>
                </head>
                <body style="min-width:900px">
                <form name="username" action="" method="get">
                Username: <input type="text" name="user">
                Root: <input type="checkbox" name="root" value="1">
                <input type="submit" value="Generate Custom Links">
                </form>
                <table>
                <thead>
                <tr>
                <th align="left">Resource</th>
                <th align="left">Description</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td align="left"><a href="/api/sites">/sites</a></td>
                <td align="left">local and remote sites</td>
                </tr>
                <tr>
                <td align="left">/download</td>
                <td align="left">download</td>
                </tr>
                <tr>
                <td align="left"><a href="/api/users">/users</a></td>
                <td align="left">list of users</td>
                </tr>
                <tr>
                <td align="left"><a href="/api/users/self">/users/self</a></td>
                <td align="left">user identity</td>
                </tr>
                <tr>
                <td align="left"><a href="/api/users/roles">/users/roles</a></td>
                <td align="left">user roles</td>
                </tr>
                <tr>
                <td align="left"><a href="/api/users/*&amp;lt;uid&amp;gt;*">/users/<em>&lt;uid&gt;</em></a></td>
                <td align="left">details for user <em>&lt;uid&gt;</em></td>
                </tr>
                <tr>
                <td align="left"><a href="/api/users/*&amp;lt;uid&amp;gt;*/groups">/users/<em>&lt;uid&gt;</em>/groups</a></td>
                <td align="left">groups for user <em>&lt;uid&gt;</em></td>
                </tr>
                <tr>
                <td align="left"><a href="/api/users/*&amp;lt;uid&amp;gt;*/projects">/users/<em>&lt;uid&gt;</em>/projects</a></td>
                <td align="left">projects for user <em>&lt;uid&gt;</em></td>
                </tr>
                <tr>
                <td align="left"><a href="/api/groups">/groups</a></td>
                <td align="left">list of groups</td>
                </tr>
                <tr>
                <td align="left">/groups/<em>&lt;gid&gt;</em></td>
                <td align="left">details for group <em>&lt;gid&gt;</em></td>
                </tr>
                <tr>
                <td align="left">/groups/<em>&lt;gid&gt;</em>/projects</td>
                <td align="left">list of projects for group <em>&lt;gid&gt;</em></td>
                </tr>
                <tr>
                <td align="left">/groups/<em>&lt;gid&gt;</em>/sessions</td>
                <td align="left">list of sessions for group <em>&lt;gid&gt;</em></td>
                </tr>
                <tr>
                <td align="left"><a href="/api/projects">/projects</a></td>
                <td align="left">list of projects</td>
                </tr>
                <tr>
                <td align="left"><a href="/api/projects/groups">/projects/groups</a></td>
                <td align="left">groups for projects</td>
                </tr>
                <tr>
                <td align="left"><a href="/api/projects/schema">/projects/schema</a></td>
                <td align="left">schema for single project</td>
                </tr>
                <tr>
                <td align="left">/projects/<em>&lt;pid&gt;</em></td>
                <td align="left">details for project <em>&lt;pid&gt;</em></td>
                </tr>
                <tr>
                <td align="left">/projects/<em>&lt;pid&gt;</em>/sessions</td>
                <td align="left">list sessions for project <em>&lt;pid&gt;</em></td>
                </tr>
                <tr>
                <td align="left"><a href="/api/sessions">/sessions</a></td>
                <td align="left">list of sessions</td>
                </tr>
                <tr>
                <td align="left"><a href="/api/sessions/schema">/sessions/schema</a></td>
                <td align="left">schema for single session</td>
                </tr>
                <tr>
                <td align="left">/sessions/<em>&lt;sid&gt;</em></td>
                <td align="left">details for session <em>&lt;sid&gt;</em></td>
                </tr>
                <tr>
                <td align="left">/sessions/<em>&lt;sid&gt;</em>/move</td>
                <td align="left">move session <em>&lt;sid&gt;</em> to a different project</td>
                </tr>
                <tr>
                <td align="left">/sessions/<em>&lt;sid&gt;</em>/acquisitions</td>
                <td align="left">list acquisitions for session <em>&lt;sid&gt;</em></td>
                </tr>
                <tr>
                <td align="left"><a href="/api/acquisitions/schema">/acquisitions/schema</a></td>
                <td align="left">schema for single acquisition</td>
                </tr>
                <tr>
                <td align="left">/acquisitions/<em>&lt;aid&gt;</em></td>
                <td align="left">details for acquisition <em>&lt;aid&gt;</em></td>
                </tr>
                <tr>
                <td align="left"><a href="/api/collections">/collections</a></td>
                <td align="left">list of collections</td>
                </tr>
                <tr>
                <td align="left"><a href="/api/collections/schema">/collections/schema</a></td>
                <td align="left">schema for single collection</td>
                </tr>
                <tr>
                <td align="left">/collections/<em>&lt;cid&gt;</em></td>
                <td align="left">details for collection <em>&lt;cid&gt;</em></td>
                </tr>
                <tr>
                <td align="left">/collections/<em>&lt;cid&gt;</em>/sessions</td>
                <td align="left">list of sessions for collection <em>&lt;cid&gt;</em></td>
                </tr>
                <tr>
                <td align="left">/collections/<em>&lt;cid&gt;</em>/acquisitions</td>
                <td align="left">list of acquisitions for collection <em>&lt;cid&gt;</em></td>
                </tr>
                <tr>
                <td align="left"><a href="/api/schema/group">/schema/group</a></td>
                <td align="left">group schema</td>
                </tr>
                <tr>
                <td align="left"><a href="/api/schema/user">/schema/user</a></td>
                <td align="left">user schema</td>
                </tr>
                </tbody>
                </table></body>
                </html>


            :query sort: one of ``hit``, ``created-at``
            :query offset: offset number. default is 0
            :query limit: limit number. default is 30
            :reqheader Accept: the response content type depends on
                              :mailheader:`Accept` header
            :reqheader Authorization: optional OAuth token to authenticate
            :resheader Content-Type: this depends on :mailheader:`Accept`
                                    header of request
        """

        resources = """
            Resource                            | Description
            :-----------------------------------|:-----------------------
            [(/sites)]                          | local and remote sites
            /download                           | download
            [(/users)]                          | list of users
            [(/users/self)]                     | user identity
            [(/users/roles)]                    | user roles
            [(/users/*<uid>*)]                  | details for user *<uid>*
            [(/users/*<uid>*/groups)]           | groups for user *<uid>*
            [(/users/*<uid>*/projects)]         | projects for user *<uid>*
            [(/groups)]                         | list of groups
            /groups/*<gid>*                     | details for group *<gid>*
            /groups/*<gid>*/projects            | list of projects for group *<gid>*
            /groups/*<gid>*/sessions            | list of sessions for group *<gid>*
            [(/projects)]                       | list of projects
            [(/projects/groups)]                | groups for projects
            [(/projects/schema)]                | schema for single project
            /projects/*<pid>*                   | details for project *<pid>*
            /projects/*<pid>*/sessions          | list sessions for project *<pid>*
            [(/sessions)]                       | list of sessions
            [(/sessions/schema)]                | schema for single session
            /sessions/*<sid>*                   | details for session *<sid>*
            /sessions/*<sid>*/move              | move session *<sid>* to a different project
            /sessions/*<sid>*/acquisitions      | list acquisitions for session *<sid>*
            [(/acquisitions/schema)]            | schema for single acquisition
            /acquisitions/*<aid>*               | details for acquisition *<aid>*
            [(/collections)]                    | list of collections
            [(/collections/schema)]             | schema for single collection
            /collections/*<cid>*                | details for collection *<cid>*
            /collections/*<cid>*/sessions       | list of sessions for collection *<cid>*
            /collections/*<cid>*/acquisitions   | list of acquisitions for collection *<cid>*
            [(/schema/group)]                   | group schema
            [(/schema/user)]                    | user schema
            """

        if self.debug and self.uid:
            resources = re.sub(r'\[\((.*)\)\]', r'[\1](/api\1?user=%s&root=%r)' % (self.uid, self.superuser_request), resources)
            resources = re.sub(r'(\(.*)\*<uid>\*(.*\))', r'\1%s\2' % self.uid, resources)
        else:
            resources = re.sub(r'\[\((.*)\)\]', r'[\1](/api\1)', resources)
        resources = resources.replace('<', '&lt;').replace('>', '&gt;').strip()

        self.response.headers['Content-Type'] = 'text/html; charset=utf-8'
        self.response.write('<html>\n')
        self.response.write('<head>\n')
        self.response.write('<title>SciTran API</title>\n')
        self.response.write('<meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">\n')
        self.response.write('<style type="text/css">\n')
        self.response.write('table {width:0%; border-width:1px; padding: 0;border-collapse: collapse;}\n')
        self.response.write('table tr {border-top: 1px solid #b8b8b8; background-color: white; margin: 0; padding: 0;}\n')
        self.response.write('table tr:nth-child(2n) {background-color: #f8f8f8;}\n')
        self.response.write('table thead tr :last-child {width:100%;}\n')
        self.response.write('table tr th {font-weight: bold; border: 1px solid #b8b8b8; background-color: #cdcdcd; margin: 0; padding: 6px 13px;}\n')
        self.response.write('table tr th {font-weight: bold; border: 1px solid #b8b8b8; background-color: #cdcdcd; margin: 0; padding: 6px 13px;}\n')
        self.response.write('table tr td {border: 1px solid #b8b8b8; margin: 0; padding: 6px 13px;}\n')
        self.response.write('table tr th :first-child, table tr td :first-child {margin-top: 0;}\n')
        self.response.write('table tr th :last-child, table tr td :last-child {margin-bottom: 0;}\n')
        self.response.write('</style>\n')
        self.response.write('</head>\n')
        self.response.write('<body style="min-width:900px">\n')
        if self.debug and not self.get_param('user'):
            self.response.write('<form name="username" action="" method="get">\n')
            self.response.write('Username: <input type="text" name="user">\n')
            self.response.write('Root: <input type="checkbox" name="root" value="1">\n')
            self.response.write('<input type="submit" value="Generate Custom Links">\n')
            self.response.write('</form>\n')
        self.response.write(markdown.markdown(resources, ['extra']))
        self.response.write('</body>\n')
        self.response.write('</html>\n')
