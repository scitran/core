FROM python:2.7

ENV MONGO_PATH='mongodb://mongo:27017/scitran'
ENV BASE_URL='http://scitran-core:8080/api'

VOLUME /usr/src/tests
WORKDIR /usr/src/tests

COPY requirements-integration-test.txt requirements.txt
RUN pip install -r requirements.txt

# Install node for testing tools
# We need node >= 4.0 for newman so install from node-source repo
RUN curl --silent https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add -
RUN echo "deb https://deb.nodesource.com/5.x trusty main" | tee /etc/apt/sources.list.d/nodesource.list
RUN echo "deb-src https://deb.nodesource.com/5.x trusty main" |  tee -a /etc/apt/sources.list.d/nodesource.list
RUN apt-get update && apt-get -y install nodejs npm && ln -s /usr/bin/nodejs /usr/bin/node 

# Install abao: https://github.com/cybertk/abao
# Using our fork of abao to expand jsonschema refs
RUN npm install -g git+https://github.com/flywheel-io/abao.git#add-jsonschema-deref

# Install newman for running postman collection tests from CLI
RUN npm install -g newman

ENTRYPOINT ["py.test"]
