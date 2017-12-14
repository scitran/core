FROM python:2.7-alpine3.6 as build

RUN apk add --no-cache build-base curl

WORKDIR /src/nginx-unit

RUN curl -L https://github.com/nginx/unit/archive/master.tar.gz | tar xz --strip-components 1
RUN ./configure --prefix=/usr/local --modules=lib --state=/var/local/unit --pid=/var/unit.pid --log=/var/log/unit.log \
 && ./configure python \
 && make install


FROM python:2.7-alpine3.6 as dist

RUN apk add --no-cache git

COPY --from=build /usr/local/sbin/unitd /usr/local/sbin/unitd
COPY --from=build /usr/local/lib/python.unit.so /usr/local/lib/python.unit.so

EXPOSE 80 8080 27017

VOLUME /data/db

WORKDIR /src/core

COPY docker/unit.json /var/local/unit/conf.json
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .
RUN pip install -e .

CMD ["unitd", "--control", "*:8080", "--no-daemon", "--log", "/dev/stdout"]


FROM dist as testing

RUN apk add --no-cache --repository http://dl-cdn.alpinelinux.org/alpine/edge/community mongodb=3.4.4-r0

RUN pip install -r tests/requirements.txt

CMD ["./docker/dev+mongo.sh"]
