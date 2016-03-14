

## Examples
Following the examples below will server up scitran/core with uwsgi on port 8080
with auto-reload enabled. This will not utilize HTTPS, thus is meant only for
development.

The below examples do not account for complexities of docker volumes, and
preserving their contents across container instances.


```
# Build Example:
   docker build -t scitran-core -f docker/Dockerfile .

# Run Example:
   # First start mongodb
   docker run --name some-mongo -d mongo

   # Then startup scitran-core, attaching to linked mongo.
   docker run \
     --name scitran-core \
     -e "SCITRAN_PERSISTENT_DB_URI=mongodb://some-mongo:27017/scitran" \
     -e "SCITRAN_CORE_INSECURE=true" \
     -e "SCITRAN_CORE_DRONE_SECRET=change-me" \
     -v $(pwd)/persistent/data:/var/scitran/data \
     -v $(pwd):/var/scitran/code/api \
     --link some-mongo \
     -p 0.0.0.0:8080:8080 \
     scitran-core \
       uwsgi \
         --ini /var/scitran/config/uwsgi-config.ini \
         --http 0.0.0.0:8080 \
         --http-keepalive \
         --python-autoreload 1


# Bootstrap Account Example:
   docker run \
     -e "SCITRAN_RUNTIME_HOST=scitran-core" \
     -e "SCITRAN_RUNTIME_PORT=8080" \
     -e "SCITRAN_RUNTIME_PROTOCOL=http" \
     -e "SCITRAN_CORE_DRONE_SECRET=change-me" \
     --link scitran-core \
     --rm \
     -v /dev/bali.prod/docker/uwsgi/bootstrap-dev.json:/accounts.json \
     scitran-core \
       /var/scitran/code/api/docker/bootstrap-accounts.sh \
       /accounts.json


# Bootstrap Data Example:
   docker run \
     -e "SCITRAN_RUNTIME_HOST=scitran-core" \
     -e "SCITRAN_RUNTIME_PORT=8080" \
     -e "SCITRAN_RUNTIME_PROTOCOL=http" \
     -e "SCITRAN_CORE_DRONE_SECRET=change-me" \
     -e "PRE_RUNAS_CMD=/var/scitran/code/api/docker/bootstrap-data.sh" \
     --link scitran-core \
     --volumes-from scitran-core \
     --rm \
     scitran-core \
       echo "Data bootstrap complete."
```


## NewRelic
To enable NewRelic APM reporting, create the docker container with the
environment variable "SCITRAN_CORE_NEWRELIC=/var/scitran/config/newrelic.ini"
and the necessary environment variables documented here: https://docs.newrelic.com/docs/agents/python-agent/installation-configuration/python-agent-configuration

Example:
```
docker run \
  --name scitran-core \
  -e "SCITRAN_PERSISTENT_DB_URI=mongodb://some-mongo:27017/scitran" \
  -e "SCITRAN_CORE_INSECURE=true" \
  -e "SCITRAN_CORE_NEWRELIC=/var/scitran/config/newrelic.ini" \
  -e "NEW_RELIC_LICENSE_KEY=<your_key_here>" \
  -e "NEW_RELIC_APP_NAME=new-scitran-core-app-name"
  -e "NEW_RELIC_MONITOR_MODE=true"
  -v $(pwd)/persistent/data:/var/scitran/data \
  -v $(pwd):/var/scitran/code/api \
  --link some-mongo \
  -p 0.0.0.0:8080:8080 \
  scitran-core \
    uwsgi \
      --ini /var/scitran/config/uwsgi-config.ini \
      --http 0.0.0.0:8080 \
      --http-keepalive \
      --python-autoreload 1
```
