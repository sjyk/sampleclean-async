#!/bin/bash

# Kill old processes
./stop.sh

# Start new processes
if [ "$1" == "-d" ] # Print errors directly to console for easy debugging with -d
then
    python manage.py celeryd -l DEBUG &
    gunicorn -b 0.0.0.0:8000 -w 1 --access-logfile access-gunicorn.log --error-logfile error-gunicorn.log --log-level debug  --certfile=crowd_server/ssl/development.crt --keyfile=crowd_server/ssl/development.key crowd_server.wsgi:application
else
    python manage.py celeryd_detach
    gunicorn -D -b 0.0.0.0:8000 -w 10 --access-logfile access-gunicorn.log --error-logfile error-gunicorn.log --log-level debug  --certfile=crowd_server/ssl/development.crt --keyfile=crowd_server/ssl/development.key crowd_server.wsgi:application
fi
