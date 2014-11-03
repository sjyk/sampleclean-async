#!/bin/bash

# Kill old processes
./stop.sh

# Process options

# Print errors directly to console for easy debugging with -d
if [ "$1" == "-d" ] || [ "$2" == "-d" ]
then
    export DEVELOP=1
    python manage.py celeryd -l DEBUG &
else
    export DEVELOP=0
    python manage.py celeryd_detach
fi

# Enable SSL
if [ "$1" == "-s" ] || [ "$2" == "-s" ]
then
    export SSL=1
else
    export SSL=0
fi

# Run the application
python manage.py collectstatic --noinput
gunicorn -c gunicorn_config.py crowd_server.wsgi:application
