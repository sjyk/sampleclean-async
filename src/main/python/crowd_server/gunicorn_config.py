import multiprocessing
import os

# Are we running in development or production mode?
debug = os.environ.get('DEVELOP', False) == "1"

# Port to run the service on
bind = "0.0.0.0:8000"

# Number of worker processes
workers = multiprocessing.cpu_count() * 2 + 1

# Run in the background as a daemon
daemon = not debug

# Logging
accesslog = "access-gunicorn.log"
errorlog = "error-gunicorn.log"
loglevel = "debug"

# SSL
use_ssl = os.environ.get('SSL', False) == "1"
if use_ssl:
    keyfile = "crowd_server/ssl/development.key"
    certfile = "crowd_server/ssl/development.crt"
