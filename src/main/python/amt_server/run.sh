# python manage.py runsslserver
python manage.py celeryd_detach
gunicorn -D -b 0.0.0.0:8000 -w 1 --access-logfile access-gunicorn.log --error-logfile error-gunicorn.log --log-level debug  --certfile=amt_server/ssl/development.crt --keyfile=amt_server/ssl/development.key amt_server.wsgi:application
