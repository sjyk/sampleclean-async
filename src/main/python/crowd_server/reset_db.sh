(python manage.py sqlclear basecrowd amt internal | sed 's/";/" CASCADE;/' | python manage.py dbshell) && python manage.py syncdb
