(python manage.py sqlclear basecrowd amt internal results_dashboard | sed 's/";/" CASCADE;/' | python manage.py dbshell) && python manage.py syncdb --noinput
