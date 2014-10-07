"""
Django settings for crowd_server project.

For more information on this file, see
https://docs.djangoproject.com/en/1.6/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.6/ref/settings/
"""

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
import os
import djcelery

# Celery Configuration
djcelery.setup_loader()
BROKER_URL = "amqp://guest:guest@localhost:5672//"

BASE_DIR = os.path.dirname(os.path.dirname(__file__))


# Settings for the AMT app
AMT_SANDBOX = True # run on the sandbox, or on the real deal?
AMT_SANDBOX_HOST = 'mechanicalturk.sandbox.amazonaws.com'
AMT_SANDBOX_WORKER_SUBMIT = 'https://workersandbox.mturk.com/mturk/externalSubmit'
AMT_HOST = 'mechanicalturk.amazonaws.com'
POST_BACK_AMT = 'https://www.mturk.com/mturk/externalSubmit'
POST_BACK_AMT_SANDBOX = 'https://workersandbox.mturk.com/mturk/externalSubmit'

HAVE_PUBLIC_IP = False # run on a server with public ip or just run locally?

AMT_DEFAULT_HIT_OPTIONS = { # See documentation in amt/connection.py:create_hit
    'title': 'Generic HIT',
    'description': 'This is a HIT to run on AMT.',
    'reward': 0.03,
    'duration': 60,
    'num_responses': 3,
    'frame_height': 800,
    'use_https': True,
}

# AMT Settings that MUST be defined in private_settings.py:
#   AMT_ACCESS_KEY
#   AMT_SECRET_KEY

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.6/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = '009&#y4^ix8uzt5wt^5d%%+2xp@ym&hfv%%y*xk4obcro-1@r6'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

TEMPLATE_DEBUG = True

ALLOWED_HOSTS = []

APPEND_SLASH = True

# Application definition

INSTALLED_APPS = (
    #'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.sites',
    'sslserver',
    'djcelery',
    'basecrowd',
    'amt',
    'internal',
    'results_dashboard',
)

MIDDLEWARE_CLASSES = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
)

ROOT_URLCONF = 'crowd_server.urls'

WSGI_APPLICATION = 'crowd_server.wsgi.application'


# Database
# https://docs.djangoproject.com/en/1.6/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'sampleclean',
        'USER': 'sampleclean',
        'PASSWORD': 'sampleclean',
        'HOST': '127.0.0.1',
        'PORT': '5432',
    }
}

# Internationalization
# https://docs.djangoproject.com/en/1.6/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.6/howto/static-files/

STATIC_URL = '/static/'

# Sites
# https://docs.djangoproject.com/en/1.6/ref/contrib/sites/
SITE_ID = 1

# Import private settings, overriding settings in this file
try:
    from private_settings import *
except ImportError:
    pass
