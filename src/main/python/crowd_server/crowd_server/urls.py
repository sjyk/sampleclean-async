from django.conf.urls import patterns, include, url

#from django.contrib import admin
#admin.autodiscover()

urlpatterns = patterns('',
    url(r'^crowds/', include('basecrowd.urls', namespace="basecrowd")),
    url(r'^crowds/internal/', include('internal.urls', namespace="internal")),
    url(r'^dashboard/', include('results_dashboard.urls',
                                namespace="dashboard")),
)
