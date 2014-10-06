from django.conf.urls import patterns, url

from internal import views

urlpatterns = patterns('',
    url(r'^$', views.index, name='index'),
    url(r'^dummy_submit$', views.fake_submit_endpoint,
        name='fake_submit_endpoint')
)
