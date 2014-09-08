from django.conf.urls import patterns, url

from amt import views

urlpatterns = patterns('',
    url(r'^assignments/$', views.get_assignment, name='get_assignment'),
    url(r'^responses/$', views.post_response, name='post_response'),
    url(r'^hitsgen/$', views.hits_gen, name = 'hits_gen'),
    url(r'^hitsdel/$', views.hits_del, name = 'hits_del'),
)

