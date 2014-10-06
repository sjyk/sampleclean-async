from django.conf.urls import patterns, url

from basecrowd import views

urlpatterns = patterns('',
    url(r'^(\w+)/assignments/$', views.get_assignment, name='get_assignment'),
    url(r'^(\w+)/responses/$', views.post_response, name='post_response'),
    url(r'^(\w+)/tasks/$', views.create_task_group, name = 'create_tasks'),
    url(r'^(\w+)/purge_tasks/$', views.purge_tasks, name = 'purge_tasks'),
)
