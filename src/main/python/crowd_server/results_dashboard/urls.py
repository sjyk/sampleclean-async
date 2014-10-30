from django.conf.urls import patterns, url

from results_dashboard import views

urlpatterns = patterns('',
    url(r'^$', views.index, name='index'),
    url(r'^results/$', views.post_result, name='post_result'),
    url(r'^results/(?P<query_id>[\w-]+)$', views.get_result, name='get_result'),
    url(r'^purge_queries$', views.purge_queries, name='purge_queries'),
    url(r'^queries/$', views.get_new_queries, name='get_new_queries'),
    url(r'^queries/(?P<query_id>[\w-]+)/delete$', views.delete_query, name='delete_query')
)
