from django.conf.urls import patterns, include, url

from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'amt_server.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

    url(r'^amt/', include('amt.urls', namespace="amt")),
    url(r'^admin/', include(admin.site.urls)),
)
