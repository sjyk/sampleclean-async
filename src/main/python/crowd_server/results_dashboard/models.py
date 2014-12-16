from django.db import models

class Query(models.Model):
    query_id = models.CharField(primary_key=True, max_length=40)
    querystring = models.TextField()
    is_grouped = models.NullBooleanField(null=True)
    pipeline_id = models.CharField(max_length=40)
    registered_at = models.DateTimeField(auto_now_add=True)

class QueryResult(models.Model):
    query = models.ForeignKey(Query, related_name='results')
    result_col_name = models.CharField(max_length=200)
    ungrouped_result = models.FloatField(null=True)
    posted_at = models.DateTimeField(auto_now_add=True)

class QueryGroupResult(models.Model):
    query_result = models.ForeignKey(QueryResult, related_name='groups')
    group_name = models.CharField(max_length=200)
    value = models.FloatField()
