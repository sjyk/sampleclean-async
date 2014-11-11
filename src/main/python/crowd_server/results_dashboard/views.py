import json
import pytz
import uuid
from datetime import datetime
from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from results_dashboard.models import Query, QueryResult, QueryGroupResult

@require_GET
def index(request):
    context = {
        'queries': Query.objects.all()
    }
    return render(request, 'results_dashboard/index.html', context)

@require_POST
@csrf_exempt
def post_result(request):
    # Extract request data
    query = request.POST.get('querystring')
    query_id = request.POST.get('query_id')
    pipeline_id = request.POST.get('pipeline_id')
    result_col_name = request.POST.get('result_col_name')
    grouped = request.POST.get('grouped', False) == 'true'
    results = request.POST.get('results')

    # Create the query object, or fetch it if it already exists.
    query, was_created = Query.objects.get_or_create(
        query_id=query_id,
        defaults={'querystring': query,
                  'is_grouped': grouped,
                  'pipeline_id': pipeline_id})

    if was_created and query.is_grouped == None:
        query.is_grouped = grouped
        query.save()

    # Create the query result object.
    result = QueryResult.objects.create(
        query=query,
        result_col_name=result_col_name,
        ungrouped_result = None if grouped else float(results))

    # Save the group results individually if the query was grouped.
    if grouped:
        groups = json.loads(results)
        for group_name, group_val in groups.iteritems():
            result_group = QueryGroupResult.objects.create(
                query_result=result,
                group_name=group_name,
                value=group_val)

    return HttpResponse(json.dumps({'status': 'ok'}),
                        content_type='application/json')

@require_GET
def get_new_queries(request):
    already_seen = json.loads(request.GET.get('seen', "[]"))
    results = (Query.objects
               .exclude(query_id__in=already_seen)
               .values('query_id', 'querystring', 'registered_at'))
    return HttpResponse(json.dumps(list(results),
                                   default=lambda dt: dt.isoformat()),
                        content_type='application/json')

@require_GET
def get_result(request, query_id):
    as_of = request.GET.get('as_of')
    if as_of:
        # Date format is ISO (example: 2014-10-15T13:19:37.273Z)
        as_of = datetime.strptime(
            as_of, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)

    try:
        results = (QueryResult.objects
                   .filter(query__query_id=query_id)
                   .order_by('-posted_at'))

        # If there are no new results, return a status of 'old'
        if as_of and results[0].posted_at < as_of:
            return HttpResponse(json.dumps({'results':'old'}),
                                content_type="application/json"
)
        result = results[0]
    except IndexError: # No query results at all for this query yet.
        return HttpResponse(json.dumps({'results': 'none'}),
                            content_type="application/json")

    if result.ungrouped_result:
        result_value = result.ungrouped_result
    else:
        result_value = [ {"name": g.group_name, "value": g.value}
                         for g in result.groups.order_by("-value") ]

    result_json = json.dumps({'results': result_value,
                              'result_col_name': result.result_col_name,
                              'posted_at': result.posted_at},
                             default=lambda dt: dt.isoformat())
    return HttpResponse(result_json, content_type="application/json")

@require_GET
def purge_queries(request):
    Query.objects.all().delete()
    return HttpResponse("{}", content_type="application/json")

@require_POST
@csrf_exempt
def delete_query(request, query_id):
    try:
        query = Query.objects.get(query_id=query_id).delete()
        result_json = json.dumps({'status': 'success'})
    except Query.DoesNotExist:
        result_json = json.dump({'status': 'invalid'})

    return HttpResponse(result_json, content_type="application/json")
