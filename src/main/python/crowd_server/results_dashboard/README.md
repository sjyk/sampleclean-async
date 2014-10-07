SampleClean Results Dashboard
=============================

This Django app provides a graphical interface for viewing approximate query
results and their changes as the underlying data is cleaned.

Setup
-----

* Add the app (`results_dashboard`) to the `INSTALLED_APPS` setting of your project's 
  `setttings.py` file.

* Set up the dashboard urls in your top-level `urls.py` file:

    ```python
    urlpatterns += url(r'^dashboard/', include('results_dashboard.urls',
                                               namespace='dashboard'))
    ```

Dashboard API
------------

* As a user, simply navigate your browser to the `/dashboard/` url to view
  results.

* To register new results with the dashboard, a SampleClean installation must
  call `POST /dashboard/results/`, with post data as x-www-form-urlencoded keys,
  including:

  * `querystring`: The string for the SQL query that was issued.
  * `query_id`: A unique identifier for the query.
  * `pipeline_id`: A unique identifier for the data cleaning pipeline this query
    is scoped to.
  * `result_col_name`: A human-readable string for the aggregated query result
    column.
  * `grouped`: Either `'true'` or `'false'`: whether this query produces
    multiple output groups.
  * `results`: If this is a grouped query, a JSON dictionary of
    `group_name: group_value` pairs. If it is ungrouped, just a single float
    value for the query result. Examples (for 'world population' queries):

    ```python
    grouped_result = { "USA": 300000000,
                       "China": 1400000000,
                       "India": 1300000000 }

    ungrouped_result = 7100000000.0
    ````

Using the Dashboard with Sample Data
------------------------------------
The script `results_dashboard/post.py` enables users to create sample query results for display
in the dashboard. From the `results_dashboard` directory, run `post.py --help` for option when
using the script. For example, to create a grouped query with 4 groups, run:

```shell
python post.py -g 4
```

and to post new results for an existing query with id `abcdef`, you could run:

```shell
python post.py -q abcdef -g 4
```


  
