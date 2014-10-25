Crowd Server
============

This package provides a django server for running data cleaning tasks on various
crowd platforms.

Thing to do to get up and running:

* install postgres, and create a user and a DB for this project :
  1. Install postgres and libpq-dev:

        ```shell
        $ sudo apt-get install postgresql
        $ sudo apt-get install libpq-dev
        ```

  2. Set up access control:
     
     a) Find the pg_hba.conf file:

        ```shell
        $ sudo -u postgres psql
        > show hba_file;
        > \q
        ```

     b) edit the pg_hba.conf file, and change the line starting with "local" to "local" "all" "all" "trust"
  3. Restart postgres:

        ```shell
	$ sudo /etc/init.d/postgresql restart
	```

  4. Create the Django DB user (right now it's 'sampleclean'):

        ```shell
        $ sudo -su postgres
        $ createuser --superuser sampleclean
        $ exit
        $ psql -u sampleclean
        >\password\
        >(Enter 'sampleclean')
        >\q
        ```

  5. Create a database(right now it's 'sampleclean'):

        ```shell
        $ createdb -O sampleclean -U sampleclean sampleclean
        ```
        
* install RabbitMQ and start its server:

```shell
$ sudo apt-get install rabbitmq-server
$ rabbitmq-server
```
        
* create a virtualenv for python dev (I like
  http://virtualenvwrapper.readthedocs.org/en/latest/).

* Install the python requirements:

```shell
$ pip install -r requirements.txt
```

* Create your own private settings file:

```shell
$ cp crowd_server/private_settings.py.default crowd_server/private_settings.py
```
        
* Sign up for a mechanical turk account, and put the credentials in
  `private_settings.py`. **NEVER CHECK THIS FILE INTO THE REPO.**

* Set up the database:

```shell
$ ./reset_db.sh
```

* Run the server:

```shell
$ ./run.sh    # Daemon mode
$ ./run.sh -d # Debug mode
```

* Make sure it works:
  1. Run the sample script which could create a couple of sample HITs on AMT:

        ```shell
        python post.py -c amt -t TASK_TYPES   # You pick 1 or more of 'sa', 'er', 'ft', described below.
        ```

  2. Log into the AMT management interface (https://requestersandbox.mturk.com/mturk/manageHITs) and 
     verify that you have justcreated the sample HITs. Then log in as a worker
     (https://workersandbox.mturk.com/mturk/searchbar) and verify that you can accept the HIT and that
     it displays correctly in AMT's iframe.
* Stop the server:

```shell
$ CTRL-C    # if you ran in debug mode
$ ./stop.sh # to clean up stray processes regardless.
```

Web Service User-Facing APIs
============================
* Create tasks for a group of points.

  - URL :

    **POST /crowds/CROWD_NAME/tasks/**
  - Available Crowds: see below.
  - Data : There is only one single field, 'data', which maps to a json dictionary with keys:
    - **configuration** : settings for this group of points, a json dictionary with keys:
      - **task_type** : The type of this task, e.g, 'sa' for sentiment analysis, 'er' for entity resolution, 
        'ft' for filtering tasks.
      - **task_batch_size** : The maximum number of points to show a crowd worker in a single task (integer).
      - **num_assignments**: The maximum number of crowd votes to acquire for each task.
      - **callback_url**: The URL to POST results to (see below).
      - **CROWD_NAME**: A json dictionary with configuration specific to the crowd running the tasks. See 
        below for the settings for specific crowds.
    - **group_id**: A unique identifier for this group of points.
    - **group_context**: The context that is shared among all the points in the group.

      1. 'sa'(Sentiment Analysis). The group_context is an empty json dictionary, i.e, `{}`.

      2. 'er'(Entity Resolution). The group_context consists of the shared schema for each pair of record. 
         For example, the following:

        ```json
        {"fields": ["price", "location"]}
        ```

      3. 'ft'(Filtering). The group_context consists of the shared schema of the records in each question. 
         It is similar to the group_context of an entity resolution task. For example :

        ```json
        {"fields": ["Conference", "First Author"]}
        ```

    - **content** : Data necessary to render the crowd interface for the selected task type. Available types are:

      1. 'sa' (Sentiment Analysis). Content should be a json dictionary mapping unique ids to tweet strings, 
         e.g, the following:

        ```json
        {
            "tweet1": "Arsenal won the 4th again!",
            "tweet2": "Theo Walcott broke the ligament in his knee last season.",
            "tweet3": "Lebron James went back to Cavaliers after he found his teammates in Heats no longer powerful."
        }
        ```

      2. 'er' (Entity Resolution). Content should consist of pairs of records for entity resolution, specified as
         a json dictionary with pairs of records mapped to unique ids, e.g, the following:

        ```json
        {
            "pair1": [["5","LA"], ["6","Berkeley"]],
            "pair2": [["80", "London"], ["80.0", "Londyn"]]
        }
        ```

      3. 'ft'(Filtering). The content for each point should be a json dictionary consisting of a title and the values
         for each attribute, e.g, the following:

        ```json
        {
            "ft1": {"title" : "Is this a paper of Michael Franklin?", 
                    "record" : ["icde", "Michael Franklin"]}
        }
        ```

  - Examples :

        ```json
        data=
        {
            "configuration": {
                "task_type": "sa",
                "task_batch_size": 2,
                "num_assignments": 1,
                "callback_url": "google.com"
            },
            "group_id": "Dan1",
            "group_context": {},
            "content": {
                "tweet1": "aa", 
                "tweet2": "bb"
            }
         }

        data=
        {
            "configuration": {
                "task_type": "er",
                "task_batch_size": 1,
                "num_assignments": 1,
                "callback_url": "google.com"
            },
            "group_id": "haha",
            "group_context": {
                "fields": ["age","name"]
            },
            "content": {
                "pair1": [["22","James"],["21","Wenbo"]]
            }
         }

        data=
        {
            "configuration": {
                "task_type": "ft",
                "task_batch_size": 1,
                "num_assignments": 1,
                "callback_url": "google.com"
            },
            "group_id": "haha",
            "group_context": {
                "fields": ["Conference","First Author"]
            },
            "content": {
                "ft1": {
                    "title": "Decide whether it is one of Michael Franklin's Paper.",
                    "record": ["icde", "Michael Franklin"]
                },
                "ft2": {
                    "title": "Decide whether it is one of Jiannan Wang's Paper.", 
                    "record" : ["nsdi", "Zhao Zhang"]
                }
            }
        }
        ```

  - The direct response for this request is a simple json dictionary, one of the following:
        
        ```json
        {"status": "ok"}
        {"status": "wrong"}
        ```

    The latter means that the format is incorrect, it may be attributed to the wrong format of the content field
    or omissions of other important fields.

* Send the results to the callback URL (**POST** method):

  When a point gets enough votes from the crowd, the EM/MV answer will be sent back to the call back url.

  - The results that are sent back consist of a single field, 'data', which maps to a json dictionary :
    - **group_id** : a string specifying the group that this point belongs to
    - **answers**: a list of 1 or more responses for points in the group, each of which contains:
      - **identifier** : the identifier of the point given when the group was created.
      - **value** : the answer value. Values should depend on the type of the crowd task. Available types are:
        - 'sa': Value should be an integer in [1,5] corresponding to:
          - **1**: Tweet is very negative
          - **2**: Tweet is somewhat negative
          - **3**: Tweet is neutral
          - **4**: Tweet is somewhat positive
          - **5**: Tweet is very positive
        - 'er': Value should be either 0.0 or 1.0, indicating 'records do not match' or 'records match', respectively.
        - 'ft': Value should be either 0.0 or 1.0, indicating 'the answer to the question is NO' or 'the answer to the
          question is YES' respectively.

  - Examples:

        ```json
        data=
        {
            "group_id": "Dan1",
            "answers": [
                {
                    "identifier": "tweet1",
                    "value": 1
                },
                {
                    "identifier": "tweet2",
                    "value": 3
                }
            ]
        }

        data=
        {
            "group_id": "haha",
            "answers": [
                {
                    "identifier": "pair1",
                    "value": 0.0
                }
            ]
        }
        ```


* Delete all the tasks that are stored in the database for a given crowd:
  - URL :
    **GET /crowds/CROWD_NAME/purge_tasks/**

Crowd platform-facing APIs
==========================
* Get a new assignment to display to a worker:

  - URL: **GET /crowds/CROWD_NAME/assignments/**
  - Data: custom for each crowd (see each crowd's `assignment_context`, below).
  - Returns: HTML for the interface that a crowd worker can use to complete the
    task.

* Post a crowd worker's response to the server:

  - URL: **POST /crowds/CROWD_NAME/responses/**
  - Data: custom for each crowd (see each crowd's `response_context`, below).
    Data should be x-www-form-urlencoded and must contain at least the key:

    - `answers`: the results of the task. Should be a JSON string containing
      key-value pairs, where each key is the unique identifier for a single
      point, and each value is that point's label assigned by the crowd worker.

  - Returns: HTTP status 200 OK on success.

Add your own task
=================
You can add your own type of crowd task by following the following steps :

1.  Define the API for creating such task, including:
   - Defining the type name of this task, which is a string
   - Defining the group_context for this task, which is a **json object** (json array or json dictionary)
   - Defining the content for each point, which is a **json object**. Note that the 'content' field in the API should
     be a json array mapping ids of points to their contents :

        ```json
        {'id1': content1, 'id2': content2}
        ```

2. Find  `/basecrowd/templates/basecrowd/TYPE.html`, copy the file to `TYPENAME.html` where `TYPENAME` is 
   the type name of this task which  was defined by you in step 1.

3. Open this file, follow the comments in the file to create the template for this task.

Available Crowds
================
The following crowds can be used to process tasks with the crowd server:

Amazon Mechanical Turk (https://www.mturk.com)
----------------------------------------------
- CROWD_NAME: 'amt'
- special configuration keys: sandbox : a 0/1 number which indicates using real amt/sandbox
- `assignment_context`: urlencoded key-value pairs that must include the
  following keys:

  - `hitId`: the AMT HIT id for the requested task.
  - `worker_id`: the id of the Turker who is assigned to the task, not present
    if the worker is previewing the task rather than working on it.
  - `turkSubmitTo`: the url which must be posted to in order for AMT to register
    that the task has been completed.
  - `assignmentId`: the unique id assigning this worker to this HIT, given the
    special value `ASSIGNMENT_ID_NOT_AVAILABLE` if the worker is previewing the
    task rather than working on it.

- `response_context`: x-www-form-urlencoded key-value pairs that must include
  the following keys:

  - `answers`: the results of the task (see API, above).
  - `HITId`: the AMT HIT id for the task.
  - `workerId`: the id of the Turker who is responding to the task.
  - `assignmentId`: the unique id assigning this worker to this HIT.

Internal Crowd
--------------
- CROWD_NAME: 'internal'
- special configuration keys: None
- `assignment_context`: urlencoded key-value pairs that must include the
  following keys:

  - `worker_id`: the id of the worker assigned to the task.
  - `task_type`: the type of task to assign to the worker.

- `response_context`: x-www-form-urlencoded key-value pairs that must include
  the following keys:

  - `answers`: the results of the task (see API, above).
  - `task_id`: the unique id for the task.
  - `worker_id`: the unique id of the worker responding to the task.
  - `assignment_id`: the unique id assigning this worker to this task.

Interfacing with a new crowd
============================
The crowd server is designed to be easily extensible to send tasks to other
crowd systems. Each crowd is implemented as a Django app that can re-use models,
views, and templates from our generic 'basecrowd' implementation. To add support
for a new crowd to the server, you must:

1. Assign your crowd a CROWD_NAME that can be referenced from within URLs.

2. Create your django app with `python manage.py startapp CROWD_NAME`.

3. In `CROWD_NAME/models.py`, define your models. A crowd must have at a minimum 
   four models: one for Tasks, one for Task Groups, one for Workers, and one for
   worker Responses. Luckily, we provide abstract implementations of all of
   these in `basecrowd/models.py`, so simply subclass
   `models.AbstractCrowdTask`, `models.AbstractCrowdTaskGroup`,
   `models.AbstractCrowdWorker`, and `models.AbstractCrowdWorkerResponse` and
   you're good to go. You can add custom fields to your subclasses if you'd
   like, but it's probably not necessary.

4. Create a new file `CROWD_NAME/interface.py`. This will be the bulk of your
   work. In the file, create a subclass of `basecrowd.interface.CrowdInterface`,
   and implement any methods necessary to support your crowd. Feel free to look
   at `basecrowd/interface.py` for the full list of available methods, but
   you're likely to need only a few, including:
   * `create_task`: create a new task on your crowd platform.
   * `delete_tasks`: delete one or more tasks on your crowd platform.
   * `get_assignment_context`: when your crowd platform requests a task
     interface, provide enough context to render your interface template
     (described more below).
   * `get_response_context`: when a crowd worker submits data, extract it
     from the request and put it in a format that can be saved to your models.
   
   Finally, create an instance of your interface as a module-level variable,
   e.g.:

    ```python
    MYCROWD_INTERFACE = MyCrowdInterface(CROWD_NAME)
    ```
       
5. In `CROWD_NAME/__init__.py`, register your new interface with the server:

    ```python
    from interface import MYCROWD_INTERFACE
    from models import *
    from basecrowd.interface import CrowdRegistry
    
    CrowdRegistry.register_crowd(
        MYCROWD_INTERFACE,
        task_model=MyCrowdTaskSubclass,
        group_model=MyTaskGroupSubclass,
        worker_model=MyWorkerSubclass,
        response_model=MyResponseSubclass)
    ```

6. Now all we need are templates to render the task interfaces to the crowd.
   Again, we provide a base template that should do almost all of the heavy
   lifting. Create your app's template directories and a new template in
   `CROWD_NAME/templates/CROWD_NAME/base.html`. Inherit from our base template
   with `{% extends "basecrowd/base.html" %}`, and implement as many blocks in
   that template as you'd like. Our base template (located in
   `basecrowd/templates/basecrowd/base.html`) contains many inheritable blocks,
   but you probably need to implement only one of them:
   `{% block get_submit_context_func %}`. This block should contain a javascript
   function definition, `get_submit_context(urlParamStrings)` that takes a list
   of URL parameters (unsplit in the form 'key=value'), and should return a JSON
   object containing any context that needs to be submitted to the server with
   the data (e.g. the task, worker, and assignment ids relevant to this
   interface). See our implementation in `amt/templates/amt/base.html` for
   details.

7. The interfaces for individual task types (sentiment analysis, deduplication,
   etc.) should just fit into your `base.html` template with no additional work.
   If you completely customized `base.html`, however, you might need custom
   templates for the task types as well. To create them, simply follow the steps
   described above for creating a new task type, but place your template in your
   crowd's template directory (e.g., `CROWD_NAME/templates/CROWD_NAME/sa.html`).

8. And that's it! Now you should be able to use the APIs described above to
   create and complete tasks on your new crowd platform. If you run into
   trouble, take a look at our implementation of the Amazon Mechanical Turk
   crowd (in `amt/`) for inspiration.
