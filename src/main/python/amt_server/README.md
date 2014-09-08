AMT Server
==========

This package provides a django server for running data cleaning tasks on AMT

Thing to do to get up and running:

* install postgres, and create a user and a DB for this project :

	1) Install postgres and libpq-dev:
		
		$ sudo apt-get install postgresql
		$ sudo apt-get install libpq-dev

	2) Set up access control:
		a) Find the pg_hba.conf file:
		
		    $ sudo -u postgres psql
				> show hba_file;
	    		> \q
		
		b) edit the pg_hba.conf file, and change the line starting with "local" to "local" "all" "all" "trust"
		
	3) Restart postgres:
		$ sudo /etc/init.d/postgresql restart

	4) Create the Django DB user (right now it's 'sampleclean'):

		$ sudo -su postgres
		$ createuser --superuser sampleclean
		$ exit
		$ psql -u sampleclean
			>\password
			>(Enter 'sampleclean')
			>\q
		
	5) Create a database(right now it's 'sampleclean'):

		$ createdb -O sampleclean -U sampleclean sampleclean


* install RabbitMQ and start its server:

          $ sudo apt-get install rabbitmq-server
		  
		  $ rabbitmq-server
		  
* create a virtualenv for python dev (I like
  http://virtualenvwrapper.readthedocs.org/en/latest/).

* Install the python requirements:

          $ pip install -r requirements.txt

* Create your own private settings file:

          $ cp amt_server/private_settings.py.default amt_server/private_settings.py

* Sign up for a mechanical turk account, and put the credentials in
  `private_settings.py`. **NEVER CHECK THIS FILE INTO THE REPO.**

* Set up the database:

          $ ./reset_db.sh

* Run the server:

          $ ./run.sh

* Make sure it works: 

  run the sample script which could create a couple of sample HITs :

        python post.py
        
  Log into the AMT management interface (https://requestersandbox.mturk.com/mturk/manageHITs) and verify that you have just created the sample HITs. Then log in as a worker(https://workersandbox.mturk.com/mturk/searchbar) and verify that you can
  accept the HIT and that it displays correctly in AMT's iframe.





Web Service APIs
=============
* Create HITs for a group of points(**POST** method). 

  - URL : 
    
    **/amt/hitsgen/**
  - Data : There is only one single field, 'data', which maps to a json dictionary with keys:
    - **configuration** : settings for this group of points, a json dictionary with keys:
      - **type** : The type of this hit, e.g, 'sa' for sentiment analysis, 'er' for entity resolution
      - **hit_batch_size** : The maximum number of points to show a crowd worker in a single HIT (integer).
      - **num_assignments**: The maximum number of crowd votes to acquire for each HIT.
      - **callback_url**: The URL to POST results to (see below).
    - **group_id**: A unique identifier for this group of points. 
    - **group_context**: The context that is shared among all the points in the group. 
      
      1. 'sa'(Sentiment Analysis). The group_context is an empty json dictionary, i.e, {}.

      2. 'er(Entity Resolution). The group_context consists of the shared schema for each pair of record. For example, the following:
      
             {"fields":["price","location"]}

    - **content** : Data necessary to render the crowd interface for the selected task type. Available types are:
      
      1. 'sa' (Sentiment Analysis). Content should be a json dictionary mapping unique ids to tweet strings, e.g, the following:
          
             {
	          "tweet1": "Arsenal won the 4th again!", 
	          "tweet2": "Theo Walcott broke the ligament in his knee last season.",
	          "tweet3": "Lebron James went back to Cavaliers after he found his teammates in Heats no longer powerful."
	         }
         
      2. 'er' (Entity Resolution). Content should consist of pairs of records for entity resolution, specified as a json dictionary with pairs of records mapped to unique ids, e.g, the following:

	         {
	          "pair1": [["5","LA"], ["6","Berkeley"]], 
	          "pair2": [["80", "London"], ["80.0", "Londyn"]]
	         }

  - Examples : 
  
         >data=
         {
          "configuration":{"type":"sa","hit_batch_size":2,"num_assignments":1,"callback_url":"google.com"},
          "group_id":"Dan1",
          "group_context":{},
          "content":{"tweet1":"aa", "tweet2": "bb"}
         }

         >data=
         {
          "configuration":{"type":"er","hit_batch_size":1,"num_assignments":1,"callback_url":"google.com"},
          "group_id":"haha",
          "group_context":{"fields":["age","name"]},
          "content":{"pair1": [["22","James"],["21","Wenbo"]]}
         }
	
  - The direct response for this request is a simple json dictionary :
     
    > {"status":"ok"}
    
    means the format is correct.
     
    > {"status":"wrong"}
    
    means the format is incorrect, it may be attributed to the wrong format of the content field or omissions of other important fields.
  
  
* Send the results to the callback URL(**POST** method):
  
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
  
  - Examples:
    > data={"group_id":"Dan1", "answers":[{"identifier":"tweet1","value":1}, {"identifier":"tweet2","value":3}]}

    > data={"group_id":"haha","answers":[{"identifier":"pair1","value":0.0}]}

* Disable all the HITs that are stored in the database(**GET** method):
  - URL :
    **/amt/hitsdel/**