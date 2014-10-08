Stress Testing for the Crowd Server
===================================

This module uses the [multi-mechanize](http://testutils.org/multi-mechanize/)
package to provide load testing of the crowd server.

Usage
-----

**NOTE:** This may be obvious, but you don't want to stress test the server from
the machine on which it is running, as the test threads will interfere with the
execution of the server threads.

On the server machine:

* Set up and run the crowd server in a production environment (you might want to
  modify `crowd_server/run.sh` to configure gunicorn).
* Make sure there are enough tasks in the database to handle the entire stress
  test. Create additional tasks using `crowd_server/post.py` (run
  `crowd_server/post.py --help` for details on using that script).

On the test machine:

* Modify `crowd_server/stress_test/config.cfg` to test according to your
  preferred settings. See http://testutils.org/multi-mechanize/configfile.html
  for options. You'll probably only want to alter `global.run_time`,
  `global.ramp_up`, and `user_group-1.threads`.
* Modify `crowd_server/stress_test/test_scripts/crowd_worker.py`, setting
  SITE_URL to the location of the server machine.
* Run the stress test from the `crowd_server/` directory with
  `multimech-run stress_test`
* Look at the results by opening
  `crowd_server/stress_test/results/results_DATETIME/results.html` (i.e., in any
  browser open
  `file:///PATH/TO/crowd_server/stress_test/results/results_DATETIME/results.html`).
