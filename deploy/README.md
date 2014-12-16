Using the Script
================

This folder contains a script, `sampleclean-ec2`, for launching SampleClean
instances on Amazon EC2.

The script calls the spark-ec2 script (documented at
http://spark.apache.org/docs/latest/ec2-scripts.html), and can be called as
either:

```shell
./sampleclean-ec2 CREDENTIALS_DIRECTORY SPARK-EC2-ARG [SPARK-EC2-ARG ...]
```

or:

```shell
./sampleclean-ec2 SPARK-EC2-ARG [SPARK-EC2-ARG ...]
```

In the latter case, `CREDENTIALS_DIRECTORY` will be set to the value of the
environment variable `$AWS_CREDENTIALS_DIR` if it exists, or the default value
`$HOME/.ssh/aws/sampleclean`.

Either way, `CREDENTIALS_DIRECTORY` must be a path to a directory containing AWS
credentials needed for using AWS and EC2:

* A `*.csv` file containing IAM credentials for accessing AWS on your behalf.
* A `KEYNAME.pem` file containing the EC2 keypair corresponding to `KEYNAME`.
* (optional) The `sampleclean1_eecs_berkeley_edu_chained.cer` file containing
  the chained sampleclean*.eecs.berkeley.edu ssl certificates.
* (optional) The `sampleclean1.eecs.berkeley.edu-san.key` file containing the
  private key for the ssl certificate.

For help with `SPARK-EC2-ARGs`, run `./sampleclean-ec2 --help`.


Launching a cluster
===================

For example, to launch a cluster named sampleclean with 8 slaves, then run the
crowd server:
```shell
# Alternatively, use a pre-saved ami with --master-ami AMI_ID
./sampleclean-ec2 ~/.ssh/aws/sampleclean/ -s 8 -t m1.large launch sampleclean
# ... lots of output ...
./sampleclean-ec2 ~/.ssh/aws/sampleclean/ get-master sampleclean
# ... get the master's hostname ...
./sampleclean-ec2 ~/.ssh/aws/sampleclean/ login sampleclean
> workon sampleclean
> cd $PROJECT_HOME
> ./run.sh -d
# ... and the crowd server should be available at the master's hostname on port 8000 ... #
```

Running code on the cluster
===========================

To actually get code running on a cluster you've launched, you'll need to:

* ssh into the cluster:
  `./sampleclean-ec2 ~/.ssh/aws/sampleclean/ login CLUSTER_NAME`

* Copy data to the master's hdfs instance:
  `/root/ephemeral-hdfs/bin/hadoop dfs -put LOCAL/PATH/TO/data HDFS/PATH/TO/data`

* Run the handy script in the `sampleclean-async` directory to start the crowd
  server and run the SCDriver program:
  ```shell
  cd /root/sampleclean-async
  ./run-all-ec2.sh
  ```

* Or to have more control (or to run things in debug mode):
  * Start the crowd server (see above).

  * Build and package the code:
    ```shell
    cd /root/sampleclean-async
    sbt/sbt assembly
    ```

  * Figure out the cluster's master url by navigating a browser to port 8080
    of the master machine and looking for a URL like
    `spark://ec2-##-##-##-##.compute-1.amazonaws.com:7077` (or looking at the
    contents of `/root/spark-ec2/cluster-url`.

  * Run the job:
    `/root/spark/bin/spark-submit --class <main-class> --master <master_url>
    [OTHER SPARK_SUBMIT OPTIONS ...]
    /root/sampleclean-async/target/scala-2.10/SampleClean-Spark-1.0-assembly-1.0.jar`

* Monitor the job: Spark creates a UI on port 4040 for the running application.
  You can also look at the cluster UI on port 8080 to confirm that the
  application has started. See
  http://spark.apache.org/docs/latest/monitoring.html for more details.

Destroying a Cluster
====================
The `sampleclean-ec2.sh` script can also be used to terminate a cluster. The following
command will terminate all running instances and delete security groups associated with
the cluster:
`sampleclean-ec2.sh ~/.ssh/aws/sampleclean/ --delete-groups destroy <cluster_name>`
