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

For help with `SPARK-EC2-ARGs`, run `./sampleclean-ec2 --help`.

For example, to launch a cluster with 8 slaves, then run the crowd server:
```shell
# Alternatively, use a pre-saved ami with --master-ami AMI_ID
./sampleclean-ec2 ~/.ssh/aws/sampleclean/ -s 8 -t x1.large launch sampleclean
# ... lots of output ...
./sampleclean-ec2 ~/.ssh/aws/sampleclean/ get-master sampleclean
# ... get the master's hostname ...
./sampleclean-ec2 ~/.ssh/aws/sampleclean/ login sampleclean
> workon sampleclean
> cd $PROJECT_HOME
> ./run.sh -d
# ... and the crowd server should be available at the master's hostname on port 8000 ... #
```