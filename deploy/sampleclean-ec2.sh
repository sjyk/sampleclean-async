#! /bin/bash

# Use the first argument to set up our AWS environment variables
source ./setup_env.sh $1
dir_was_passed=$?

# Pass the remaining arguments to spark ec2, along with the credentials we just
# loaded.
if [ "$dir_was_passed" -ne 0 ]
then
    args=${@:2}
else
    args=${@:1}
fi
./spark-ec2 -i $AWS_EC2_KEY_FILE -k $AWS_EC2_KEY_NAME --ssl-cert-file $SSL_CERT_FILE --ssl-key-file $SSL_KEY_FILE $args
