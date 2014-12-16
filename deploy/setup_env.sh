#! /bin/bash

# Require 1 argument.
hardcoded_credentials_dir="$HOME/.ssh/aws/sampleclean"
if [ "$#" -gt "1" ]
then
    echo "USAGE: $0 [CREDENTIALS_DIRECTORY]"
    exit
fi

no_cred_dir=""
ret=1
if [ "$#" -eq "1" ] && [ ${1:0:1} != "-" ]
then
    if [ "$1" != "launch" ] && [ "$1" != "destroy" ] && [ "$1" != "login" ] && [ "$1" != "stop" ] && [ "$1" != "start" ] && [ "$1" != "get-master" ]
    then
	no_cred_dir="false"
	credentials_dir=$1
    fi
fi


if [ -z "$no_cred_dir" ]
then
    ret=0
    credentials_dir=$AWS_CREDENTIALS_DIR
    if [ -z "$credentials_dir" ]
    then
	credentials_dir=$hardcoded_credentials_dir
    fi
fi

# Make sure there is only 1 .csv and 1 .pem file in the directory
if [ `ls $credentials_dir | grep ".csv" | wc -l` -ne "1" ]
then
    echo "ERROR: credentials directory must contain exactly one .csv file"
    exit
fi
if [ `ls $credentials_dir | grep ".pem" | wc -l` -ne "1" ]
then
    echo "ERROR: credentials directory must contain exactly one .pem file"
    exit
fi

# Look up the secret and access keys in the aws credentials.csv file
credentials_file=`ls $credentials_dir/*.csv`
export AWS_ACCESS_KEY_ID=`cat $credentials_file | grep -v "User Name" | cut -f 2 -d ","`
export AWS_SECRET_ACCESS_KEY=`cat $credentials_file | grep -v "User Name" | cut -f 3 -d ","`

# get the name of the key file
key_file=`ls $credentials_dir/*.pem`
export AWS_EC2_KEY_NAME=$(basename "$key_file" .pem)
export AWS_EC2_KEY_FILE=$key_file

# file paths for ssl certificates (these may not exist)
export SSL_CERT_FILE="$credentials_dir/sampleclean1_eecs_berkeley_edu_chained.cer"
export SSL_KEY_FILE="$credentials_dir/sampleclean1.eecs.berkeley.edu-san.key"

# return whether the credentials directory was in the argument list.
return $ret
