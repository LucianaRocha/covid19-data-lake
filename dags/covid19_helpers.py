import logging


from airflow import settings
from airflow.models.connection import Connection
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.S3_hook import S3Hook


def check_csv_data_exists(bucket, prefix, file):
    logging.info('checking whether data exists in s3')
    source_s3 = S3Hook(aws_conn_id='aws_default')

    if not source_s3.check_for_bucket(bucket):
        raise Exception('Bucket not found:', bucket)

    if not source_s3.check_for_prefix(bucket, prefix, "/"):
        raise Exception('Prefix not found:', prefix)

    if not source_s3.check_for_key(prefix+'/'+file, bucket):
        raise Exception('File not found:', file)

    return f'File found: bucket: {bucket}, prefix: {prefix}, file: {file}'


def check_wildcard_data_exists(bucket, prefix):
    logging.info('checking whether data exists in s3')
    source_s3 = S3Hook(aws_conn_id='aws_default')

    if not source_s3.check_for_bucket(bucket):
        raise Exception('Bucket not found:', bucket)

    if not source_s3.check_for_prefix(bucket, prefix, "/"):
        raise Exception('Prefix not found:', prefix)

    if not source_s3.check_for_wildcard_key(
            prefix+'/*.json', bucket, delimiter='/'):
        raise Exception(f'No file found in bucket: {bucket} prefix: {prefix}')

    return f'File found in: bucket: {bucket}, prefix: {prefix}'


def transfer_brazil_data_file():
    logging.info('Transfering Brazil data file from the source to s3 bucket')

    # Create a connection to the source server
    conn = Connection(
          conn_id='http_conn_brasilio',
          conn_type='http',
          host='data.brasil.io',
          port=80
    ) #create a connection object
    session = settings.Session() # get the session
    session.add(conn)
    session.commit()

    # Get the data file
    http_hook = HttpHook(method='GET', http_conn_id='http_conn_brasilio')
    response_br_data = http_hook.run('dataset/covid19/caso.csv.gz')

    # Store data file into s3 bucket
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_bytes(response_br_data.content,
                       'raw-data/COVID-19-Brazil.csv.gz',
                       bucket_name='covid19-input',
                       replace=True)

    logging.info('Data transfer finished')


emr_settings = {
   "Applications": [
      {
         "Name": "Spark"
      }
   ],
   "EbsRootVolumeSize": 10,
   "Instances": { 
      "Ec2SubnetId": "subnet-07c6842aab83fbc68",
      "EmrManagedMasterSecurityGroup": "sg-0a8e4b7fe2ca836d4",
      "EmrManagedSlaveSecurityGroup": "sg-08274d6acf976f276",
      "InstanceGroups": [
         { 
            "EbsConfiguration": { 
               "EbsBlockDeviceConfigs": [ 
                  { 
                     "VolumeSpecification": { 
                        "SizeInGB": 32,
                        "VolumeType": "gp2"
                     },
                     "VolumesPerInstance": 2
                  }
               ],
            },
            "InstanceCount": 1,
            "InstanceRole": "MASTER",
            "InstanceType": "m5.xlarge",
            "Name": "Master node"
         },
         { 
            "EbsConfiguration": { 
               "EbsBlockDeviceConfigs": [ 
                  { 
                     "VolumeSpecification": { 
                        "SizeInGB": 32,
                        "VolumeType": "gp2"
                     },
                     "VolumesPerInstance": 2
                  }
               ],
            },
            "InstanceCount": 2,
            "InstanceRole": "CORE",
            "InstanceType": "m5.xlarge",
            "Name": "Core node"
         }
      ],
      "KeepJobFlowAliveWhenNoSteps": True,
   },
   "JobFlowRole": "EMR_EC2_DefaultRole",
   "LogUri": "s3n://aws-logs-837754688468-sa-east-1/elasticmapreduce/",
   "Name": "covid19-emr-cluster",
   "ReleaseLabel": "emr-5.30.0",
   "ServiceRole": "EMR_DefaultRole",
   "VisibleToAllUsers": True
}


pipeline_path = "https://raw.githubusercontent.com/LucianaRocha/"\
               +"covid19-data-lake/master/covid19_etl.py"


covid19_pipeline = [{
   "Name": "Spark Step One",
   "ActionOnFailure": "CONTINUE",
   "HadoopJarStep": {
      "Jar":"command-runner.jar",
      "Args": [
         "spark-submit",
         "--deploy-mode", "client",
         "--py-files", pipeline_path,
         pipeline_path
         ]
      }
   }]

   