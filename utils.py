"""This module offers some basic utility methods to create and delete redshift clusters.
Functions are mainly for the purpose of the project, hence they are not generic.

PLEASE before running make sure to populate the following global variables:
- KEY = Access key ID for an AWS user with permissions to create and delete redshift clusters.
- SECRET = Secret access key for an AWS user with permissions to create and delete redshift clusters.
- CLUSTER_IDENTIFIER = ... The cluster identifier, which must be unique for the region it's created in.
"""

import configparser
from functools import wraps
import time
import pandas as pd
import boto3


KEY = ''
SECRET = ''
CLUSTER_IDENTIFIER = 'udcty-project-cluster'


def timer_wrap(func):
    """A wrapper function that calculates the execution time of the function wrapped.

    :param func: function to be wrapped by the timing logic.
    :return: function wrapped by the timing logic which prints execution time.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        print(f"Function '{func.__name__}' executed in {end - start:.4f} seconds")
        print(f"args: {args}")
        print(f"kwargs: {kwargs}")
        print('-' * 70)
        return result

    return wrapper


def pretty_redshift_props(props):
    pd.set_option('display.max_colwidth', None)
    keys_to_show = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                    "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keys_to_show]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def create_redshift_cluster(access_key, access_secret):
    """Creates a 'non-custom' redshift cluster for the purpose of the project.
        Following parameters must be filled in the "dwh.cfg" file:
            - CLUSTER.db_name
            - CLUSTER.db_user
            - CLUSTER.db_password
            - CLUSTER.db_port
            - IAM_ROLE.arn

    :param (str) access_key: Access key id for an AWS user with permissions to create a redshift cluster.
    :param (str) access_secret: Secret access key for an AWS user with permissions to create a redshift cluster.

    :raises Exception: If an error occurs while AWS is processing the cluster creation process.

    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    _, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT = config['CLUSTER'].values()
    role_arn = config['IAM_ROLE']['arn']

    # Creating new cluster
    print('Creating Redshift cluster...')

    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=access_key,
                            aws_secret_access_key=access_secret
                            )

    try:
        redshift.create_cluster(
            # HW
            ClusterType='multi-node',
            NodeType='dc2.large',
            NumberOfNodes=8,

            # Identifiers & Credentials
            DBName=DB_NAME,
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            PubliclyAccessible=True,

            # Roles (for s3 access)
            IamRoles=[role_arn])
    except Exception as e:
        raise e

    # Adding endpoint for the created newly cluster
    timeout_mins = 10
    start_time = time.time()
    host = None
    cluster_props = None

    while (time.time() - start_time) < timeout_mins * 60:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
        if cluster_props['ClusterStatus'] == 'Available':
            host = cluster_props['Endpoint']['Address']
            break
        time.sleep(15)

    if host:
        config['CLUSTER']['host'] = host
        with open('dwh.cfg', 'w') as configfile:
            config.write(configfile)
    else:
        print("Couldn't get the cluster's host. "
              "Please find it in the AWS redshift console and update 'dwh.cfg' accordingly.")

    redshift.close()
    if cluster_props:
        print('CLUSTER PROPS:')
        print(pretty_redshift_props(cluster_props))
        print('-' * 100)


def delete_redshift_cluster(access_key, access_secret):
    """Deletes the redshift cluster created for the project purpose.
        (Doesn't delete IAM role )

    :param (str) access_key: Access key id for an AWS user with permissions to delete redshift clusters.
    :param (str) access_secret: Secret access key for an AWS user with permissions to create redshift clusters.

    :raises Exception: If an error occurs while AWS is processing the cluster creation process.
    """
    # Deleting cluster
    print('Deleting Redshift cluster...')

    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=access_key,
                            aws_secret_access_key=access_secret
                            )
    try:
        redshift.delete_cluster(ClusterIdentifier=CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)

    except redshift.exceptions.ClusterNotFoundFault:
        print(f"Deletion will be skipped as cluster '{CLUSTER_IDENTIFIER}' was not found.")
        return
    except Exception as e:
        raise e

    # keep checking cluster status for about 5 mins to confirm deletion process is completed
    deletion_confirmed = False
    for i in range(5):
        try:
            status = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']
            print(f'Cluster status is: {status}')
            time.sleep(30)  # wait for 30 secs before checking again
        except redshift.exceptions.ClusterNotFoundFault:
            print('Cluster no longer found (deletion confirmed)')
            deletion_confirmed = True
            break
        except Exception as e:
            raise e

    if not deletion_confirmed:
        print(
            'Cluster deletion process initiated successfully, but cluster deletion could not be confirmed after 5 mins.')

    # Clearing Endpoint value since the cluster is being/was deleted...
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    config['CLUSTER']['HOST'] = ''
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)

    redshift.close()


if __name__ == '__main__':
    # -- Uncomment the following code line to create redshift cluster
    # create_redshift_cluster(KEY, SECRET)

    # -- Uncomment the following code line to delete redshift cluster
    # delete_redshift_cluster(KEY, SECRET)
    pass
