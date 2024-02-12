import time
import pandas as pd

import boto3

class ResAthena():
    AWS__REGION = 'us-east-1'
    AWS__DATABASE_NAME = 'athena'
    AWS_TYPE_MAP = {
        'varchar': 'str',
        'double': 'float',
        'bigint': 'int',
        'boolean': 'bool',
        'date': 'datetime64[ns]',
        'timestamp': 'datetime64[ns]',
    }
    
    def __init__(
        self,
        aws_access_key,
        aws_access_secret,
        aws_s3_output_location,
        aws_region=AWS__REGION,
        aws_database_name=AWS__DATABASE_NAME,
        verbose=True,
    ):
        """
        Init ResAthena object

        Args:
            aws_access_key_id (str): AWS access key
            aws_access_secret (str): AWS access secret
            aws_s3_output_location (str): S3 output location (e.g. S3://my-bucket/athena/)
            aws_region (str): AWS region
            aws_database_name (str): the database name this instance will access
            verbose (bool): verbose flag
        
        Returns:
            None
        """
        self.aws_s3_output_location = aws_s3_output_location
        self.aws_region = aws_region
        self.aws_database_name = aws_database_name
        
        self.verbose = verbose

        self.session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_access_secret,
        )

        self.client = self.session.client('athena', region_name=aws_region)

        self.query_ids = []
    
    def register_query(
        self,
        q,
        aws_database_name=None,
        s3_output_location=None,
    ):
        """
        register a query

        Args:
            q (str): the query string
            (optional) aws_database_name (str): the database name
            (optional) s3_output_location (str): the output location

        Returns:
            str: the query execution id

        """
        aws_database = aws_database_name or self.aws_database_name
        s3_output_location = s3_output_location or self.aws_s3_output_location

        if self.verbose:
            print(f'registering query: {q}')
        
        response = self.client.start_query_execution(
            QueryString=q,
            QueryExecutionContext={
                'Database': aws_database
            },
            ResultConfiguration={
                'OutputLocation': s3_output_location,
            }
        )

        query_execution_id = response['QueryExecutionId']

        if self.verbose:
            print(f"QueryExecutionId={query_execution_id}")

        self.query_ids.append(query_execution_id)

        return query_execution_id

    def get_query_result(self, q_id, time_out=60):
        """
        Retrieve query result in raw format

        Args:
            q_id (str): the query execution id
            (optional) time_out (int): the time out in seconds

        Returns:
            (bool, dict): the success flag and the raw response or query execution info
        """

        query_status = ""

        # wait for query execution
        total_time = 0
        while True:
            response = self.client.get_query_execution(QueryExecutionId=q_id)
            query_status = response['QueryExecution']['Status']['State']
            if query_status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(5)
            total_time += 5

            if total_time > time_out:
                raise Exception("Query Retrieval Timed Out!")

        # get results
        if query_status == 'SUCCEEDED':
            return True, self.client.get_query_results(QueryExecutionId=q_id)

        else:
            return False, self.client.get_query_execution(QueryExecutionId=q_id)

    def get_query_df(self, q_id, time_out=60):
        """
        Retrieve query result in DataFrame format

        Args:
            q_id (str): the query execution id
            (optional) time_out (int): the time out in seconds
        
        Returns:
            (bool, pd.DataFrame): the success flag and the DataFrame

        """

        succeeded, response = self.get_query_result(
            q_id=q_id,
            time_out=time_out
        )

        # failed query
        if not succeeded:
            return succeeded, response

        out = []
        column_info = response['ResultSet']['ResultSetMetadata']['ColumnInfo']
        column_names = [col['Name'] for col in column_info]
        column_types = [self.AWS_TYPE_MAP.get(col['Type'], 'str') for col in column_info]
        
        for row in response['ResultSet']['Rows']:
            # Each row is a dictionary with a 'Data' key
            data = row['Data']
            row_data = [field.get('VarCharValue', None) for field in data]
            out.append(row_data)

        if out[0] == column_names:  # Check if first row matches column names
            df = pd.DataFrame(out[1:], columns=column_names)
        else:
            df = pd.DataFrame(out, columns=column_names)

        df = df.astype(dict(zip(column_names, column_types)))

        return True, df