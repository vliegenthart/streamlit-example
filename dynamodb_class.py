# ##### User
# - Main entity which represents a customer.
# - On the primary index, indexed by the Cognito user ID.
# - On GSI the record is indexed by the customer's email to make it easy to retrieve a record by email.
# - On GSI2, for enterprise customers only, the record is indexed by the enterprise ID and sorted by enterprise user type & email.

# ##### Dataset

# - Represents a dataset of a user.
# - Indexed on primary index by user ID and sorted by dataset ID which is a "ulid". Enables querying datasets of a user sorted by creation date.
# - Indexed on GSI by Reap Document ID. Enables finding the dataset by Reap Document ID, when Reap notifies an update to a document via SNS.
# - Sorted on LSI by dataset name. Enables querying datasets of a user sorted by dataset name.

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import logging
import json
from benedict import benedict
import os

logger = logging.getLogger(__name__)

ENV = os.getenv("ENV", "staging")
TABLE_NAME = f"parsel-backend-{ENV}"

# For inspiration see https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/python/example_code/dynamodb/GettingStarted/scenario_getting_started_movies.py
class Items:
    """Encapsulates an Amazon DynamoDB table of items data."""

    def __init__(self):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.dyn_resource = boto3.resource("dynamodb", region_name="eu-west-1")
        self.table = None
        self.table_name = None

        self.exists(TABLE_NAME)

    def exists(self, table_name):
        """
        Determines whether a table exists. As a side effect, stores the table in
        a member variable.
        :param table_name: The name of the table to check.
        :return: True when the table exists; otherwise, False.
        """
        try:
            table = self.dyn_resource.Table(table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response["Error"]["Code"] == "ResourceNotFoundException":
                exists = False
            else:
                logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    table_name,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
                raise
        else:
            self.table = table
            self.table_name = table_name
        return exists

    def get_user(self, id):
        """
        TODO

        :param id: User Id
        :return: The user data
        """
        try:
            response = self.table.get_item(Key={"pk": id, "sk": "user"})
        except ClientError as err:
            logger.error(
                "Couldn't get user %s from table %s. Here's why: %s: %s",
                id,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return response

    def query_enterprise_users(self, enterprise_id):
        try:

            return self.query(Key("gsi2Pk").eq(enterprise_id), "gsi-2").get("Items")

        except Exception as exc:
            return None

    def query_enterprise(self, enterprise_id):
        try:
            return benedict(
                self.query(
                    Key("pk").eq(enterprise_id) & Key("sk").eq("enterprise"),
                    None,
                    False,
                ).get("Items")[0]
            )
        except Exception as exc:
            return None

    def query_user_by_email(self, email):

        try:
            return benedict(
                self.query(Key("gsiPk").eq(f"user#{email}"), "gsi")["Items"][0]
            )
        except Exception as exc:
            return None

    def query_user_datasets(self, user_id):
        try:
            return self.query(
                Key("pk").eq(user_id) & Key("sk").begins_with("dataset"), None, False
            )["Items"]
        except Exception as exc:
            return None

    def query(
        self,
        key_condition_expression,
        index_name=None,
        is_index=True,
    ):
        """
        Queries the table.
        """
        try:
            if is_index:

                response = self.table.query(
                    IndexName=f"{self.table_name}-{index_name}",
                    KeyConditionExpression=key_condition_expression,
                )
            else:
                response = self.table.query(
                    KeyConditionExpression=key_condition_expression,
                )

        except ClientError as err:
            logger.error(
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return benedict(dict(json.loads(json.dumps(response, cls=DecimalEncoder))))


import decimal


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


# if __name__ == "__main__":
#     ENV = "prod"
#     TABLE_NAME = f"parsel-backend-{ENV}"

#     items_table = Items(boto3.resource("dynamodb", region_name="eu-west-1"))
#     exists = items_table.exists(TABLE_NAME)
#     # user_response = (
#     #     dict(items_table.get_user("user#55e6af69-c4de-47cd-9da1-53fb8acb0815")),
#     # )
#     # print(user_response)
#     # enterprise_users_response = items_table.query_enterprise_user_ids(
#     #     user_response[0]["Item"]["gsi2Pk"]
#     # )["Items"]

#     # print(json.dumps(enterprise_users_response, indent=4, cls=DecimalEncoder))
#     # print([x["email"] for x in enterprise_users_response])

#     user = items_table.query_user_by_email("daniel@parsel.ai")
#     e_users = items_table.query_enterprise_users(user["gsi2Pk"])["Items"]

#     datasets = items_table.query_user_datasets(user["pk"])

#     print([x["sk"] for x in datasets])
#     print([x["email"] for x in e_users])


from botocore.exceptions import ClientError


def create_presigned_url(
    object_name, bucket_name=f"{ENV}-parsel-backend-documents", expiration=3600
):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    s3_client = boto3.client("s3")
    try:
        response = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_name},
            ExpiresIn=expiration,
        )
    except ClientError as e:
        logging.error(e)
        return None

    # The response contains the presigned URL
    return response
