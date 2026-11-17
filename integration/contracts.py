"""Client-visible boundaries that span catalog generations or multiple requests."""
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from urllib.error import HTTPError
from uuid import uuid4

from lock import request


def outcome(operation, **kwargs):
    try:
        return request(operation, **kwargs)
    except HTTPError as error:
        return json.load(error)["__type"]


def race(operation, *requests):
    start = Barrier(len(requests))

    def run(kwargs):
        start.wait(timeout=10)
        return outcome(operation, **kwargs)

    with ThreadPoolExecutor(len(requests)) as pool:
        return list(pool.map(run, requests))


def table_request(name, key):
    return dict(TableName=name, BillingMode="PAY_PER_REQUEST",
                KeySchema=[dict(AttributeName=key, KeyType="HASH")],
                AttributeDefinitions=[dict(AttributeName=key, AttributeType="S")])


def exercise():
    name = "Contract" + uuid4().hex
    definitions = [table_request(name, key) for key in ("pk", "sk")]
    unused = definitions[0]["AttributeDefinitions"] + definitions[1]["AttributeDefinitions"]
    assert outcome("CreateTable", **{**definitions[0], "AttributeDefinitions": unused}) == "ValidationException"
    results = race("CreateTable", *definitions)
    try:
        assert results.count("ResourceInUseException") == 1
        winner = next(result["TableDescription"] for result in results if isinstance(result, dict))
        pk = winner["KeySchema"][0]["AttributeName"]
        table = dict(TableName=name)
        key = {pk: {"S": "0"}}
        assert outcome("GetItem", **table, Key={pk: {"N": "1"}}) == "ValidationException"
        assert request("DeleteItem", **table, Key=key, ReturnValues="ALL_OLD") == {}
        assert outcome("DeleteItem", **table, Key=key, ConditionExpression=f"attribute_exists({pk})") == "ConditionalCheckFailedException"
        request("UpdateItem", **table, Key=key, UpdateExpression="REMOVE unusedField")
        assert "Item" not in request("GetItem", **table, Key=key, ConsistentRead=True)
        writes = [{"PutRequest": {"Item": {pk: {"S": str(i)}}}} for i in range(26)]
        assert outcome("BatchWriteItem", RequestItems={name: writes}) == "ValidationException"
        assert outcome("BatchWriteItem", RequestItems={name: writes[:1]}, ReturnItemCollectionMetrics="SIZE") == "ValidationException"
        assert "Item" not in request("GetItem", **table, Key=key)
        assert not request("BatchWriteItem", RequestItems={name: writes[:25]})["UnprocessedItems"]
        values = {":v": {"L": [{"S": "added"}]}}
        assert outcome("UpdateItem", **table, Key=key, UpdateExpression="SET arr = list_append(list_append(:v,:v),:v)",
                       ExpressionAttributeValues=values) == "ValidationException"
        assert request("UpdateItem", **table, Key=key, UpdateExpression="SET arr = list_append(if_not_exists(arr,:v),:v)",
                       ExpressionAttributeValues=values, ReturnValues="ALL_NEW")["Attributes"]["arr"]["L"] == values[":v"]["L"] * 2
        arn = winner["TableArn"]
        tags = dict(ResourceArn=arn)
        request("TagResource", **tags, Tags=[dict(Key=str(i), Value="") for i in range(49)])
        additions = race("TagResource", *[{**tags, "Tags": [dict(Key=k, Value="")]} for k in ("one", "two")])
        assert additions.count("ValidationException") == 1 and {} in additions
        assert len(request("ListTagsOfResource", **tags)["Tags"]) == 50
        for invalid in ("", "x"*129, "aws:owned"):
            assert outcome("UntagResource", **tags, TagKeys=["0", invalid]) == "ValidationException"
        assert len(request("ListTagsOfResource", **tags)["Tags"]) == 50
        assert outcome("ListTagsOfResource", **tags, NextToken="token") == "ValidationException"
        assert outcome("ListTagsOfResource", ResourceArn="invalid") == "ValidationException"
        assert request("DescribeContinuousBackups", TableName=arn)["ContinuousBackupsDescription"]["ContinuousBackupsStatus"] == "DISABLED"
        competing = race("UpdateTimeToLive", *[{**table, "TimeToLiveSpecification": dict(Enabled=True, AttributeName=k)}
                                               for k in ("expiresA", "expiresB")])
        assert competing.count("ValidationException") == 1
        attr = next(r["TimeToLiveSpecification"]["AttributeName"] for r in competing if isinstance(r, dict))
        assert request("DescribeTimeToLive", **table)["TimeToLiveDescription"]["AttributeName"] == attr
        policy = {**table, "TimeToLiveSpecification": dict(Enabled=False, AttributeName=attr)}
        replacement = {**table, "TimeToLiveSpecification": dict(Enabled=True, AttributeName="later")}
        disabled, enabled = race("UpdateTimeToLive", policy, replacement)
        assert isinstance(disabled, dict)
        current = request("DescribeTimeToLive", **table)["TimeToLiveDescription"]
        if isinstance(enabled, dict):
            assert current == dict(TimeToLiveStatus="ENABLED", AttributeName="later")
        else:
            assert enabled == "ValidationException" and current["TimeToLiveStatus"] == "DISABLED"
        request("DeleteTable", **table)
        assert outcome("DescribeContinuousBackups", **table) == "TableNotFoundException"
        request("CreateTable", **definitions[0 if pk == "sk" else 1])
        assert not request("ListTagsOfResource", **tags)["Tags"]
        assert outcome("GetItem", **table, Key=key, ProjectionExpression="arr") == "ValidationException"
        # A page cursor remains usable if the last table on that page is deleted.
        later = name + "B"
        request("CreateTable", **table_request(later, "pk"))
        try:
            page = request("ListTables", ExclusiveStartTableName=name[:-1], Limit=1)
            assert page["TableNames"] == [name]
            request("DeleteTable", TableName=name)
            assert request("ListTables", ExclusiveStartTableName=page["LastEvaluatedTableName"],
                           Limit=1)["TableNames"] == [later]
        finally:
            request("DeleteTable", TableName=later)
    finally:
        outcome("DeleteTable", TableName=name)
