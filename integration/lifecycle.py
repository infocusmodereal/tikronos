"""Check TTL index cleanup, paged stream images and generation isolation through HTTP."""
import time
from uuid import uuid4

from lock import request


def stream_request(operation, **payload):
    return request(operation, _target="DynamoDBStreams_20120810.", **payload)


def exercise(call=request, streams=stream_request):
    name = "Lifecycle" + uuid4().hex
    schema = dict(TableName=name, BillingMode="PAY_PER_REQUEST",
                  KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                  AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                        {"AttributeName": "group", "AttributeType": "S"}],
                  GlobalSecondaryIndexes=[{"IndexName": "by_group",
                      "KeySchema": [{"AttributeName": "group", "KeyType": "HASH"}],
                      "Projection": {"ProjectionType": "ALL"}}],
                  StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"})
    created = call("CreateTable", **schema)["TableDescription"]
    old_arn = created["LatestStreamArn"]
    try:
        key = {"pk": {"S": "row"}}
        initial = {**key, "group": {"S": "g"}, "expires": {"N": str(int(time.time()) + 3600)}}
        call("PutItem", TableName=name, Item=initial)
        call("UpdateTimeToLive", TableName=name,
             TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires"})
        assert call("DescribeTimeToLive", TableName=name)["TimeToLiveDescription"]["TimeToLiveStatus"] == "ENABLED"
        query = dict(TableName=name, IndexName="by_group", KeyConditionExpression="#g = :g",
                     ExpressionAttributeNames={"#g": "group"}, ExpressionAttributeValues={":g": {"S": "g"}})
        assert call("Query", **query)["Count"] == 1
        call("UpdateItem", TableName=name, Key=key, UpdateExpression="SET expires = :t",
             ExpressionAttributeValues={":t": {"N": str(int(time.time()) - 1)}})
        deadline = time.monotonic() + 30
        while call("GetItem", TableName=name, Key=key, ConsistentRead=True).get("Item"):
            assert time.monotonic() < deadline, "TTL worker did not expire the item"
            time.sleep(0.1)
        assert call("Query", **query)["Count"] == 0
        shard = streams("DescribeStream", StreamArn=old_arn)["StreamDescription"]["Shards"][0]["ShardId"]
        iterator = streams("GetShardIterator", StreamArn=old_arn, ShardId=shard,
                           ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
        events = []
        while len(events) < 3:
            page = streams("GetRecords", ShardIterator=iterator, Limit=1)
            events.extend(page["Records"])
            iterator = page["NextShardIterator"]
            if not page["Records"]:
                time.sleep(0.1)
            assert time.monotonic() < deadline, "Expected stream records were not visible"
        assert [event["eventName"] for event in events] == ["INSERT", "MODIFY", "REMOVE"]
        assert events[0]["dynamodb"]["NewImage"] == initial
        assert "OldImage" not in events[0]["dynamodb"]
        assert events[-1]["userIdentity"]["PrincipalId"] == "dynamodb.amazonaws.com"
        sequences = [int(event["dynamodb"]["SequenceNumber"]) for event in events]
        assert sequences == sorted(set(sequences))
        call("UpdateTable", TableName=name, StreamSpecification={"StreamEnabled": False})
        assert "NextShardIterator" not in streams("GetRecords", ShardIterator=iterator)
        enabled = call("UpdateTable", TableName=name,
                       StreamSpecification={"StreamEnabled": True, "StreamViewType": "KEYS_ONLY"})
        assert enabled["TableDescription"]["LatestStreamArn"] != old_arn
        call("DeleteTable", TableName=name)
        recreated = call("CreateTable", **schema)["TableDescription"]
        assert recreated["TableId"] != created["TableId"]
        assert recreated["LatestStreamArn"] != old_arn
        assert call("DescribeTimeToLive", TableName=name)["TimeToLiveDescription"]["TimeToLiveStatus"] == "DISABLED"
        assert call("Scan", TableName=name)["Count"] == 0
        assert streams("DescribeStream", StreamArn=old_arn)["StreamDescription"]["StreamStatus"] == "DISABLED"
    finally:
        call("DeleteTable", TableName=name)


if __name__ == "__main__":
    exercise()
