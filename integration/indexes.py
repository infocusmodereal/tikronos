"""Check index ordering, concurrent backfill, sparse updates and table generations."""
import time
import json
from urllib.error import HTTPError
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from lock import request


def schema(hash_key, sort_key=None):
    return [{"AttributeName": hash_key, "KeyType": "HASH"}] + (
        [{"AttributeName": sort_key, "KeyType": "RANGE"}] if sort_key else [])


def definition(*names):
    return [{"AttributeName": name, "AttributeType": "S"} for name in names]


def validation(call, operation, **args):
    try:
        call(operation, **args)
    except HTTPError as error:
        assert json.load(error)["__type"] == "ValidationException"
    except Exception as error:
        assert error.response["Error"]["Code"] == "ValidationException"
    else:
        raise AssertionError("Expected ValidationException")


def exercise(call=request, restart=None):
    name = "Indexes" + uuid4().hex
    create = dict(TableName=name, BillingMode="PAY_PER_REQUEST", KeySchema=schema("pk", "sk"),
                  AttributeDefinitions=definition("pk", "sk", "rank"),
                  GlobalSecondaryIndexes=[{"IndexName": "byRank", "KeySchema": schema("rank"),
                                           "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["detail"]}}],
                  LocalSecondaryIndexes=[{"IndexName": "localRank", "KeySchema": schema("pk", "rank"),
                                          "Projection": {"ProjectionType": "KEYS_ONLY"}}])
    table = call("CreateTable", **create)["TableDescription"]
    try:
        arn = table["TableArn"]
        call("TagResource", ResourceArn=arn, Tags=[{"Key": "owner", "Value": "local"}])
        assert call("ListTagsOfResource", ResourceArn=arn)["Tags"] == [{"Key": "owner", "Value": "local"}]
        call("UntagResource", ResourceArn=arn, TagKeys=["owner"])
        assert not call("ListTagsOfResource", ResourceArn=arn)["Tags"]
        assert call("DescribeContinuousBackups", TableName=name)["ContinuousBackupsDescription"]["ContinuousBackupsStatus"] == "DISABLED"
        rows = [{"pk": {"S": "p"}, "sk": {"S": f"{i:03}"}, "groupKey": {"S": "g"},
                 "rank": {"S": "a" if i % 2 else "a\0"}, "detail": {"S": "payload"},
                 "unprojected": {"S": "base"}} for i in range(96)]
        for row in rows:
            call("PutItem", TableName=name, Item=row)
        call("PutItem", TableName=name, Item={"pk": {"S": "p"}, "sk": {"S": "sparse"}})
        status = call("UpdateTable", TableName=name, AttributeDefinitions=definition("groupKey", "rank"),
                      GlobalSecondaryIndexUpdates=[{"Create": {"IndexName": "byGroup", "KeySchema": schema("groupKey", "rank"),
                                                                "Projection": {"ProjectionType": "ALL"}}}])
        def by_group_status(description):
            return next(i["IndexStatus"] for i in description["GlobalSecondaryIndexes"] if i["IndexName"] == "byGroup")

        assert by_group_status(status["TableDescription"]) == "CREATING"

        def concurrent_updates():
            for row in rows[:12]:
                call("UpdateItem", TableName=name, Key={k: row[k] for k in ("pk", "sk")},
                     UpdateExpression="SET groupKey = :g", ExpressionAttributeValues={":g": {"S": "moved"}})

        if restart:
            before = call("DescribeTable", TableName=name)["Table"]
            assert by_group_status(before) == "CREATING"
            restart()
        with ThreadPoolExecutor(1) as pool:
            pool.submit(concurrent_updates).result()
        for _ in range(120):
            desc = call("DescribeTable", TableName=name)["Table"]
            if by_group_status(desc) == "ACTIVE":
                break
            time.sleep(0.5)
        else:
            raise AssertionError("Index backfill never became ACTIVE")

        query = dict(TableName=name, IndexName="byGroup", KeyConditionExpression="groupKey = :g",
                     ExpressionAttributeValues={":g": {"S": "g"}})

        def pages(**args):
            result, cursor = [], {}
            seen_cursors = set()
            while True:
                page = call("Query", **args, Limit=7, **cursor)
                result.extend(page["Items"])
                if not page.get("LastEvaluatedKey"):
                    return result
                marker = json.dumps(page["LastEvaluatedKey"], sort_keys=True)
                assert marker not in seen_cursors, "Pagination cursor did not advance"
                seen_cursors.add(marker)
                cursor = {"ExclusiveStartKey": page["LastEvaluatedKey"]}

        assert call("Scan", TableName=name)["Count"] == 97
        validation(call, "Query", **query, ConsistentRead=True)
        validation(call, "PutItem", TableName=name, Item={**rows[12], "rank": {"N": "1"}})
        assert call("GetItem", TableName=name, Key={k: rows[12][k] for k in ("pk", "sk")})["Item"] == rows[12]
        capacity = call("Query", **query, Limit=1, ReturnConsumedCapacity="INDEXES")["ConsumedCapacity"]
        assert capacity["GlobalSecondaryIndexes"]["byGroup"]["CapacityUnits"] > 0
        forward = pages(**query)
        backward = pages(**query, ScanIndexForward=False)
        assert len(forward) == 84 and backward == forward[::-1]
        assert all(row["unprojected"] == {"S": "base"} for row in forward)
        partial = dict(TableName=name, IndexName="byRank")
        assert all("unprojected" not in row for row in call("Scan", **partial)["Items"])
        validation(call, "Scan", **partial, ProjectionExpression="unprojected")
        validation(call, "Scan", **partial, FilterExpression="attribute_exists(unprojected)")
        assert len({row["sk"]["S"] for row in forward}) == 84
        assert [row["rank"]["S"] for row in forward] == sorted(row["rank"]["S"] for row in forward)
        equals = pages(TableName=name, IndexName="byGroup", KeyConditionExpression="groupKey = :g AND #r = :r",
                       ExpressionAttributeNames={"#r": "rank"},
                       ExpressionAttributeValues={":g": {"S": "g"}, ":r": {"S": "a"}})
        assert len(equals) == 42 and all(row["rank"] == {"S": "a"} for row in equals)
        moved = pages(**{**query, "ExpressionAttributeValues": {":g": {"S": "moved"}}})
        assert len(moved) == 12
        local = call("Query", TableName=name, IndexName="localRank", ConsistentRead=True,
                     KeyConditionExpression="pk = :p", ExpressionAttributeValues={":p": {"S": "p"}})["Items"]
        assert len(local) == 96 and all(set(row) == {"pk", "sk", "rank"} for row in local)
        order = [row["sk"] for row in local]
        key = {"pk": rows[12]["pk"], "sk": rows[12]["sk"]}
        call("UpdateItem", TableName=name, Key=key, UpdateExpression="SET detail = :d",
             ExpressionAttributeValues={":d": {"S": "updated"}})
        local = call("Query", TableName=name, IndexName="localRank", ConsistentRead=True,
                     KeyConditionExpression="pk = :p", ExpressionAttributeValues={":p": {"S": "p"}},
                     ProjectionExpression="pk, sk, detail")["Items"]
        assert [row["sk"] for row in local] == order
        filtered = call("Scan", TableName=name, IndexName="localRank", Select="COUNT",
                        FilterExpression="detail = :d", ExpressionAttributeValues={":d": {"S": "updated"}},
                        ReturnConsumedCapacity="INDEXES")
        assert (filtered["Count"], filtered["ScannedCount"]) == (1, 96)
        assert filtered["ConsumedCapacity"]["Table"]["CapacityUnits"] > 0
        assert next(row for row in local if row["sk"] == key["sk"])["detail"] == {"S": "updated"}
        assert next(row for row in pages(**query) if row["sk"] == key["sk"])["detail"] == {"S": "updated"}
        call("UpdateItem", TableName=name, Key=key, UpdateExpression="REMOVE groupKey")
        assert len(pages(**query)) == 83
        call("DeleteTable", TableName=name)
        recreated = call("CreateTable", **create)["TableDescription"]
        assert recreated["TableId"] != table["TableId"]
        assert not call("Scan", TableName=name, IndexName="localRank")["Items"]
    finally:
        call("DeleteTable", TableName=name)


if __name__ == "__main__":
    exercise()
