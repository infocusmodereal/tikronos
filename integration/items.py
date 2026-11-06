"""Cross-operation checks for item updates, cursor boundaries and partial batches."""
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from lock import request


def exercise(call=request):
    table = "Items" + uuid4().hex
    call("CreateTable", TableName=table, BillingMode="PAY_PER_REQUEST",
         KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
         AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "B"}, {"AttributeName": "sk", "AttributeType": "N"}])
    partition = {"B": "AAE="}
    keys = [{"pk": partition, "sk": {"N": n}} for n in ["-12", "-0.1", "0", "1.01", "1.1", "10"]]
    try:
        rows = [{**key, "doc": {"M": {"vals": {"L": [{"NULL": True}, {"BOOL": False}]}}},
                 "labels": {"SS": ["old", "keep"]}} for key in keys]
        resp = call("BatchWriteItem", RequestItems={table: [{"PutRequest": {"Item": row}} for row in rows]})
        assert not resp["UnprocessedItems"]
        query = dict(TableName=table, KeyConditionExpression="pk = :p", ExpressionAttributeValues={":p": partition})
        for reverse in (False, True):
            seen, cursor = [], {}
            while True:
                page = call("Query", **query, ScanIndexForward=not reverse, Limit=2, **cursor)
                seen.extend(row["sk"]["N"] for row in page["Items"])
                if not page.get("LastEvaluatedKey"):
                    break
                cursor = {"ExclusiveStartKey": page["LastEvaluatedKey"]}
            ordered = [key["sk"]["N"] for key in keys]
            assert seen == (ordered[::-1] if reverse else ordered), seen
        empty = call("Query", **query, FilterExpression="attribute_exists(missingField)", Limit=1)
        assert not empty["Items"] and empty["LastEvaluatedKey"] and empty["ScannedCount"] == 1
        result = call("UpdateItem", TableName=table, Key=keys[0], ReturnValues="ALL_NEW",
                      UpdateExpression="SET doc.vals[1] = :s ADD #n :one DELETE labels :old",
                      ConditionExpression="size(doc.vals) = :two AND contains(labels, :tag)",
                      ExpressionAttributeNames={"#n": "counter"},
                      ExpressionAttributeValues={":one": {"N": "1"}, ":s": {"S": "new"}, ":two": {"N": "2"},
                                                 ":old": {"SS": ["old"]}, ":tag": {"S": "old"}})
        assert result["Attributes"]["labels"] == {"SS": ["keep"]}
        assert result["Attributes"]["doc"]["M"]["vals"]["L"][1] == {"S": "new"}

        def increment(_):
            return call("UpdateItem", TableName=table, Key=keys[0],
                        AttributeUpdates={"counter": {"Action": "ADD", "Value": {"N": "0.1"}}})

        with ThreadPoolExecutor(3) as pool:
            list(pool.map(increment, range(3)))
        res = call("BatchGetItem", RequestItems={table: {"Keys": [keys[0]], "ConsistentRead": True}})
        assert res["Responses"][table][0]["counter"] == {"N": "1.3"}
        segments = [call("Scan", TableName=table, Segment=i, TotalSegments=3)["Items"] for i in range(3)]
        assert sorted(row["sk"]["N"] for segment in segments for row in segment) == sorted(k["sk"]["N"] for k in keys)
    finally:
        call("DeleteTable", TableName=table)


if __name__ == "__main__":
    exercise()
