"""Exercise conditional writes through the HTTP API with a real TiKV store."""
import json
import io
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

TABLE = "Locks" + uuid4().hex
CLIENTS = 4


def request(operation, _target="DynamoDB_20120810.", **payload):
    req = Request("http://127.0.0.1:8000/", json.dumps(payload).encode(), headers={
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": _target + operation,
    })
    for attempt in range(8):
        try:
            with urlopen(req, timeout=30) as response:
                return json.load(response)
        except HTTPError as error:
            body = error.read()
            code = json.loads(body).get("__type")
            if code != "ProvisionedThroughputExceededException" or attempt == 7:
                raise HTTPError(error.url, error.code, error.msg, error.hdrs, io.BytesIO(body)) from error
            # This response guarantees a rejected prewrite. Never retry an ambiguous 500 here.
            print(f"Retrying throttled {operation}", file=sys.stderr)
            time.sleep(min(.05 * 2 ** attempt, .5))


def main():
    for attempt in range(60):
        try:
            request("ListTables")
            break
        except (URLError, TimeoutError):
            if attempt == 59:
                raise
            time.sleep(1)
    request("CreateTable", TableName=TABLE, BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}])
    try:
        barrier = Barrier(CLIENTS)
        key = {"id": {"S": "shared"}}

        def acquire(owner):
            barrier.wait(timeout=10)
            try:
                request("PutItem", TableName=TABLE, Item={**key, "owner": {"S": owner}},
                        ConditionExpression="attribute_not_exists(id)")
                return owner
            except HTTPError as exc:
                if exc.code != 400 or json.load(exc)["__type"] != "ConditionalCheckFailedException":
                    raise
                return None

        with ThreadPoolExecutor(CLIENTS) as pool:
            winners = [owner for owner in pool.map(acquire, map(str, range(CLIENTS))) if owner is not None]
        stored = request("GetItem", TableName=TABLE, Key=key, ConsistentRead=True)["Item"]["owner"]["S"]
        assert winners == [stored], winners
    finally:
        request("DeleteTable", TableName=TABLE)


if __name__ == "__main__":
    main()
