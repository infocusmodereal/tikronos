"""Smoke the documented Compose endpoint with independently signed SDK-compatible requests."""
import hashlib
import hmac
import http.client
import json
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4


def request(operation, payload, duplicates=False):
    body = json.dumps(payload).encode()
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    day = now[:8]
    scope = f"{day}/us-east-1/dynamodb/aws4_request"
    names = "host;x-amz-date;x-amz-target" + (";x-test" if duplicates else "")
    target = "DynamoDB_20120810." + operation
    extra = "x-test:a b,c\n" if duplicates else ""
    canonical = (f"POST\n/\n\nhost:localhost:8000\nx-amz-date:{now}\nx-amz-target:{target}\n{extra}\n"
                 f"{names}\n{hashlib.sha256(body).hexdigest()}")
    message = f"AWS4-HMAC-SHA256\n{now}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
    key = b"AWS4local-development-only"
    for part in scope.split("/"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, message.encode(), hashlib.sha256).hexdigest()
    headers = {"Content-Type": "application/x-amz-json-1.0", "X-Amz-Date": now, "X-Amz-Target": target,
               "Authorization": f"AWS4-HMAC-SHA256 Credential=local/{scope}, SignedHeaders={names}, Signature={signature}"}
    if duplicates:
        connection = http.client.HTTPConnection("localhost", 8000, timeout=5)
        try:
            connection.putrequest("POST", "/")
            for name, value in headers.items():
                connection.putheader(name, value)
            connection.putheader("X-Test", "  a  b  ")
            connection.putheader("x-test", "c")
            connection.putheader("Transfer-Encoding", "chunked")
            connection.endheaders()
            for chunk in (body[:1], body[1:]):
                connection.send(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
            connection.send(b"0\r\n\r\n")
            response = connection.getresponse()
            result = json.load(response)
            assert response.status == 200, result
            return result
        finally:
            connection.close()
    with urlopen(Request("http://localhost:8000/", body, headers), timeout=5) as response:
        return json.load(response)


def main():
    deadline = time.monotonic() + 120
    while True:
        try:
            request("ListTables", {})
            break
        except HTTPError as error:
            if error.code < 500:
                raise
            last_error = f"HTTP {error.code}: {error.read().decode()}"
        except (URLError, TimeoutError, ConnectionError) as error:
            last_error = str(error)
        assert time.monotonic() < deadline, f"Compose storage did not become ready: {last_error}"
        time.sleep(1)
    request("ListTables", {}, duplicates=True)
    name = "Container" + uuid4().hex
    request("CreateTable", {"TableName": name, "BillingMode": "PAY_PER_REQUEST",
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}]})
    try:
        item = {"id": {"S": "smoke"}, "value": {"N": "21"}}
        request("PutItem", {"TableName": name, "Item": item})
        result = request("GetItem", {"TableName": name, "Key": {"id": item["id"]}, "ConsistentRead": True})
        assert result["Item"] == item
        try:
            urlopen(Request("http://localhost:8000/", b"{}",
                            {"X-Amz-Target": "DynamoDB_20120810.ListTables"}), timeout=5)
        except HTTPError as error:
            assert json.load(error)["__type"] == "AccessDeniedException"
        else:
            raise AssertionError("Container allowed an unsigned request")
    finally:
        request("DeleteTable", {"TableName": name})
    print("PASS Compose signed CRUD and unsigned-request rejection", flush=True)


if __name__ == "__main__":
    main()
