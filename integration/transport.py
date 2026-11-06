"""Verify wire behavior changed by the HTTP engine, without changing DynamoDB fixtures."""
import http.client
import json
import socket
import time


def request(method, path, body=None, headers=None, chunked=False):
    connection = http.client.HTTPConnection("127.0.0.1", 8000, timeout=5)
    try:
        connection.request(method, path, body, headers or {}, encode_chunked=chunked)
        response = connection.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        connection.close()


def exercise():
    headers = {"Content-Type": "application/x-amz-json-1.0",
               "X-Amz-Target": "DynamoDB_20120810.ListTables"}
    with socket.create_connection(("127.0.0.1", 8000), timeout=5) as head:
        head.sendall(b"HEAD /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        wire_headers, _, wire_body = head.makefile("rb").read().partition(b"\r\n\r\n")
        assert wire_headers.startswith(b"HTTP/1.1 200") and wire_body == b""
    assert request("POST", "/healthz")[0] == 405
    assert request("GET", "/")[0] == 405
    for path in ("/%2f", "//", "/other"):
        assert request("POST", path, b"{}", headers)[0] == 404, path
    assert request("POST", "/?", b"{}", headers)[0] == 400
    status, response_headers, body = request("POST", "/", iter([b"{", b"}"]), headers, chunked=True)
    assert status == 200 and "TableNames" in json.loads(body)
    assert any(k.lower() == "x-amzn-requestid" for k in response_headers)
    assert "Date" in response_headers
    assert response_headers["Content-Type"] == "application/x-amz-json-1.0"
    status, _, body = request("POST", "/", iter([b" " * (16 * 1024 * 1024), b" "]), headers, chunked=True)
    assert status == 413 and json.loads(body)["__type"] == "RequestEntityTooLarge"

    # Eight stalled uploads still leave room for a complete request.
    held = []
    try:
        for index in range(16):
            conn = socket.create_connection(("127.0.0.1", 8000), timeout=5)
            held.append(conn)
            length = 16 * 1024 * 1024 if index < 2 else 100
            conn.sendall(f"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: {length}\r\n\r\nx".encode())
            if index == 7:
                assert request("POST", "/", b"{}", headers)[0] == 200
        deadline = time.monotonic() + 5
        while True:
            status, _, body = request("POST", "/", b"{}", headers)
            if status == 400 and json.loads(body)["__type"] == "ProvisionedThroughputExceededException":
                break
            assert time.monotonic() < deadline, "Admission was not bounded"
            time.sleep(.05)
        oversized = {**headers, "Content-Length": str(16 * 1024 * 1024 + 1)}
        assert request("POST", "/", headers=oversized)[0] == 413
        assert request("GET", "/healthz")[0] == 200
        response = http.client.HTTPResponse(held[0])
        response.begin()
        assert response.status == 408 and json.load(response)["__type"] == "RequestTimeout"
    finally:
        for conn in held:
            conn.close()
    deadline = time.monotonic() + 5
    while request("POST", "/", b"{}", headers)[0] != 200:
        assert time.monotonic() < deadline, "Disconnected clients leaked admission slots"
        time.sleep(.05)
    print("PASS raw paths, chunked body bounds, HEAD and bounded admission", flush=True)


def drain(process):
    connection = http.client.HTTPConnection("127.0.0.1", 8000, timeout=15)
    try:
        connection.putrequest("POST", "/")
        connection.putheader("X-Amz-Target", "DynamoDB_20120810.ListTables")
        connection.putheader("Content-Length", "2")
        connection.endheaders()
        connection.send(b"{")
        time.sleep(.2)
        process.terminate()
        deadline = time.monotonic() + 5
        while True:
            status, _, body = request("POST", "/", b"{}", {"X-Amz-Target": "DynamoDB_20120810.ListTables"})
            if status == 400 and json.loads(body)["__type"] == "ProvisionedThroughputExceededException":
                break
            assert time.monotonic() < deadline, "Server did not enter drain"
            time.sleep(.05)
        connection.send(b"}")
        response = connection.getresponse()
        assert response.status == 200 and "TableNames" in json.load(response)
        process.wait(timeout=15)
    finally:
        connection.close()
    print("PASS SIGTERM drains admitted requests before closing connections", flush=True)
