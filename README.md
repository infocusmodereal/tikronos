# TiKronos

TiKronos aims to let applications use the DynamoDB API with TiKV as their storage engine. Its first compatibility target is conditional writes for state locking.

## Run locally

With JDK 17 installed, start the Kotlin/JVM service using the Gradle wrapper.

```sh
./gradlew run
```

Run `curl http://127.0.0.1:8000/healthz` to check the HTTP process. This initial scaffold returns HTTP 501 for DynamoDB requests. TiKV storage and locking are planned.

## First milestone

Support two Terraform clients contending for the same state lock on a preconfigured table with a string partition key named `LockID`. Implement the required DescribeTable, GetItem, PutItem and DeleteItem behavior, then verify against a real TiKV cluster that concurrent conditional acquisitions yield exactly one winner.

Terraform's S3 backend deprecates DynamoDB locking. Existing pipelines using that mode are the first compatibility target. The state file stays in object storage. See the [Terraform S3 backend documentation](https://developer.hashicorp.com/terraform/language/backend/s3).
