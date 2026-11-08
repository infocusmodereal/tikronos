# Quickstart

You need Git, Docker with Compose, and the AWS CLI for the examples. Docker builds the Java application, so a host JDK is optional.

## Start the development stack

```sh
git clone https://github.com/infocusmodereal/tikronos.git
cd tikronos
docker compose up --build -d
```

This starts one API instance, one PD instance and one TiKV instance. Only the API is published, at `127.0.0.1:8000`. This single-node stack is for development.

Configure a shell for the example credentials. These are deliberately public development credentials; do not use them for a remote service.

```sh
export AWS_ACCESS_KEY_ID=local
export AWS_SECRET_ACCESS_KEY=local-development-only
export AWS_DEFAULT_REGION=us-east-1
unset AWS_SESSION_TOKEN
export AWS_PAGER=""
```

Check storage readiness with a signed request. A freshly started TiKV may need a moment; retry if storage is still starting.

```sh
aws dynamodb list-tables --endpoint-url http://localhost:8000
```

## Create and read an item

```sh
aws dynamodb create-table --endpoint-url http://localhost:8000 \
  --table-name Example \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

aws dynamodb put-item --endpoint-url http://localhost:8000 \
  --table-name Example \
  --item '{"pk":{"S":"hello"},"message":{"S":"from TiKV"}}'

aws dynamodb get-item --endpoint-url http://localhost:8000 \
  --table-name Example --key '{"pk":{"S":"hello"}}' --consistent-read
```

Use the same endpoint, region and credentials in your SDK. API compatibility is described in the [compatibility guide](compatibility.md).

## Stop and inspect

```sh
docker compose logs --tail 100 api
docker compose down
```

Named volumes retain data across restarts. Adding `--volumes` to `down` permanently deletes this development stack's data.

## Terraform locking

For Terraform versions that still support DynamoDB locking, create a table with a string partition key named `LockID`, then point the S3 backend's DynamoDB endpoint at TiKronos. State remains in object storage; TiKronos holds the lock and consistency records. Object storage and its credentials are configured independently.

DynamoDB-based locking is deprecated in Terraform. Check the [S3 backend documentation](https://developer.hashicorp.com/terraform/language/backend/s3) for your Terraform version before adopting this path.
