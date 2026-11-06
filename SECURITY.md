# Security policy

TiKronos is experimental. Security fixes target the current `main` branch; older snapshots have no maintenance guarantee.

Report vulnerabilities through [GitHub private vulnerability reporting](https://github.com/infocusmodereal/tikronos/security/advisories/new). Include the affected commit, impact and a minimal reproduction. Do not publish credentials, private data or exploit details in a public issue. Response times are best effort.

Keep PD and TiKV on trusted networks, configure unique API credentials and terminate TLS before exposing the HTTP service remotely. The supplied Compose credentials are public development values. SigV4 authentication does not implement AWS IAM policy authorization.
