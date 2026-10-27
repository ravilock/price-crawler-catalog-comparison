# catalog-comparison: local development

This repository owns its Compose environment and local tooling. No sibling checkout is
required. The default dependency inventory is **rabbitmq, catalog-comparison-db**. The `app` profile
builds this checkout. Broker, database and Keycloak data use project-scoped named volumes.

## Prerequisites and settings

Install Docker Engine with Compose v2.24.4+ (including `!reset` support for acceptance),
Make and Python 3. Host Go commands require the version in `go.mod`; Node services require
Node 22 and their declared npm packages. Renderer workflows also require
`npx playwright install --with-deps chromium` on the host. Authenticate Docker to GHCR if
your selected packages are private; registry tokens stay in Docker's credential store.
Copy `.env.example` to `.env` and replace local password placeholders. Use URL-safe local
password characters or percent-encode credentials in connection URLs. Keep secrets out of
Git and logs. These local credentials are externally supplied and are not demo user accounts.

For each setting: Make command-line > exported shell > selected `ENV_FILE` > Compose default.
Empty dependency tags and owners are errors. `GHCR_OWNER` defaults in the example to
`ravilock`; project image references themselves require a nonempty owner. Override a sibling
with its `<SERVICE>_IMAGE_TAG`, such as `CRAWLER_IMAGE_TAG`. Normal `make config` prints
only service names and image references. Full `docker compose config` is a sensitive
operation and can print credentials. `COMPOSE_PROJECT_NAME` defaults to `price-catalog-comparison-local`;
use a distinct value and host ports for simultaneous environments.

## Run dependencies, then the checkout

```sh
cp .env.example .env
# Edit .env to supply local-only credentials and desired host ports.
make config
make deps.up
# Equivalently, after reviewing image access: docker compose up -d
make run
```

`make deps.up` pulls selected dependencies and stops on failure; it never substitutes a
cached image after a failed pull. The direct Compose command uses its normal pull policy;
explicitly pull first when freshness/availability verification is required.
Stateful host execution will require `make migrate.up` followed by `make migrate.version`
once its migration task is delivered; do not treat this startup scaffold as migrated.
For Node services, run `npm install` before host execution. `make run` maps database/broker
connections to the configured localhost ports, and binds the host process to `127.0.0.1`.

To build and run this checkout in a container:

```sh
make app.up
# Equivalent app-profile source build after dependency pull:
docker compose --profile app up --build -d
```

Container processes use Compose names and internal ports; the host process uses localhost.
Default application host port is `19680`, AMQP is `19672`, management is `19673`,
and owned PostgreSQL is `19632` when present. Keycloak uses `19681` when selected;
a sibling user API uses `19682`. Ports bind only to loopback. Configure `APP_PORT`,
`RABBITMQ_PORT`, `RABBITMQ_MANAGEMENT_PORT`, `POSTGRES_PORT`, `KEYCLOAK_PORT`, and
`USER_API_PORT` in `.env`; sibling database port overrides are named in Compose.

The browser-visible issuer is `OIDC_BASE_URL/realms/price-intelligence`. User API verifies
that same issuer and fetches keys through `http://keycloak:8080` inside Compose; host
`make run` maps this key URL to the published Keycloak port. If browser origins change,
update this repository's `local/keycloak/price-intelligence-realm.json` redirect/web origins
before importing a fresh realm, or update the retained realm through the local admin UI.
An existing realm import does not overwrite retained Keycloak settings.

## Optional workflows

Enable profiles only for the workflow being exercised, together with the checkout's `app`
profile for container execution. Their complete additional dependency closure is:

| Profile | Additional dependencies |
|---|---|
| collection | crawler, crawler-db |
| browser | user-api, keycloak |

```sh
# Example: select one profile listed above, without requiring sibling source.
make deps.up WORKFLOW_PROFILES="collection"
make app.up WORKFLOW_PROFILES="collection"
# Or docker compose --profile collection --profile app up --build -d
```

The controlled mock-source image/profile is delivered separately by T120 after its real
publication; no static fixture or invented image tag is substituted here. Dependency-only
startup is distinct from a working collection, browser, monitoring or demo workflow.

## Image inventory and compatibility

Official providers: Docker Official Images for `postgres:16-alpine` (PostgreSQL) and
`rabbitmq:3.13-management-alpine` (RabbitMQ); Keycloak's official
`quay.io/keycloak/keycloak:26.1.4` when identity is selected. Only the dependencies present
in this Compose file are started. These explicit infrastructure tags were checked against
the registry on 2026-10-27. Project sibling defaults, also verified available on that date:

- `ghcr.io/ravilock/price-crawler-crawler:31c8ff5c57be3921c41b9d7662b498623199861c`
- `ghcr.io/ravilock/price-crawler-user-api:06b665889d7087c14eca709497edb57c046f3a26`

These are published baseline commits using v1 messaging/configuration conventions. No
integrated readiness/business-workflow validation is claimed for this selection. `make`
startup warns that compatibility has not been established. An override is permitted, but
must remain a published explicit version or commit tag; changing a tag does not establish
content compatibility. Missing tags fail visibly. Review and record actual image content
and acceptance evidence before claiming compatibility.

## Source changes, verification and shutdown

For a reversible source-only check, change one startup log message in
`cmd/catalog-comparison/main.go`
(keep behavior unchanged).
Stop the host process with Ctrl-C and run `make run` again to observe the new message
in the terminal. For a container, run
`docker compose --profile app up --build -d app` and inspect `docker compose logs app`
. Revert the source edit and repeat.
Go/Node container assets rebuild in the Dockerfile; the frontend host dev server rebuilds
assets automatically. No migration is needed for this log/title edit.
Run the repository's `make test lint build` checks before publishing changes.

```sh
make down
# Or docker compose --profile '*' down
```

Normal shutdown retains volumes. Back up any retained data before an explicit reset,
verify the selected project with `docker compose ls`, then deliberately select
`docker compose --profile '*' down --volumes` only for that disposable project.
Never reset automatically after startup or migration errors. Dirty-version recovery must
stop the owner, inspect its version/error and actual schema, back up data, and repair or
restore the schema before an operator explicitly corrects the version and retries; no
automatic rollback or forced version is permitted.

## Delivery status

## Owned migrations — T075

The catalog image packages its versioned SQL and the pinned
`migrate/migrate:v4.18.3` PostgreSQL CLI. `make migrate.up` applies pending schema changes;
`make migrate.version` reports the version. The app profile waits for the health-gated,
one-off migration job to succeed. A failed or dirty migration prevents the app from
starting. Neither command rolls back or forces a version.

Existing databases created from the former `001_message_delivery.sql` are adopted by
reapplying its idempotent table/index definitions, which preserves outbox and inbox rows
while recording migration version 1. Back up the volume and compare the existing tables
and index with `migrations/001_message_delivery.up.sql` before adoption. If the schema
differs, repair it manually first; do not drop data or force the migration version.
Down migrations are only for explicitly planned destructive rollback.

Infrastructure readiness is gated by authenticated PostgreSQL `SELECT 1`, RabbitMQ health,
and Keycloak realm discovery with the expected issuer where selected. Application
connection handling, migration gating and dependency-loss readiness are delivered by
T073–T076/T085–T086/T105–T109. Current baseline worker images may exit until executable
wiring is implemented. This guide does not claim those future acceptance checks passed.

## Verification — 2026-10-27

`make test build` and Compose resolution passed after adding T075. Fresh migration reached
version 1. A retained-schema adoption check seeded one outbox and one inbox row, removed
only migration metadata, reapplied migration 1, and confirmed both rows remained with a
clean version 1. The isolated test project and volume were removed afterward.

`make test lint build`, Compose resolution and the dependency-image inventory check
passed. The current test target contains no service behavior tests yet. Minimum
dependencies passed real `docker compose up --wait` in isolated project `local-check-catalog-comparison-31ed33eb`,
with host ports removed; normal shutdown retained its named volumes. This verifies
dependency startup, not application, migration, demo or business-workflow readiness.
