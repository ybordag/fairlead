# Fixture and Local Config Examples

Fairlead tests should use generic node names and loopback/mock services. Avoid
committing real hostnames, usernames, home-directory paths, API tokens, private
model paths, or provider keys.

Use these conventions in committed examples:

- Node IDs: `spark-a`, `spark-b`, `node-a`, `node-b`.
- Backend IDs: `spark-a-vllm`, `spark-b-vllm`.
- Local URLs: `http://127.0.0.1:<port>/v1` for tests, or
  `http://spark-a:8000/v1` / `http://spark-b:8000/v1` for generic two-node docs.
- Model names: public small models such as `qwen2.5-0.5b-instruct`.
- Secrets: environment variable names only, never values.

## Example BACKENDS_JSON

```json
[
  {
    "id": "spark-a-vllm",
    "url": "http://spark-a:8000/v1",
    "node_id": "spark-a",
    "pool": "local-llm",
    "workloads": ["chat_completions", "embeddings"]
  },
  {
    "id": "spark-b-vllm",
    "url": "http://spark-b:8000/v1",
    "node_id": "spark-b",
    "pool": "local-llm",
    "workloads": ["chat_completions", "embeddings"]
  }
]
```

## Private Local Configs

Private local configs should live in ignored paths, for example:

```text
.env
fairlead.local.env
config/local/backends.json
fixtures/local/backends.json
fixtures/private/cluster.json
```

The repo's `.gitignore` excludes those paths. If a local config would be useful
to document, add a sanitized example to docs instead of committing the real
file.

## Test Fixtures

Inline test fixtures should stay generic:

```rust
healthy_on_node("http://node-a:8000/v1", "node-a")
healthy_on_node("http://node-b:8000/v1", "node-b")
```

For integration tests that need real machines, prefer reading hostnames from
environment variables and skipping the test when they are absent:

```text
FAIRLEAD_TEST_NODE_A_URL=http://spark-a:8000/v1
FAIRLEAD_TEST_NODE_B_URL=http://spark-b:8000/v1
```

Those variables should be set in a private `.env` or shell profile, not checked
into git.
