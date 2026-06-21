# DGX Spark Deployment Notes

This document describes how to run Fairlead in front of vLLM on a small local
GPU cluster. The concrete test topology is two NVIDIA DGX Spark systems
connected over InfiniBand, but the same pattern applies to any pair of
OpenAI-compatible inference backends reachable over the network.

The deployment has three moving pieces:

```text
OpenAI-compatible client
  -> Fairlead on one cluster node
  -> vLLM on local DGX Spark
  -> vLLM on peer DGX Spark
```

Fairlead does not run inference. vLLM owns model loading, GPU execution, KV
cache, batching, and token generation. Fairlead owns the routing decision: which
backend should receive the request, whether a circuit is open, and whether the
request origin should prefer a same-node backend.

## What uv Is

`uv` is a Python environment and package manager. In this setup it replaces the
usual `python -m venv` plus `pip install` workflow.

It is useful for vLLM because it can:

- Create isolated Python environments.
- Install Python builds in user space when system packages are missing.
- Install vLLM and choose a compatible PyTorch/CUDA wheel set with
  `--torch-backend=auto`.

The user-space Python detail matters on DGX Spark systems where the system
Python interpreter may be present without development headers such as
`Python.h`. Triton and PyTorch can need those headers when compiling small CUDA
helpers during vLLM startup. If system headers are unavailable and installing
`python3-dev` is inconvenient, a `uv` managed Python can provide the headers
without modifying system Python.

## Install vLLM

Run this on each DGX Spark node.

```bash
python3 -m venv ~/venvs/fairlead-vllm-bootstrap
~/venvs/fairlead-vllm-bootstrap/bin/python -m pip install --upgrade pip uv
~/venvs/fairlead-vllm-bootstrap/bin/uv python install 3.12
~/venvs/fairlead-vllm-bootstrap/bin/uv venv --python 3.12 \
  ~/venvs/fairlead-vllm --seed
~/venvs/fairlead-vllm-bootstrap/bin/uv pip install \
  --python ~/venvs/fairlead-vllm/bin/python \
  vllm --torch-backend=auto
```

Verify that vLLM imports and that PyTorch can see the GPU:

```bash
~/venvs/fairlead-vllm/bin/python - <<'PY'
import torch
import vllm

print("vllm", vllm.__version__)
print("torch", torch.__version__)
print("cuda_available", torch.cuda.is_available())
print("device_count", torch.cuda.device_count())
if torch.cuda.is_available():
    print("device_name", torch.cuda.get_device_name(0))
    print("capability", torch.cuda.get_device_capability(0))
PY
```

## Start vLLM

Start one vLLM server on each DGX Spark node. Use a small model for initial
sanity checks so the test exercises routing before it exercises large-model
operations.

```bash
source ~/venvs/fairlead-vllm/bin/activate

vllm serve Qwen/Qwen2.5-0.5B-Instruct \
  --host 0.0.0.0 \
  --port 8000 \
  --served-model-name qwen2.5-0.5b-instruct \
  --gpu-memory-utilization 0.50 \
  --max-model-len 2048 \
  --max-num-seqs 2
```

For a durable local test, run the command under a process supervisor, terminal
multiplexer, or `nohup` with logs:

```bash
mkdir -p ~/logs
nohup bash -lc 'source ~/venvs/fairlead-vllm/bin/activate && \
  vllm serve Qwen/Qwen2.5-0.5B-Instruct \
    --host 0.0.0.0 \
    --port 8000 \
    --served-model-name qwen2.5-0.5b-instruct \
    --gpu-memory-utilization 0.50 \
    --max-model-len 2048 \
    --max-num-seqs 2' \
  > ~/logs/fairlead-vllm.log 2>&1 &
```

From the node that will run Fairlead, verify that both vLLM servers are
reachable:

```bash
curl http://localhost:8000/v1/models
curl http://spark-b:8000/v1/models
```

Replace `spark-b` with the peer DGX Spark hostname.

## Build Fairlead

Build Fairlead on the Linux/aarch64 DGX Spark node rather than copying a macOS
binary. Even when the laptop and DGX Spark systems are both arm64, the binary
target differs by operating system.

```bash
cd ~/Code/fairlead
source ~/.cargo/env
cargo build --release
```

The resulting binary is:

```text
target/release/fairlead
```

## Run Fairlead

Run Fairlead on one DGX Spark node and point it at both vLLM servers.

```bash
BACKENDS_JSON='[
  {
    "id": "spark-a-vllm",
    "url": "http://localhost:8000/v1",
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
]' \
PORT=7000 \
LOG_LEVEL=info \
./target/release/fairlead
```

Detached form:

```bash
mkdir -p ~/logs
nohup bash -lc 'BACKENDS_JSON='"'"'[
  {
    "id": "spark-a-vllm",
    "url": "http://localhost:8000/v1",
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
]'"'"' PORT=7000 LOG_LEVEL=info ./target/release/fairlead' \
  > ~/logs/fairlead-router.log 2>&1 &
```

Verify Fairlead:

```bash
curl http://localhost:7000/health
curl http://localhost:7000/metrics
```

The metrics output should show both backend circuits:

```text
fairlead_circuit_state{backend="spark-a-vllm",node="spark-a",pool="local-llm",...} 0
fairlead_circuit_state{backend="spark-b-vllm",node="spark-b",pool="local-llm",...} 0
```

## Test Origin-Aware Routing

Send a request that declares it originated on the peer node:

```bash
curl http://localhost:7000/v1/chat/completions \
  -H 'content-type: application/json' \
  -H 'X-Fairlead-Origin-Node: spark-b' \
  -H 'X-Fairlead-Thread-Id: demo-thread' \
  -d '{
    "model": "qwen2.5-0.5b-instruct",
    "messages": [{"role": "user", "content": "Say hello in one sentence."}],
    "max_tokens": 32,
    "temperature": 0
  }'
```

With both circuits closed, Fairlead should prefer the backend whose `node_id`
matches `X-Fairlead-Origin-Node`. The vLLM access logs on the peer node should
show the `POST /v1/chat/completions` request.

Then send the symmetric local-origin request:

```bash
curl http://localhost:7000/v1/chat/completions \
  -H 'content-type: application/json' \
  -H 'X-Fairlead-Origin-Node: spark-a' \
  -H 'X-Fairlead-Thread-Id: local-demo-thread' \
  -d '{
    "model": "qwen2.5-0.5b-instruct",
    "messages": [{"role": "user", "content": "Say hello in one sentence."}],
    "max_tokens": 32,
    "temperature": 0
  }'
```

The local vLLM access log should show this request.

## Test Circuit Fallback

Stop the peer vLLM process, then send a peer-origin request through Fairlead.

Fairlead records the peer backend failure and retries the next eligible backend
before returning to the caller, as long as the failure happens before response
bytes are streamed. With two DGX Sparks, this means a request that initially
selects the peer vLLM can fall back to the local vLLM in the same request.

Check metrics after the failure:

```bash
curl http://localhost:7000/metrics
```

The peer backend should show circuit state `2`, which means open:

```text
fairlead_circuit_state{backend="spark-b-vllm",node="spark-b",pool="local-llm",...} 2
```

Now send the same peer-origin request again. Fairlead should skip the
circuit-open peer backend and route directly to the local backend.

## Operational Notes

- `--host 0.0.0.0` is required if peer nodes need to reach the vLLM server.
- Use a small model first to separate routing bugs from model-load or memory
  issues.
- Keep vLLM and Fairlead logs separate; routing verification often comes from
  matching Fairlead responses to vLLM access logs.
- Fairlead probes OpenAI-compatible backends at `/v1/models` by default. Use
  `health_path` in `BACKENDS_JSON` for backends that expose health elsewhere,
  such as `/health`.
- Detached `nohup` processes are useful for manual tests. Production deployment
  should use systemd, Docker, k3s, or another supervisor.
