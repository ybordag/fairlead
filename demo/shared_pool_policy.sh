# Shared GPU-free demo pool policy.
#
# Source this file from local demo scripts before starting Fairlead. The policy
# intentionally mentions every known workload so demos can run with
# STRICT_WORKLOAD_POOLS=true.

POOLS_JSON='["local-llm", "vision", "batch"]'
WORKLOAD_POOLS_JSON='{
  "chat_completions": ["local-llm"],
  "embeddings": ["local-llm"],
  "vision_analysis": ["vision"],
  "embed_batch": ["batch", "vision"],
  "index_build": ["batch"],
  "cluster": ["batch"]
}'
STRICT_WORKLOAD_POOLS=true
STRICT_WORKER_POOLS=true
