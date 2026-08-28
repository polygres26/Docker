package com.nexagres.advisor.llm;

/**
 * BUILTIN: the server's own Claude access (ANTHROPIC_API_KEY env var) -- no per-user key needed.
 * LOCAL: a locally-managed llama-server sidecar (see {@link LocalLlamaManager}/{@link
 * LocalLlamaProcess}) running a model file the operator points at -- no API key, no network call
 * leaves the machine; this is the default for a fresh install (see {@link LlmSettings}) since it
 * works out of the box wherever llama-server + a model file are already available, same
 * "self-hosted first" posture Omnigate takes with its own local Qwen/Gemma sidecars. EXTERNAL: an
 * OpenAI-compatible endpoint (OpenAI itself, Azure OpenAI, a remote Ollama/vLLM server, etc.) with
 * a user-supplied API key and base URL.
 */
public enum LlmProviderType {
    BUILTIN, LOCAL, EXTERNAL
}
