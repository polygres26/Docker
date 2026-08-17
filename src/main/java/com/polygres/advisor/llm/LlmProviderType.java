package com.polygres.advisor.llm;

/**
 * BUILTIN: the server's own Claude access (ANTHROPIC_API_KEY env var) -- no per-user key needed,
 * "just works" out of the box. EXTERNAL: an OpenAI-compatible endpoint (OpenAI itself, Azure
 * OpenAI, a local Ollama/vLLM server, any other provider that speaks the chat-completions shape)
 * with a user-supplied API key and base URL.
 */
public enum LlmProviderType {
    BUILTIN, EXTERNAL
}
