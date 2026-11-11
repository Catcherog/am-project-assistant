"""Host-configured transports for AM gateway R2 workers."""
from .base import AdapterError
from .callable import CallableAdapter
from .http import HTTPJSONAdapter, GrokBotAdapter
from .subprocess import SubprocessJsonAdapter, GrokCLIAdapter, CodexCLIAdapter
from .ollama import OllamaChatAdapter, QwenOllamaAdapter

__all__ = [
    "AdapterError", "CallableAdapter", "HTTPJSONAdapter", "GrokBotAdapter",
    "SubprocessJsonAdapter", "GrokCLIAdapter", "CodexCLIAdapter",
    "OllamaChatAdapter", "QwenOllamaAdapter",
]
