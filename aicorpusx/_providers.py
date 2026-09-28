"""OpenAI-compatible chat-completions adapter.

Pass ``base_url``, ``apis`` (API keys), and ``model`` for any service that
speaks the OpenAI ``/chat/completions`` protocol. Custom backends can still
implement ``translate(...)`` and be passed as ``provider``.
"""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Union


class ProviderError(RuntimeError):
    """A sanitized provider failure used by the scheduler."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        retryable: bool = False,
        disable_api: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable
        self.disable_api = disable_api


class TranslationProvider(ABC):
    """Interface implemented by translation providers."""

    @abstractmethod
    def translate(
        self,
        *,
        api_key: str,
        text: str,
        source_language: str,
        target_language: str,
        mode: str,
        glossary_terms: Iterable[Mapping[str, str]],
        glossary_mode: str,
    ) -> str:
        """Translate one item and return only the translated text."""


class OpenAICompatibleProvider(TranslationProvider):
    """Any OpenAI-compatible ``/chat/completions`` endpoint."""

    def __init__(
        self,
        *,
        model: str,
        base_url: str,
        timeout: float = 60.0,
        **options: Any,
    ) -> None:
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.options = dict(options)

    @property
    def endpoint(self) -> str:
        if self.base_url.endswith("/chat/completions"):
            return self.base_url
        return f"{self.base_url}/chat/completions"

    def translate(
        self,
        *,
        api_key: str,
        text: str,
        source_language: str,
        target_language: str,
        mode: str,
        glossary_terms: Iterable[Mapping[str, str]],
        glossary_mode: str,
    ) -> str:
        constraints = list(glossary_terms)
        system = (
            "You are a professional translation engine. Return only the translated "
            "text, without commentary, labels, markdown, or quotation marks. Preserve "
            "meaning, formatting, and line breaks."
        )
        user_lines = [
            f"Source language: {source_language}",
            f"Target language: {target_language}",
            f"Content mode: {mode}",
        ]
        if constraints and glossary_mode != "off":
            strength = "MUST" if glossary_mode == "strict" else "should"
            user_lines.append(
                "Use these terminology mappings; "
                f"each target form {strength} be preserved:"
            )
            for item in constraints:
                user_lines.append(f"- {item['source']} => {item['target']}")
        user_lines.extend(["", "Text:", text])

        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": "\n".join(user_lines)},
            ],
            "temperature": 0,
            "stream": False,
        }
        payload.update(self.options)
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            self.endpoint,
            data=body,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                data = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            if status in (401, 403):
                raise ProviderError(
                    f"Provider rejected an API credential (HTTP {status}).",
                    status_code=status,
                    disable_api=True,
                ) from None
            retryable = status in (408, 429) or 500 <= status <= 599
            raise ProviderError(
                f"Provider request failed (HTTP {status}).",
                status_code=status,
                retryable=retryable,
            ) from None
        except (urllib.error.URLError, TimeoutError, socket.timeout, ConnectionError) as exc:
            raise ProviderError(
                f"Provider connection failed ({type(exc).__name__}).",
                retryable=True,
            ) from None
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ProviderError("Provider returned invalid JSON.", retryable=True) from None

        try:
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError):
            raise ProviderError(
                "Provider response did not contain translated text.",
                retryable=True,
            ) from None
        if not isinstance(content, str) or not content.strip():
            raise ProviderError("Provider returned an empty translation.", retryable=True)
        return content.strip()


ProviderFactory = Callable[..., TranslationProvider]
_PROVIDERS: Dict[str, ProviderFactory] = {
    "openai": OpenAICompatibleProvider,
    "openai-compatible": OpenAICompatibleProvider,
}


def register_provider(name: str, factory: ProviderFactory) -> None:
    """Register a provider factory for internal or advanced integrations."""

    clean_name = str(name).strip().lower()
    if not clean_name:
        raise ValueError("Provider name cannot be empty.")
    _PROVIDERS[clean_name] = factory


def make_provider(
    provider: Optional[Union[str, TranslationProvider]],
    *,
    model: str,
    base_url: str,
    timeout: float,
    options: Optional[Mapping[str, Any]],
) -> TranslationProvider:
    if provider is None:
        factory = OpenAICompatibleProvider
    elif not isinstance(provider, str):
        if not callable(getattr(provider, "translate", None)):
            raise TypeError("provider must be None or an object with translate().")
        return provider
    else:
        name = provider.strip().lower()
        try:
            factory = _PROVIDERS[name]
        except KeyError:
            available = ", ".join(sorted(_PROVIDERS))
            raise ValueError(
                f"Unknown provider {provider!r}. Available providers: {available}."
            ) from None
    kwargs: Dict[str, Any] = {
        "model": model,
        "base_url": base_url,
        "timeout": timeout,
    }
    kwargs.update(dict(options or {}))
    return factory(**kwargs)
