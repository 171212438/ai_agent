"""Dataclasses describing providers and resolved backends.

This module is the leaf of the package's dependency graph: it defines the two
value types the rest of the package builds on, and imports nothing from its
siblings.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

__all__ = ["Backend", "Provider"]


@dataclass(frozen=True)  # 根据类中声明的字段，自动生成初始化等方法。显式传入的字段使用传入值，未传入的字段使用定义中的默认值
class Provider:
    """模型服务商配置类：保存服务商名称、默认 API 地址、默认模型等信息，并提供读取 API Key、确定实际 API 地址的方法

    Attributes:
        name: Canonical provider name, e.g. ``"kimi"``.
        base_url: Default API endpoint, used when no override is set.
        default_model: Model id used when the caller does not pick one.
        key_vars: Environment variables holding the API key, tried in order.
            The first non-empty one wins; later entries exist for backwards
            compatibility.
        base_url_var: Environment variable overriding ``base_url``, for
            self-hosted or regional deployments. ``None`` if not overridable.
        requires_key: Whether a missing key is an error. Local runtimes such as
            Ollama accept any placeholder, so they set this to ``False``.
        namespaces_models: Whether this backend expects vendor-namespaced model
            ids such as ``openai/gpt-4o`` rather than bare ones. True for
            aggregators that resell many vendors' models; a bare id given to
            one of these is mapped before the request goes out.

            This describes *model-id formatting only*. It says nothing about
            which endpoint to call or whose credentials are valid -- an
            aggregator sharing OpenRouter's id format still has its own
            ``base_url`` and its own key, and is never routed through
            OpenRouter on that basis.
    """

    name: str  # 服务商的规范名称
    base_url: str  # 默认 API 地址
    default_model: str  # 默认模型名称
    key_vars: tuple[str, ...] = ()  # 保存 API Key 的环境变量名称
    base_url_var: str | None = None  # 用于覆盖默认 API 地址的环境变量名称
    requires_key: bool = True  # 标记该服务商是否需要凭证
    namespaces_models: bool = False  # 标记模型名是否采用“厂商/模型”这样的格式

    def api_key(self) -> str:
        """按顺序读取环境变量，返回第一个清理后非空的凭证

        Returns:
            The first non-empty value among ``key_vars``, stripped of
            surrounding whitespace, or ``""`` when none is set.
        """
        for var in self.key_vars:
            value = os.getenv(var, "").strip()
            if value:
                return value
        return ""

    def resolved_base_url(self) -> str:
        """优先返回环境变量中的非空地址，否则返回默认地址

        Returns:
            The value of ``base_url_var`` if that variable is set and non-empty,
            otherwise the built-in ``base_url``.
        """
        if self.base_url_var:
            return os.getenv(self.base_url_var, "").strip() or self.base_url
        return self.base_url


@dataclass(frozen=True)
class Backend:
    """保存已经确定好的模型调用配置:
    使用哪个 API 地址、哪个模型、哪个 API Key，以及是否通过 OpenRouter 调用

    Attributes:
        api_key: Credential for ``base_url``. Never empty -- local runtimes get
            a placeholder, because the OpenAI client rejects an empty key.
        base_url: The endpoint to send requests to.
        model: Model id valid at ``base_url``. Note this may differ from the
            requested id when the request was rerouted through OpenRouter.
        provider: The provider that was requested, after alias resolution.
        using_openrouter: Whether the request is going through OpenRouter
            rather than the provider's own API.
    """

    api_key: str
    base_url: str
    model: str
    provider: str
    using_openrouter: bool

    def __iter__(self):
        """Unpack as the 4-tuple the pre-registry chapter helpers returned.

        Returns:
            An iterator over ``api_key``, ``base_url``, ``model`` and
            ``using_openrouter``, in that order.
        """
        return iter((self.api_key, self.base_url, self.model, self.using_openrouter))
