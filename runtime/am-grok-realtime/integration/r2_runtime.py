"""Install the R2 sidecar into the EXISTING consumer using trusted host ports."""
from dataclasses import dataclass
import importlib
from typing import Any, Callable

from am_gateway.core import Gateway
from am_gateway.models import Config, ContractError
from am_gateway.scheduler import Scheduler
from am_gateway.store import Store
from integration.am_multi_ai_host import BridgeIngress


@dataclass
class HostPorts:
    config: Config
    database_path: str
    adapters: dict
    sender: Any
    verified_scope: Callable
    legacy_control: Callable
    projects_for: Callable
    archive: Callable | None = None
    collectors: Any = None
    artifact_resolver: Callable | None = None
    collectors_factory: Callable | None = None


def install(bridge,host_config,client,root):
    settings=host_config.get("multi_ai_gateway",{})
    if not settings.get("enabled",False):
        return None,None
    if host_config.get("grok_realtime",{}).get("enabled",False):
        raise ContractError("select R2 or R1; never run two consumers of the same reply")
    factory=settings.get("host_factory","")
    if ":" not in factory:
        raise ContractError("set the actual host_factory module:function")
    module,attribute=factory.split(":",1)
    ports=getattr(importlib.import_module(module),attribute)(bridge,host_config,client,root)
    if settings.get('implementation') == 'phase1':
        from integration.phase1_runtime import install as install_phase1
        # Enabled in the real consumer means activation, which remains refused
        # until the actual Grok Bot ports have been implemented and verified.
        return install_phase1(ports, activate=True)
    if not isinstance(ports,HostPorts):
        raise ContractError("host factory must return HostPorts")
    gateway=Gateway(Store(ports.database_path),ports.config,ports.adapters,
                    artifact_resolver=ports.artifact_resolver)
    collectors=ports.collectors_factory(gateway) if ports.collectors_factory else ports.collectors
    scheduler=Scheduler(gateway,ports.sender,collectors=collectors)
    ingress=BridgeIngress(gateway,legacy_control=ports.legacy_control,verified_scope=ports.verified_scope,
                          projects_for=ports.projects_for,scheduler=scheduler,archive=ports.archive)
    scheduler.start()
    return ingress,scheduler
