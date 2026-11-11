"""Authorization is evaluated before retrieval, including derived facts."""
from .models import Principal, Source, ScopeViolation


def can_read(source: Source, principal: Principal) -> bool:
    if not source.enabled or source.tenant_id != principal.tenant_id:
        return False
    identities = {f"user:{principal.user_id}", *(f"group:{g}" for g in principal.groups)}
    allowed = source.owner_user_id == principal.user_id or bool(identities.intersection(source.readers))
    # Owner privileges do not allow exposing a private source in a group reply.
    if principal.output_chat_id is not None:
        allowed = allowed and principal.output_chat_id in source.output_chats
    return allowed


def require_operator(source: Source, principal: Principal) -> None:
    if source.tenant_id != principal.tenant_id or source.owner_user_id != principal.user_id:
        raise ScopeViolation("Only the registered source owner can submit an offline record")
    if not source.enabled or principal.output_chat_id is not None:
        raise ScopeViolation("Offline records require an enabled private owner context")
