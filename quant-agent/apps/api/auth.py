from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from infra.config import env_flag, env_int, env_text

SESSION_COOKIE = "quant_session"
CSRF_COOKIE = "quant_csrf"
ROLE_LEVEL = {"read": 1, "approve": 2, "execute": 3}
SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}


@dataclass(frozen=True)
class AuthContext:
    role: str
    source: str
    csrf_token: str | None = None


class LoginRequest(BaseModel):
    password: str


router = APIRouter(tags=["auth"])


def _session_ttl_seconds() -> int:
    return env_int("QUANT_AGENT_SESSION_TTL_SECONDS", 28800, minimum=300)


def _passwords() -> dict[str, str]:
    access = env_text("QUANT_AGENT_ACCESS_PASSWORD")
    return {
        "read": env_text("QUANT_AGENT_READ_PASSWORD") or access,
        "approve": env_text("QUANT_AGENT_APPROVAL_PASSWORD") or access,
        "execute": env_text("QUANT_AGENT_EXECUTION_PASSWORD") or access,
    }


def auth_is_configured() -> bool:
    return any(_passwords().values())


def role_for_password(provided: str) -> str | None:
    if not provided:
        return None
    passwords = _passwords()
    for role in ("execute", "approve", "read"):
        expected = passwords[role]
        if expected and hmac.compare_digest(provided, expected):
            return role
    return None


def _signing_key() -> bytes:
    configured = env_text("QUANT_AGENT_AUTH_SIGNING_SECRET")
    if configured:
        return configured.encode("utf-8")
    material = "|".join(_passwords().values())
    return hashlib.sha256(f"quant-agent-session|{material}".encode("utf-8")).digest()


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64decode(raw: str) -> bytes:
    return base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))


def create_session(role: str) -> tuple[str, str, datetime]:
    if role not in ROLE_LEVEL:
        raise ValueError(f"Unsupported role: {role}")
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=_session_ttl_seconds())
    csrf_token = secrets.token_urlsafe(24)
    payload = {
        "role": role,
        "exp": int(expires_at.timestamp()),
        "csrf": csrf_token,
        "nonce": secrets.token_urlsafe(12),
    }
    encoded = _b64encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signature = _b64encode(hmac.new(_signing_key(), encoded.encode("ascii"), hashlib.sha256).digest())
    return f"{encoded}.{signature}", csrf_token, expires_at


def verify_session(token: str) -> AuthContext | None:
    try:
        encoded, provided_signature = token.split(".", 1)
        expected_signature = _b64encode(
            hmac.new(_signing_key(), encoded.encode("ascii"), hashlib.sha256).digest()
        )
        if not hmac.compare_digest(provided_signature, expected_signature):
            return None
        payload = json.loads(_b64decode(encoded))
        role = str(payload["role"])
        if role not in ROLE_LEVEL or int(payload["exp"]) <= int(datetime.now(timezone.utc).timestamp()):
            return None
        csrf_token = str(payload.get("csrf") or "") or None
        return AuthContext(role=role, source="cookie", csrf_token=csrf_token)
    except (ValueError, KeyError, TypeError, json.JSONDecodeError):
        return None


def authenticate_request(request: Request) -> AuthContext | None:
    password = request.headers.get("x-access-password")
    if password:
        role = role_for_password(password)
        return AuthContext(role=role, source="password_header") if role else None

    bearer = request.headers.get("authorization", "")
    if bearer.lower().startswith("bearer "):
        context = verify_session(bearer[7:].strip())
        if context is not None:
            return AuthContext(role=context.role, source="bearer")

    token = request.cookies.get(SESSION_COOKIE)
    return verify_session(token) if token else None


def required_role(method: str, path: str) -> str:
    if method.upper() in SAFE_METHODS:
        return "read"
    if path == "/paper-orders/risk-plan":
        return "read"
    if path.startswith("/recommendations/") and path.endswith("/approval"):
        return "approve"
    if path.startswith("/research/") or path.startswith("/backtests"):
        return "approve"
    if path.startswith("/source-snapshots/") and (path.endswith("/replay") or path.endswith("/compare")):
        return "approve"
    return "execute"


def role_allows(actual: str, required: str) -> bool:
    return ROLE_LEVEL.get(actual, 0) >= ROLE_LEVEL.get(required, 99)


def csrf_is_valid(request: Request, context: AuthContext) -> bool:
    if request.method.upper() in SAFE_METHODS or context.source != "cookie":
        return True
    header_token = request.headers.get("x-csrf-token")
    cookie_token = request.cookies.get(CSRF_COOKIE)
    return bool(
        header_token
        and cookie_token
        and context.csrf_token
        and hmac.compare_digest(header_token, cookie_token)
        and hmac.compare_digest(header_token, context.csrf_token)
    )


def _set_auth_cookies(response: JSONResponse, token: str, csrf_token: str) -> None:
    ttl = _session_ttl_seconds()
    secure = env_flag(
        "QUANT_AGENT_COOKIE_SECURE",
        default=not env_flag("QUANT_AGENT_TEST_MODE"),
    )
    response.set_cookie(
        SESSION_COOKIE,
        token,
        max_age=ttl,
        httponly=True,
        secure=secure,
        samesite="strict",
        path="/",
    )
    response.set_cookie(
        CSRF_COOKIE,
        csrf_token,
        max_age=ttl,
        httponly=False,
        secure=secure,
        samesite="strict",
        path="/",
    )


@router.post("/auth/login", include_in_schema=False)
def login(payload: LoginRequest) -> JSONResponse:
    role = role_for_password(payload.password)
    if role is None:
        return JSONResponse(status_code=401, content={"detail": "Invalid credentials"})
    token, csrf_token, expires_at = create_session(role)
    response = JSONResponse(
        content={"authenticated": True, "role": role, "expires_at": expires_at.isoformat()}
    )
    _set_auth_cookies(response, token, csrf_token)
    response.headers["Cache-Control"] = "no-store"
    return response


@router.post("/auth/logout", include_in_schema=False)
def logout() -> JSONResponse:
    response = JSONResponse(content={"authenticated": False})
    response.delete_cookie(SESSION_COOKIE, path="/")
    response.delete_cookie(CSRF_COOKIE, path="/")
    response.headers["Cache-Control"] = "no-store"
    return response


@router.get("/auth/session", include_in_schema=False)
def session_info(request: Request) -> dict:
    context = getattr(request.state, "auth", None)
    return {
        "authenticated": context is not None,
        "role": context.role if context is not None else None,
        "source": context.source if context is not None else None,
    }


@router.get("/login", response_class=HTMLResponse, include_in_schema=False)
def login_page(request: Request) -> str:
    next_path = request.query_params.get("next") or "/dashboard"
    if not next_path.startswith("/") or next_path.startswith("//"):
        next_path = "/dashboard"
    safe_next = quote(next_path, safe="/?:=&")
    return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Quant Agent 登录</title><style>
body{{margin:0;min-height:100vh;display:grid;place-items:center;background:#eef4f8;font-family:-apple-system,BlinkMacSystemFont,'PingFang SC',sans-serif;color:#153047}}
main{{width:min(420px,calc(100vw - 32px));background:white;border:1px solid #d8e2ea;border-radius:16px;padding:28px;box-shadow:0 18px 50px #17324d1c}}
h1{{margin:0 0 8px;font-size:24px}}p{{color:#60758a}}input,button{{width:100%;box-sizing:border-box;padding:12px;border-radius:9px;font-size:16px}}input{{border:1px solid #bdcad6}}button{{margin-top:12px;border:0;background:#0f766e;color:white;font-weight:700;cursor:pointer}}#status{{min-height:22px;color:#b42318;margin-top:10px}}
</style></head><body><main><h1>Quant Agent</h1><p>请输入访问密码。密码通过 POST 请求体发送，不写入 URL 或浏览器历史；生产环境请启用 HTTPS。</p>
<form id="login"><input id="password" type="password" autocomplete="current-password" required autofocus><button>登录</button></form><div id="status"></div></main>
<script>document.getElementById('login').addEventListener('submit',async(e)=>{{e.preventDefault();const status=document.getElementById('status');status.textContent='';const res=await fetch('/auth/login',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{password:document.getElementById('password').value}})}});if(res.ok){{location.replace('{safe_next}')}}else{{status.textContent='密码错误或认证未配置'}}}});</script></body></html>"""
