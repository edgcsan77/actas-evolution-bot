import asyncio
import os
import threading
import time

import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response


UPSTREAM = os.getenv(
    "EVOLUTION_GATEWAY_UPSTREAM",
    "http://127.0.0.1:8080",
).rstrip("/")

EXPECTED_API_KEY = os.getenv(
    "EVOLUTION_API_KEY",
    "",
).strip()

if not EXPECTED_API_KEY:
    raise RuntimeError("EVOLUTION_API_KEY is required")


ACK_CONCURRENT = int(
    os.getenv("EVOLUTION_GATEWAY_ACK_CONCURRENT", "1")
)

NORMAL_CONCURRENT = int(
    os.getenv("EVOLUTION_GATEWAY_NORMAL_CONCURRENT", "2")
)

MEDIA_CONCURRENT = int(
    os.getenv("EVOLUTION_GATEWAY_MEDIA_CONCURRENT", "1")
)

QUEUE_WAIT_SECONDS = float(
    os.getenv("EVOLUTION_GATEWAY_QUEUE_WAIT_SECONDS", "8")
)

ACK_QUEUE_WAIT_SECONDS = float(
    os.getenv("EVOLUTION_GATEWAY_ACK_QUEUE_WAIT_SECONDS", "20")
)


ACK_SEM = asyncio.Semaphore(ACK_CONCURRENT)
NORMAL_SEM = asyncio.Semaphore(NORMAL_CONCURRENT)
MEDIA_SEM = asyncio.Semaphore(MEDIA_CONCURRENT)
STATE_SEM = asyncio.Semaphore(1)

_thread_local = threading.local()

_waiting = 0
_active = 0


REQUEST_SKIP_HEADERS = {
    "host",
    "content-length",
    "connection",
    "transfer-encoding",
    "accept-encoding",
    "x-actas-priority",
}

RESPONSE_SKIP_HEADERS = {
    "content-length",
    "connection",
    "transfer-encoding",
    "content-encoding",
}


class GatewayBusy(Exception):
    pass


class ClientGone(Exception):
    pass


app = FastAPI(
    title="ACTAS Evolution Gateway",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


def _session():
    session = getattr(_thread_local, "session", None)

    if session is None:
        session = requests.Session()
        session.trust_env = False
        _thread_local.session = session

    return session


def _upstream_timeout(path: str):
    if path.startswith("message/sendMedia/"):
        return (3.0, 75.0)

    if path.startswith("message/sendText/"):
        return (3.0, 25.0)

    if path.startswith("instance/connectionState/"):
        return (3.0, 10.0)

    return (3.0, 30.0)


def _forward(
    method,
    path,
    query_items,
    headers,
    body,
):
    started = time.monotonic()

    response = _session().request(
        method=method,
        url=f"{UPSTREAM}/{path}",
        params=query_items,
        headers=headers,
        data=body,
        timeout=_upstream_timeout(path),
        allow_redirects=False,
    )

    elapsed = time.monotonic() - started

    response_headers = {
        key: value
        for key, value in response.headers.items()
        if key.lower() not in RESPONSE_SKIP_HEADERS
    }

    return (
        response.status_code,
        response.content,
        response_headers,
        elapsed,
    )


async def _acquire_one(
    sem,
    request,
    deadline,
):
    while True:
        if await request.is_disconnected():
            raise ClientGone()

        remaining = deadline - time.monotonic()

        if remaining <= 0:
            raise GatewayBusy()

        try:
            await asyncio.wait_for(
                sem.acquire(),
                timeout=min(0.5, remaining),
            )
            return

        except asyncio.TimeoutError:
            continue


async def _execute(
    request,
    method,
    path,
    query_items,
    headers,
    body,
    semaphores,
    lane,
):
    global _waiting, _active

    _waiting += 1
    queued_at = time.monotonic()
    acquired = []

    try:
        queue_wait_seconds = (
            ACK_QUEUE_WAIT_SECONDS
            if lane == "ack"
            else QUEUE_WAIT_SECONDS
        )

        deadline = (
            time.monotonic()
            + queue_wait_seconds
        )

        for sem in semaphores:
            await _acquire_one(
                sem,
                request,
                deadline,
            )
            acquired.append(sem)

        if await request.is_disconnected():
            raise ClientGone()

        wait_seconds = (
            time.monotonic()
            - queued_at
        )

        _waiting -= 1
        _active += 1

        try:
            result = await asyncio.to_thread(
                _forward,
                method,
                path,
                query_items,
                headers,
                body,
            )
        finally:
            _active -= 1

        return wait_seconds, result

    except Exception:
        if _waiting > 0:
            _waiting -= 1
        raise

    finally:
        for sem in reversed(acquired):
            sem.release()


async def _dispatch(
    request,
    method,
    path,
    query_items,
    headers,
    body,
    priority,
):
    if priority == "ack":
        return await _execute(
            request,
            method,
            path,
            query_items,
            headers,
            body,
            [ACK_SEM],
            "ack",
        )

    if path.startswith("message/sendMedia/"):
        return await _execute(
            request,
            method,
            path,
            query_items,
            headers,
            body,
            [MEDIA_SEM, NORMAL_SEM],
            "media",
        )

    if path.startswith("instance/connectionState/"):
        return await _execute(
            request,
            method,
            path,
            query_items,
            headers,
            body,
            [STATE_SEM, NORMAL_SEM],
            "state",
        )

    return await _execute(
        request,
        method,
        path,
        query_items,
        headers,
        body,
        [NORMAL_SEM],
        "normal",
    )


@app.get("/_actas_gateway/health")
async def health():
    return {
        "ok": True,
        "upstream": UPSTREAM,
        "ack_concurrent": ACK_CONCURRENT,
        "normal_concurrent": NORMAL_CONCURRENT,
        "media_concurrent": MEDIA_CONCURRENT,
        "queue_wait_seconds": QUEUE_WAIT_SECONDS,
        "ack_queue_wait_seconds": ACK_QUEUE_WAIT_SECONDS,
        "max_possible_active":
            ACK_CONCURRENT + NORMAL_CONCURRENT,
        "active": _active,
        "waiting": _waiting,
    }


@app.api_route(
    "/{path:path}",
    methods=[
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "OPTIONS",
    ],
)
async def proxy(path: str, request: Request):
    priority = request.headers.get(
        "x-actas-priority",
        "",
    ).strip().lower()

    if (
        request.headers.get("apikey", "")
        != EXPECTED_API_KEY
    ):
        return JSONResponse(
            status_code=401,
            content={"error": "Unauthorized"},
        )

    body = await request.body()

    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in REQUEST_SKIP_HEADERS
    }

    headers["Accept-Encoding"] = "identity"

    query_items = list(
        request.query_params.multi_items()
    )

    try:
        wait_seconds, result = await _dispatch(
            request,
            request.method,
            path,
            query_items,
            headers,
            body,
            priority,
        )

        status_code, content, response_headers, upstream_seconds = result

        response_headers["X-Actas-Gateway-Wait"] = (
            f"{wait_seconds:.3f}"
        )
        response_headers["X-Actas-Gateway-Upstream"] = (
            f"{upstream_seconds:.3f}"
        )

        print(
            "EVOLUTION_GATEWAY =",
            {
                "priority": priority or "normal",
                "method": request.method,
                "path": path,
                "wait": round(wait_seconds, 3),
                "upstream": round(upstream_seconds, 3),
                "status": status_code,
                "active": _active,
                "waiting": _waiting,
            },
            flush=True,
        )

        return Response(
            content=content,
            status_code=status_code,
            headers=response_headers,
        )

    except ClientGone:
        print(
            "EVOLUTION_GATEWAY_CLIENT_GONE =",
            {"path": path},
            flush=True,
        )

        return Response(status_code=499)

    except GatewayBusy:
        print(
            "EVOLUTION_GATEWAY_BUSY =",
            {
                "priority": priority or "normal",
                "path": path,
                "waiting": _waiting,
            },
            flush=True,
        )

        return JSONResponse(
            status_code=503,
            headers={"Retry-After": "2"},
            content={
                "error": "EVOLUTION_GATEWAY_BUSY",
            },
        )

    except requests.Timeout:
        return JSONResponse(
            status_code=504,
            content={
                "error": "EVOLUTION_UPSTREAM_TIMEOUT",
            },
        )

    except requests.RequestException as exc:
        print(
            "EVOLUTION_GATEWAY_ERROR =",
            {
                "path": path,
                "error": str(exc),
            },
            flush=True,
        )

        return JSONResponse(
            status_code=502,
            content={
                "error": "EVOLUTION_UPSTREAM_ERROR",
            },
        )
