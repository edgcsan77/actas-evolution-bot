from app.queue import redis_conn
from app.services.provider16_sidea import (
    SIDEA_BASE_URL,
    SIDEA_HTTP_CONNECT_TIMEOUT,
    SIDEA_HTTP_READ_TIMEOUT,
    SideaPool,
    _sidea_html_is_authenticated,
    _sidea_safe_cookie_dict,
    load_sidea_accounts,
)


def main():
    pool = SideaPool(redis_conn)

    accounts = load_sidea_accounts()

    for account in accounts:

        if not account.enabled:
            continue

        account_key = account.key

        status = pool.get_status(account_key)

        if status not in {
            "READY",
            "UNKNOWN",
        }:
            print(
                "SIDEA_KEEPALIVE_SKIP =",
                {
                    "account": account_key,
                    "status": status,
                },
                flush=True,
            )
            continue

        try:
            session, state = (
                pool.build_http_session(
                    account_key
                )
            )

            response = session.get(
                f"{SIDEA_BASE_URL}/solicitudes.do",
                timeout=(
                    SIDEA_HTTP_CONNECT_TIMEOUT,
                    SIDEA_HTTP_READ_TIMEOUT,
                ),
                allow_redirects=True,
            )

            html = response.text or ""

            if not _sidea_html_is_authenticated(
                html
            ):
                pool.clear_session(
                    account_key,
                    reason="NEED_LOGIN",
                )

                print(
                    "SIDEA_KEEPALIVE_NEED_LOGIN =",
                    {
                        "account": account_key,
                    },
                    flush=True,
                )

                continue

            # Guardar las cookies refrescadas y renovar
            # también el TTL local de Redis.
            pool.save_session(
                account_key,
                _sidea_safe_cookie_dict(
                    session
                ),
                session_id=str(
                    state.get("session_id")
                    or ""
                ),
                usuario=str(
                    state.get("usuario")
                    or ""
                ),
                usuario_rol=str(
                    state.get("usuario_rol")
                    or ""
                ),
                usuario_entidad=str(
                    state.get("usuario_entidad")
                    or ""
                ),
            )

            print(
                "SIDEA_KEEPALIVE_OK =",
                {
                    "account": account_key,
                    "http_status": response.status_code,
                    "usage": pool.usage(
                        account_key
                    ),
                },
                flush=True,
            )

        except Exception as exc:
            print(
                "SIDEA_KEEPALIVE_ERROR =",
                {
                    "account": account_key,
                    "error_type": (
                        type(exc).__name__
                    ),
                    "error": str(exc)[:300],
                },
                flush=True,
            )


if __name__ == "__main__":
    main()
