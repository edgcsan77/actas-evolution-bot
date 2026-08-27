import time

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


# ============================================================
# SIDEA_KEEPALIVE_AUTH_CONFIRM_V1
#
# Nunca destruir una sesión SIDEA por una respuesta HTML
# ambigua o por un único falso negativo.
# ============================================================

def _keepalive_auth_state(
    html: str,
) -> str:

    html = html or ""
    lower = html.lower()

    if _sidea_html_is_authenticated(
        html
    ):
        return "AUTHENTICATED"

    expired_signals = (
        "sesi&oacute;n finalizada",
        "sesión finalizada",
        "sesion finalizada",
        "ha finalizado debido",
        "tiempo de inactiv",
        "acceder nuevamente",
    )

    if any(
        signal in lower
        for signal in expired_signals
    ):
        return "EXPIRED"

    login_form = (
        "autenticacion.do"
        in lower
        and (
            "contrasenia"
            in lower
            or "contrase&ntilde;a"
            in lower
            or "contraseña"
            in lower
        )
    )

    if login_form:
        return "LOGIN_FORM"

    return "UNKNOWN"


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

            auth_state = (
                _keepalive_auth_state(
                    html
                )
            )

            if auth_state != "AUTHENTICATED":

                print(
                    "SIDEA_KEEPALIVE_AUTH_SUSPECT =",
                    {
                        "account": account_key,
                        "state": auth_state,
                        "http_status": (
                            response.status_code
                        ),
                    },
                    flush=True,
                )

                # Una respuesta desconocida NO demuestra
                # que SIDEA haya cerrado la sesión.
                if auth_state == "UNKNOWN":
                    print(
                        "SIDEA_KEEPALIVE_AUTH_INCONCLUSIVE_SESSION_PRESERVED =",
                        {
                            "account": account_key,
                            "state": auth_state,
                        },
                        flush=True,
                    )
                    continue

                # EXPIRED / LOGIN_FORM:
                # confirmar una segunda vez con una sesión HTTP
                # reconstruida desde el estado todavía guardado
                # en Redis.
                time.sleep(2.0)

                (
                    confirm_session,
                    confirm_saved_state,
                ) = pool.build_http_session(
                    account_key
                )

                confirm_response = (
                    confirm_session.get(
                        (
                            f"{SIDEA_BASE_URL}"
                            "/solicitudes.do"
                        ),
                        timeout=(
                            SIDEA_HTTP_CONNECT_TIMEOUT,
                            SIDEA_HTTP_READ_TIMEOUT,
                        ),
                        allow_redirects=True,
                    )
                )

                confirm_html = (
                    confirm_response.text
                    or ""
                )

                confirm_auth_state = (
                    _keepalive_auth_state(
                        confirm_html
                    )
                )

                if (
                    confirm_auth_state
                    == "AUTHENTICATED"
                ):
                    session = confirm_session
                    state = confirm_saved_state
                    response = confirm_response

                    print(
                        "SIDEA_KEEPALIVE_AUTH_RECOVERED =",
                        {
                            "account": account_key,
                            "first_state": auth_state,
                            "confirm_state": (
                                confirm_auth_state
                            ),
                            "http_status": (
                                confirm_response
                                .status_code
                            ),
                        },
                        flush=True,
                    )

                elif (
                    confirm_auth_state
                    in {
                        "EXPIRED",
                        "LOGIN_FORM",
                    }
                ):
                    pool.clear_session(
                        account_key,
                        reason="NEED_LOGIN",
                    )

                    print(
                        "SIDEA_KEEPALIVE_NEED_LOGIN_CONFIRMED =",
                        {
                            "account": account_key,
                            "first_state": (
                                auth_state
                            ),
                            "confirm_state": (
                                confirm_auth_state
                            ),
                            "first_http_status": (
                                response.status_code
                            ),
                            "confirm_http_status": (
                                confirm_response
                                .status_code
                            ),
                        },
                        flush=True,
                    )

                    continue

                else:
                    # Segunda respuesta también ambigua:
                    # conservar la sesión. No tenemos prueba
                    # suficiente para destruirla.
                    print(
                        "SIDEA_KEEPALIVE_AUTH_CONFIRM_INCONCLUSIVE_SESSION_PRESERVED =",
                        {
                            "account": account_key,
                            "first_state": (
                                auth_state
                            ),
                            "confirm_state": (
                                confirm_auth_state
                            ),
                            "confirm_http_status": (
                                confirm_response
                                .status_code
                            ),
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
