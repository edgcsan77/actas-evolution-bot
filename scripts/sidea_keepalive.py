import time

from app.queue import redis_conn
from app.services.provider16_sidea import (
    SIDEA_BASE_URL,
    SIDEA_HTTP_CONNECT_TIMEOUT,
    SIDEA_HTTP_READ_TIMEOUT,
    SideaPool,
    _sidea_html_is_authenticated,
    _sidea_html_requires_password_change,
    _sidea_safe_cookie_dict,
    _sidea_prod_acquire_lock,
    _sidea_prod_release_lock,
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

    if _sidea_html_requires_password_change(
        html
    ):
        return "PASSWORD_CHANGE_REQUIRED"

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

        # =====================================================
        # SIDEA_KEEPALIVE_ACCOUNT_LOCK_V1
        #
        # El keepalive comparte las mismas sesiones Redis
        # que los workers de producción.
        #
        # JAMÁS tocar una sesión mientras un worker esté
        # usando esa misma cuenta.
        # =====================================================
        lock_token = _sidea_prod_acquire_lock(
            pool,
            account_key,
            ttl_sec=120,
        )

        if not lock_token:
            print(
                "SIDEA_KEEPALIVE_SKIP_BUSY =",
                {
                    "account": account_key,
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

                if (
                    auth_state
                    == "PASSWORD_CHANGE_REQUIRED"
                ):
                    pool.clear_session(
                        account_key,
                        reason=(
                            "PASSWORD_CHANGE_REQUIRED"
                        ),
                    )

                    pool.set_status(
                        account_key,
                        "PASSWORD_CHANGE_REQUIRED",
                        ttl_sec=604800,
                    )

                    print(
                        "SIDEA_KEEPALIVE_"
                        "PASSWORD_CHANGE_REQUIRED =",
                        {
                            "account": account_key,
                        },
                        flush=True,
                    )

                    continue

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

                    # ============================================
                    # SIDEA_KEEPALIVE_EXPIRED_3_CYCLES_V1
                    #
                    # SIDEA está alternando entre:
                    #   AUTH / HTTP 500 / EXPIRED.
                    #
                    # Dos EXPIRED separados sólo por 2 segundos
                    # ya no son prueba suficiente para destruir
                    # la sesión.
                    #
                    # Exigimos 3 CICLOS independientes.
                    #
                    # El gate evita que varios arranques manuales
                    # del keepalive sumen strikes inmediatamente.
                    # ============================================

                    strike_key = (
                        "provider16:sidea:"
                        "auth_expired_streak:v1:"
                        f"{account_key}"
                    )

                    gate_key = (
                        "provider16:sidea:"
                        "auth_expired_strike_gate:v1:"
                        f"{account_key}"
                    )

                    try:
                        gate_acquired = bool(
                            pool.redis.set(
                                gate_key,
                                "1",
                                nx=True,
                                ex=180,
                            )
                        )

                        if gate_acquired:
                            strike_count = int(
                                pool.redis.incr(
                                    strike_key
                                )
                            )

                            pool.redis.expire(
                                strike_key,
                                3600,
                            )

                        else:
                            strike_raw = (
                                pool.redis.get(
                                    strike_key
                                )
                            )

                            strike_count = int(
                                strike_raw
                                or 0
                            )

                    except Exception as exc:

                        # Si Redis no permite llevar el contador
                        # de forma fiable, NO borrar la sesión.
                        print(
                            "SIDEA_KEEPALIVE_"
                            "EXPIRED_COUNTER_ERROR =",
                            {
                                "account": account_key,
                                "error": str(exc)[:250],
                            },
                            flush=True,
                        )

                        continue

                    print(
                        "SIDEA_KEEPALIVE_"
                        "EXPIRED_STRIKE =",
                        {
                            "account": account_key,
                            "strike": strike_count,
                            "required": 3,
                            "first_state": auth_state,
                            "confirm_state": (
                                confirm_auth_state
                            ),
                            "gate_acquired": (
                                gate_acquired
                            ),
                        },
                        flush=True,
                    )

                    if strike_count < 3:

                        print(
                            "SIDEA_KEEPALIVE_"
                            "EXPIRED_SESSION_PRESERVED =",
                            {
                                "account": account_key,
                                "strike": strike_count,
                                "required": 3,
                            },
                            flush=True,
                        )

                        continue

                    # Tercer ciclo confirmado:
                    # ahora sí consideramos la sesión vencida.
                    pool.clear_session(
                        account_key,
                        reason="NEED_LOGIN",
                    )

                    try:
                        pool.redis.delete(
                            strike_key
                        )
                        pool.redis.delete(
                            gate_key
                        )
                    except Exception:
                        pass

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
                            "expired_cycles": (
                                strike_count
                            ),
                            "required_cycles": 3,
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

            # SIDEA_KEEPALIVE_EXPIRED_3_CYCLES_V1
            #
            # Llegar aquí significa sesión autenticada.
            # Cualquier racha previa de EXPIRED queda anulada.
            try:
                pool.redis.delete(
                    "provider16:sidea:"
                    "auth_expired_streak:v1:"
                    f"{account_key}"
                )
                pool.redis.delete(
                    "provider16:sidea:"
                    "auth_expired_strike_gate:v1:"
                    f"{account_key}"
                )
            except Exception as exc:
                print(
                    "SIDEA_KEEPALIVE_"
                    "EXPIRED_RESET_WARN =",
                    {
                        "account": account_key,
                        "error": str(exc)[:200],
                    },
                    flush=True,
                )

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

        finally:
            _sidea_prod_release_lock(
                pool,
                account_key,
                lock_token,
            )


if __name__ == "__main__":
    main()
