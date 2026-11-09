import base64
from secrets import compare_digest
from starlette.responses import Response


def protect(app):
    expected = base64.b64encode(b"admin:admin")

    @app.middleware("http")
    async def local_auth(request, call_next):
        if request.url.path != "/health":
            scheme, _, value = request.headers.get("authorization", "").partition(" ")
            if scheme.lower() != "basic" or not compare_digest(
                value.encode("utf-8"), expected
            ):
                return Response(
                    status_code=401,
                    headers={
                        "WWW-Authenticate": 'Basic realm="Northwind Classic"',
                        "Cache-Control": "no-store",
                    },
                )
        return await call_next(request)
