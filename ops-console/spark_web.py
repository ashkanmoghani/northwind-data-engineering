from fastapi import FastAPI, Request
from starlette.responses import Response, StreamingResponse
from starlette.background import BackgroundTask
import requests
from urllib.parse import urlsplit
from local_login import protect

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
protect(app)
UPSTREAM = "http://spark-master:8080"


@app.get("/health")
def health():
    return {"status": "ok"}


@app.api_route("/{path:path}", methods=["GET", "HEAD"])
def show(request: Request, path: str):
    raw = request.scope["raw_path"].decode("ascii")
    query = request.scope["query_string"].decode("ascii")
    target = UPSTREAM + raw + ("?" + query if query else "")
    try:
        source = requests.request(
            request.method,
            target,
            stream=True,
            allow_redirects=False,
            timeout=(4, 30),
            headers={"Accept-Encoding": "identity"},
        )
    except requests.RequestException:
        return Response("Spark UI is unavailable", status_code=502)
    headers = {
        k: v
        for k, v in source.headers.items()
        if k.lower()
        in {
            "content-type",
            "content-language",
            "cache-control",
            "last-modified",
            "etag",
            "location",
        }
    }
    for k in list(headers):
        if k.lower() == "location" and urlsplit(headers[k]).netloc in {
            "spark-master:8080",
            "localhost:8080",
        }:
            u = urlsplit(headers[k])
            headers[k] = u.path + ("?" + u.query if u.query else "")
    return StreamingResponse(
        source.iter_content(64 * 1024),
        status_code=source.status_code,
        headers=headers,
        background=BackgroundTask(source.close),
    )
