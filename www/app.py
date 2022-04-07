import logging; logging.basicConfig(level=logging.INFO)
from aiohttp import web

def getIndex(request):
    return web.Response(body="Hello World!")

app = web.Application()
app.add_routes([web.get("/", getIndex)])
web.run_app(app, host="localhost", port=10090)
       