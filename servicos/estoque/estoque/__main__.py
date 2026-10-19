"""python -m estoque  (sobe a API e o consumidor)"""

import asyncio
import sys

import uvicorn

if sys.platform == "win32":  # psycopg assíncrono não funciona com o loop Proactor do Windows
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

config = uvicorn.Config("estoque.app:criar_app", factory=True, host="0.0.0.0", port=8000, loop="none")
asyncio.run(uvicorn.Server(config).serve())
