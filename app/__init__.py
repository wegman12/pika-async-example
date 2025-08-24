from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI

from app import new_task, worker_async
from app.stop_flag import StopFlag
from .models import QueueOptions
from .worker_async import WorkerAsync
from .extensions import register_extensions

worker_manager = WorkerAsync(queue_options=QueueOptions(name="hello"))


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[Any]:
    register_extensions(application)
    yield
    if worker_manager.is_running:
        worker_manager.stop_workers()


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root() -> dict[str, str]:
    return {"Hello": "World"}


@app.post("/start-workers")
def start_worker() -> None:
    global worker_manager
    worker_manager.start_workers()


@app.post("/stop-workers")
def stop_workers() -> None:
    global worker_manager
    worker_manager.stop_workers()


@app.post("/create-task")
async def create_task(message: str | None = None) -> None:
    await new_task.create_task(message)
