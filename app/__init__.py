from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI

from app import configuration
from app.models.stop_flag import StopFlag
from app.services import message_publishing_service, worker_thread_manager

from .extensions import register_extensions
from .models import QueueOptions

worker_manager = worker_thread_manager.WorkerThreadManager()


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
    worker_manager.start_workers(queue_options=QueueOptions(name="hello"))


@app.post("/stop-workers")
def stop_workers() -> None:
    global worker_manager
    worker_manager.stop_workers()


@app.post("/create-task")
async def create_task(message: str | None = None) -> None:
    await message_publishing_service.publish_message(
        routing_key="hello", message=message
    )


@app.post("/set-prefetch-count")
async def set_prefetch_count(count: int) -> None:

    configuration.DEFAULT_CONSUMER_PREFETCH_COUNT = count
