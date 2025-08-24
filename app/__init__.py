from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI

from app.models.stop_flag import StopFlag
from app.services import worker_thread_manager, message_publishing_service
from .models import QueueOptions
from .extensions import register_extensions
from app import configuration

worker_manager = worker_thread_manager.WorkerThreadManager(
    queue_options=QueueOptions(name="hello")
)


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
    await message_publishing_service.publish_message(
        routing_key="hello", message=message
    )
