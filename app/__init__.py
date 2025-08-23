import threading

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Never

from fastapi import FastAPI

from app import new_task, worker_async
from app.stop_flag import StopFlag

stop_flag = StopFlag()


def create_worker_manager() -> threading.Thread:
    global stop_flag
    return threading.Thread(target=worker_async.start_workers, args=(stop_flag,))


worker_manager = create_worker_manager()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[Any]:
    yield
    stop_flag.stop = True
    if worker_manager.is_alive():
        worker_manager.join()


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root() -> dict[str, str]:
    return {"Hello": "World"}


@app.post("/start-workers")
def start_worker() -> None:
    global worker_manager
    worker_manager.start()


@app.post("/stop-workers")
def stop_workers() -> None:
    global worker_manager
    global stop_flag
    stop_flag.stop = True
    if worker_manager.is_alive():
        worker_manager.join()
    stop_flag.stop = False
    worker_manager = create_worker_manager()


@app.post("/create-task")
async def create_task(message: str | None = None) -> None:
    await new_task.create_task(message)
