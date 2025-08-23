from fastapi import FastAPI
import threading

from app import worker_async
from app import new_task
from app.stop_flag import StopFlag

app = FastAPI()


stop_flag = StopFlag()


def create_worker_manager() -> threading.Thread:
    global stop_flag
    return threading.Thread(target=worker_async.start_workers, args=(stop_flag,))


worker_mangager = create_worker_manager()


@app.get("/")
def read_root() -> dict[str, str]:
    return {"Hello": "World"}


@app.post("/start-workers")
def start_worker() -> None:
    global worker_mangager
    worker_mangager.start()


@app.post("/stop-workers")
def stop_workers() -> None:
    global worker_mangager
    global stop_flag
    stop_flag.stop = True
    if worker_mangager.is_alive():
        worker_mangager.join()
    stop_flag.stop = False
    worker_mangager = create_worker_manager()


@app.post("/create-task")
def create_task(message: str | None = None) -> None:
    new_task.create_task(message)
