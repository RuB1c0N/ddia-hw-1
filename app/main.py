import asyncio
import os
import random
from dataclasses import dataclass

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field

from endpoint_metrics import EndpointMetrics, Identity


@dataclass(frozen=True)
class Settings:
    pod: str = "pod-local"
    datacenter: str = "dc-local"
    deployment: str = "ddia-hw-1"
    error_rate: float = 0.02
    max_items: int = 5000

    @classmethod
    def from_env(cls) -> "Settings":
        settings = cls(
            pod=os.getenv("POD_ID", "pod-local"),
            datacenter=os.getenv("DATACENTER", "dc-local"),
            deployment=os.getenv("DEPLOYMENT", "ddia-hw-1"),
            error_rate=float(os.getenv("APP_ERROR_RATE", "0.02")),
            max_items=int(os.getenv("MAX_ITEMS", "5000")),
        )
        if not 0 <= settings.error_rate <= 0.2 or settings.max_items < 1:
            raise ValueError("APP_ERROR_RATE must be 0..0.2 and MAX_ITEMS must be positive")
        if not all((settings.pod, settings.datacenter, settings.deployment)):
            raise ValueError("Instance labels must not be empty")
        return settings


class ItemInput(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    value: int = Field(default=0, ge=0, le=1_000_000)


class Item(ItemInput):
    id: int


def create_app(settings: Settings | None = None) -> FastAPI:
    config = settings or Settings.from_env()
    application = FastAPI(title="DDIA HW1", version="1.0.0")
    metrics = EndpointMetrics(Identity(config.pod, config.datacenter, config.deployment))
    items: dict[int, Item] = {}
    next_id = 1
    application.state.metrics = metrics

    async def demo_conditions() -> None:
        await asyncio.sleep(random.uniform(0.005, 0.04))
        if random.random() < config.error_rate:
            raise HTTPException(500, "Controlled demo failure")

    router = metrics.router(dependencies=[Depends(demo_conditions)])

    @router.post("/items", response_model=Item, status_code=201)
    async def create_item(payload: ItemInput) -> Item:
        nonlocal next_id
        item = Item(id=next_id, **payload.model_dump())
        next_id += 1
        if len(items) >= config.max_items:
            items.pop(next(iter(items)))
        items[item.id] = item
        return item

    @router.get("/items/{item_id}", response_model=Item)
    async def get_item(item_id: int) -> Item:
        if item_id not in items:
            raise HTTPException(404, "Item not found")
        return items[item_id]

    @router.put("/items/{item_id}", response_model=Item)
    async def update_item(item_id: int, payload: ItemInput) -> Item:
        if item_id not in items:
            raise HTTPException(404, "Item not found")
        item = Item(id=item_id, **payload.model_dump())
        items[item_id] = item
        return item

    application.include_router(router)

    @application.get("/health", include_in_schema=False)
    async def health() -> dict:
        return {"status": "ok", "pod": config.pod, "datacenter": config.datacenter}

    @application.get("/metrics", include_in_schema=False)
    async def exposition():
        return metrics.response()

    @application.get("/handlers", include_in_schema=False)
    async def handlers() -> dict:
        return {"deployment": config.deployment, "handlers": metrics.handlers(application)}

    return application


app = create_app()
