import asyncio
from typing import cast

import pytest

from aiosafeconsumer import ConsumerWorker, DataTransformer

from .conftest import IntProcessor, StrProcessor, StrSource


@pytest.mark.asyncio
async def test_consumer(consumer: ConsumerWorker) -> None:
    source = cast(StrSource, consumer.source)
    processor = cast(StrProcessor, consumer.processor)

    assert processor.storage == []

    items_count = sum([len(batch) for batch in source.BATCHES])

    task = asyncio.create_task(consumer.run())

    while not task.done() and len(processor.storage) < items_count * 3:
        await asyncio.sleep(0)

    assert not task.done()
    task.cancel()

    assert processor.storage == [
        *source.BATCHES[0],
        *source.BATCHES[1],
        *source.BATCHES[0],
        *source.BATCHES[1],
        *source.BATCHES[0],
        *source.BATCHES[1],
    ]


@pytest.mark.asyncio
async def test_consumer_with_transformer(
    consumer_with_transformer: ConsumerWorker,
) -> None:
    consumer = consumer_with_transformer
    source = cast(StrSource, consumer.source)
    transformer = cast(DataTransformer, consumer.processor)
    target = cast(IntProcessor, transformer.target_processor)

    assert target.storage == []

    items_count = sum([len(batch) for batch in source.BATCHES])

    task = asyncio.create_task(consumer.run())

    while not task.done() and len(target.storage) < items_count:
        await asyncio.sleep(0)

    assert not task.done()
    task.cancel()

    assert target.storage == list(range(1, 11))
