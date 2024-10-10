from typing import cast

import pytest

from .conftest import IntProcessor, StrProcessor, StrSource, StrToIntTransformer


@pytest.mark.asyncio
async def test_processor(source: StrSource, processor: StrProcessor) -> None:
    assert processor.storage == []

    await processor.process(source.BATCHES[0])
    await processor.process(source.BATCHES[1])

    assert processor.storage == [
        *source.BATCHES[0],
        *source.BATCHES[1],
    ]


@pytest.mark.asyncio
async def test_transformer(source: StrSource, transformer: StrToIntTransformer) -> None:
    target = cast(IntProcessor, transformer.target_processor)

    assert target.storage == []

    await transformer.process(source.BATCHES[0])
    await transformer.process(source.BATCHES[1])

    assert target.storage == list(range(1, 11))
