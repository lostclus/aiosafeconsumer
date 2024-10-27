import pytest

from .conftest import StrSource


@pytest.mark.asyncio
async def test_source_generator(source: StrSource) -> None:
    assert source.is_resource_allocated is False

    batches: list[list[str]] = []

    generator = source.read()

    for n in range(5):
        batch = await anext(generator)
        assert source.is_resource_allocated is True
        batches.append(batch)

    await generator.aclose()
    assert source.is_resource_allocated is False

    assert batches == [
        source.BATCHES[0],
        source.BATCHES[1],
        source.BATCHES[0],
        source.BATCHES[1],
        source.BATCHES[0],
    ]


@pytest.mark.asyncio
async def test_source_generator_stop(source: StrSource) -> None:
    source.settings.stop_on = 2
    assert source.is_resource_allocated is False

    generator = source.read()

    await anext(generator)
    await anext(generator)

    with pytest.raises(StopAsyncIteration):
        await anext(generator)

    assert source.is_resource_allocated is False
