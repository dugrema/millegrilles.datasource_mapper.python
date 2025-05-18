from typing import AsyncIterable

from millegrilles_datasourcemapper.DataParserUtilities import DatedItemData

async def parse(data: str) -> AsyncIterable[DatedItemData]:
    raise NotImplementedError("use this function to test parsers")
