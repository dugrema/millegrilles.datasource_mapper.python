import feedparser
from datetime import datetime
from typing import AsyncIterable
from millegrilles_datasourcemapper.DataParserUtilities import DatedItemData


async def parse(data: str) -> AsyncIterable[DatedItemData]:
    """
    Parsing function.
    :param data: Input data, same structure as the provided sample.
    """

    # Parse the RSS feed
    feed = feedparser.parse(data)

    for entry in feed.entries:
        date = int(datetime(*entry.published_parsed[:6]).timestamp())

        dated_item_data = DatedItemData(
            label=entry.title,
            date=date
        )

        if 'link' in entry:
            dated_item_data.associated_urls = {entry.link: 'main'}

        if 'description' in entry:
            dated_item_data.data_str = {'summary': entry.description}

        yield dated_item_data
