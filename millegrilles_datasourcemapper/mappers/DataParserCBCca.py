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
        date_str = entry.get('published')
        if date_str:
            date = int(datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %Z').timestamp())
        else:
            date = None

        item_data = DatedItemData(
            label=entry.title,
            date=date
        )

        # Optional data
        if 'link' in entry:
            item_data.associated_urls = {entry.link: 'main'}
        if 'summary' in entry:
            item_data.data_str = {'summary': entry.summary}
        if 'description' in entry:
            item_data.data_str['description'] = entry.description

        # Extract image URL
        if 'media_content' in entry and entry.media_content[0].medium == 'image':
            item_data.associated_urls[entry.media_content[0].url] = 'picture'

        yield item_data
