import feedparser
from datetime import datetime
from typing import AsyncIterable
from millegrilles_datasourcemapper.DataParserUtilities import DatedItemData
import re

async def parse(data: str) -> AsyncIterable[DatedItemData]:
    """
    Parsing function.
    :param data: Input data, same structure as the provided sample.
    """

    # Parse the RSS feed
    feed = feedparser.parse(data)

    for entry in feed.entries:
        date_str = entry.get('published')
        if not date_str:
            continue

        # Convert date to epoch seconds
        try:
            date_epoch = int(datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z").timestamp())
        except ValueError:
            continue

        # Extract title and link
        title = entry.get('title')
        if title:
            title = re.sub(r'<[^>]+>', '', title)

        link = entry.get('link')

        # Extract description for summary
        summary = entry.get('summary', '')
        if summary:
            summary = re.sub(r'<[^>]+>', '', summary)

        # Extract image URL from the links
        image_url = None
        if 'links' in entry:
            for link_info in entry.links:
                if link_info.rel == 'enclosure' and link_info.type.startswith('image/'):
                    image_url = link_info.href
                    break

        # Create DatedItemData object
        dated_item_data = DatedItemData(
            label=title,
            date=date_epoch,
            data_str={'summary': summary},
            associated_urls={link: 'main'} if link else None
        )

        # Add picture URL if found
        if image_url:
            dated_item_data.associated_urls[image_url] = 'picture'

        yield dated_item_data
