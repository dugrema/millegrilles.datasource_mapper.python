import feedparser
from datetime import datetime
from typing import AsyncIterable, Optional
from millegrilles_datasourcemapper.DataParserUtilities import DatedItemData

async def parse(data: str) -> AsyncIterable[DatedItemData]:
    """
    Parsing function.
    :param data: Input data, same structure as the provided sample.
    """

    # Parse the RSS feed
    feed = feedparser.parse(data)

    # Loop through each item in the feed
    for entry in feed.entries:
        # Extract the title and publication date
        label = entry.title
        pub_date_str = entry.published

        # Convert the publication date to epoch seconds
        pub_date = int(datetime.strptime(pub_date_str, "%a, %d %b %Y %H:%M:%S GMT").timestamp())

        # Extract optional data
        summary = entry.summary if 'summary' in entry else None
        description = entry.description if 'description' in entry else None

        # Extract the link and media thumbnail (optional)
        link = entry.link
        media_thumbnail = entry.media_thumbnail[0]['url'] if 'media_thumbnail' in entry else None

        # Create a DatedItemData object
        dated_item_data = DatedItemData(
            label=label,
            date=pub_date,
            data_str={'summary': summary, 'description': description} if summary or description else None,
            associated_urls={link: 'main'} if link else None
        )

        # Add media thumbnail to associated_urls if it exists
        if media_thumbnail:
            if dated_item_data.associated_urls is None:
                dated_item_data.associated_urls = {}
            dated_item_data.associated_urls[media_thumbnail] = 'picture'

        # Yield the DatedItemData object
        yield dated_item_data
