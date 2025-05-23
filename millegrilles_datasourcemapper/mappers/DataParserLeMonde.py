from typing import AsyncIterable
import feedparser
from datetime import datetime
import pytz

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
        pub_date = datetime.strptime(pub_date_str, '%a, %d %b %Y %H:%M:%S %z')
        date = int(pub_date.timestamp())

        # Extract optional data
        summary = entry.summary
        description = entry.description
        try:
           keywords = entry.tags
        except AttributeError:
           keywords = None

        # Extract URLs
        link = entry.link
        media_content = entry.get('media_content', [])
        picture_url = None
        if media_content:
            picture_url = media_content[0].get('url')

        # Create the DatedItemData object
        data_str = {
            'summary': summary,
            'description': description,
            'keywords': keywords
        }

        associated_urls = {}
        if link:
            associated_urls[link] = 'main'
        if picture_url:
            associated_urls[picture_url] = 'picture'

        yield DatedItemData(
            label=label,
            date=date,
            data_str=data_str,
            associated_urls=associated_urls
        )
