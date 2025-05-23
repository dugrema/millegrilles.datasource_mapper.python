import feedparser
from datetime import datetime
from typing import AsyncIterable, Optional
from millegrilles_datasourcemapper.DataParserUtilities import DatedItemData

async def parse(data: str) -> AsyncIterable[DatedItemData]:
    """
    Parsing function.
    :param data: Input data, same structure as the provided sample.
    """

    feed = feedparser.parse(data)

    for entry in feed.entries:
        date = int(datetime(*entry.published_parsed[:6]).timestamp())

        associated_urls = {}
        if 'link' in entry:
            associated_urls[entry.link] = 'main'
        if 'media_thumbnail' in entry and 'url' in entry.media_thumbnail:
            associated_urls[entry.media_thumbnail.url] = 'picture'

        data_str = {
            'summary': entry.summary,
            'description': entry.description,
            'snippet': entry.title_detail.get('value', None)
        }

        if 'tags' in entry and entry.tags:
            data_str['subject'] = entry.tags[0].term
            data_str['keywords'] = ', '.join([tag.term for tag in entry.tags])
        else:
            data_str['subject'] = None
            data_str['keywords'] = None

        yield DatedItemData(
            label=entry.title,
            date=date,
            data_str=data_str,
            data_number=None,
            associated_urls=associated_urls
        )
