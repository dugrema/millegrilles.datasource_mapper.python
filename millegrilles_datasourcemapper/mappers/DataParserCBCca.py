import feedparser
from typing import AsyncIterable
from datetime import datetime
from bs4 import BeautifulSoup
from millegrilles_datasourcemapper.DataParserUtilities import DatedItemData

def remove_html_tags(text):
    return BeautifulSoup(text, "html.parser").get_text()

async def parse(data: str) -> AsyncIterable[DatedItemData]:
    """
    Parsing function.
    :param data: Input data, same structure as the provided sample.
    """

    # Parse the RSS feed
    feed = feedparser.parse(data)

    for entry in feed.entries:
        date_epoch = int(datetime(*entry.published_parsed[:6]).timestamp())

        associated_urls = {}
        if 'link' in entry:
            associated_urls[entry.link] = 'main'

        # Extract picture URL from summary
        picture_url = None
        soup = BeautifulSoup(entry.summary, "html.parser")
        img_tag = soup.find('img')
        if img_tag and 'src' in img_tag.attrs:
            picture_url = img_tag.attrs['src']

        if picture_url:
            associated_urls[picture_url] = 'picture'

        data_str = {
            'summary': remove_html_tags(entry.get('summary', '')),
            'description': remove_html_tags(entry.get('description', '')),
            'subject': remove_html_tags(entry.get('title', '')),
            'keywords': ', '.join(tag.term for tag in entry.get('tags', [])),
            'snippet': remove_html_tags(entry.get('content', [{}])[0].get('snippet', ''))
        }

        yield DatedItemData(
            label=remove_html_tags(entry.title),
            date=date_epoch,
            data_str=data_str,
            associated_urls=associated_urls
        )
