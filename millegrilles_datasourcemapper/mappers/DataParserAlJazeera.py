from typing import AsyncIterable
import xml.etree.ElementTree as ET
from datetime import datetime
import email.utils

from millegrilles_datasourcemapper.DataParserUtilities import DatedItemData


async def parse(data: str) -> AsyncIterable[DatedItemData]:
    """
    Parses an RSS feed string and yields DatedItemData objects for each <item>.

    The function extracts the title as the label, converts the publication date (<pubDate>)
    into epoch seconds, and optionally gathers additional text data (snippet, summary,
    description, subject, keywords) as well as a main URL from the <link> element.
    """

    # Parse the XML data with namespaces
    try:
        root = ET.fromstring(data)
    except Exception as e:
        return

    # Define namespace mapping for RSS feed elements.
    ns = {
        'atom': "http://www.w3.org/2005/Atom",
        'content': "http://purl.org/rss/1.0/modules/content/",
        'media': "http://search.yahoo.com/mrss/"
    }

    # Find the channel element which contains the items.
    channel = root.find('channel')
    if channel is None:
        return

    def parse_pub_date(pub_date_str: str) -> int:
        parsed_tuple = email.utils.parsedate_tz(pub_date_str)
        if not parsed_tuple:
            raise ValueError("Invalid date format")
        dt = datetime(*parsed_tuple[:7])
        offset = parsed_tuple[9] or 0
        return int(dt.timestamp() - offset)

    # Iterate over each <item> element within the channel.
    for item in channel.findall('item', ns):
        title_elem = item.findtext('title', ns)
        pub_date_str = item.findtext('pubDate', ns)
        if not title_elem or not pub_date_str:
            continue

        epoch_seconds = parse_pub_date(pub_date_str)

        data_str = {}
        for key in ['snippet', 'summary', 'description', 'subject', 'keywords']:
            value = item.findtext(key, ns)
            if isinstance(value, str) and value.strip():
                data_str[key] = value.strip()

        associated_urls = {}
        link = item.findtext('link')
        if link:
            associated_urls[link] = 'main'

        dated_item = DatedItemData(label=title_elem.strip(), date=epoch_seconds)
        if data_str:
            dated_item.data_str = data_str
        if associated_urls:
            dated_item.associated_urls = associated_urls

        yield dated_item