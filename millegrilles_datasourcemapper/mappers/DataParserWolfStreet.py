import xml.etree.ElementTree as ET
import calendar
import datetime
from email.utils import parsedate_tz
from typing import AsyncIterable
from millegrilles_datasourcemapper.DataParserUtilities import DatedItemData


async def parse(data: str) -> AsyncIterable[DatedItemData]:
    """
    Parses the provided XML data (as a string) and yields DatedItemData objects.

    Each item is expected to have at least:
      - A <title> element for the label.
      - A <pubDate> element whose text is parsed into an epoch timestamp.

    Optional elements are processed as follows:
      • The <link> element (if present) is added to associated_urls with key 'main'.
      • Any child element ending with "content" having attribute type="image"
        and a URL in its attributes is added to associated_urls with key 'picture'.
      • Elements named 'snippet', 'summary' and 'description' are collected into data_str.
      • All <category> elements (if present) are combined (comma separated) as the "subject".
      • The <slash:comments> element (if present and numeric) is added to data_number.
    """
    try:
        root = ET.fromstring(data)
    except Exception as e:
        # If parsing fails, yield nothing.
        return

    channel = root.find('channel')
    if channel is None:
        return

    for item in channel.findall('item'):
        # Get the label from <title>
        label = item.findtext('title', default="").strip()

        # Parse the publication date into an epoch timestamp
        pubDate_text = item.findtext('pubDate', default="")
        epoch = 0  # default value if parsing fails
        if pubDate_text:
            dt_tuple = parsedate_tz(pubDate_text)
            if dt_tuple and dt_tuple[-1] is not None:
                # dt_tuple[:6] contains (year, month, day, hour, minute, second)
                epoch = calendar.timegm(dt_tuple[:6]) - dt_tuple[-1]
            else:
                try:
                    dt = datetime.datetime.strptime(pubDate_text, "%a, %d %b %Y %H:%M:%S %z")
                    epoch = int(dt.timestamp())
                except Exception:
                    epoch = 0

        # Create the DatedItemData object with mandatory fields
        dated_item = DatedItemData(label=label, date=epoch)

        # Build associated_urls dictionary
        urls = {}
        link_text = item.findtext('link')
        if link_text:
            urls[link_text.strip()] = 'main'

        # Look for a picture URL in any child element that might represent media content.
        # For example, some feeds include <media:content type="image" url="...">.
        for elem in item:
            if elem.tag.endswith("content") and elem.attrib.get('type', '').lower() == 'image':
                pic_url = elem.attrib.get('url')
                if pic_url:
                    urls[pic_url.strip()] = 'picture'
        dated_item.associated_urls = urls

        # Build data_str dictionary from optional text elements.
        data_str = {}
        for tag in ['snippet', 'summary', 'description']:
            text_val = item.findtext(tag)
            if text_val is not None:
                data_str[tag] = text_val.strip()
        # Combine category elements as the subject (if any exist)
        categories = [cat.text.strip() for cat in item.findall('category') if cat.text]
        if categories:
            data_str['subject'] = ", ".join(categories)
        dated_item.data_str = data_str

        # Build data_number dictionary from numeric values.
        data_num = {}
        comments_text = item.findtext('slash:comments')
        if comments_text is not None:
            try:
                data_num['comments'] = int(comments_text.strip())
            except ValueError:
                pass
        dated_item.data_number = data_num

        yield dated_item
