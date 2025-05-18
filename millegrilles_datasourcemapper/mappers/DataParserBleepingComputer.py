from typing import AsyncIterable
import xml.etree.ElementTree as ET
from datetime import datetime

from millegrilles_datasourcemapper.DataParserUtilities import DatedItemData


async def parse(data: str) -> AsyncIterable[DatedItemData]:
    """
    Parses an RSS feed XML string and yields DatedItemData objects for each <item>.

    Mandatory mapping:
      - label: from the <title> element.
      - date: from the <pubDate> element, converted to epoch seconds.

    Optional mappings:
      - associated_urls: if a <link> element exists, add it with value 'main'.
      - data_str: includes keys "snippet", "summary", "description", "subject", and "keywords".
        * snippet: first 100 characters of the description (or title if description is empty).
        * summary: full text from the <description>.
        * description: same as summary.
        * subject: first category found in <category> elements.
        * keywords: all categories joined by a comma.
    """

    # Parse the XML data
    root = ET.fromstring(data)

    # Iterate over each <item> element within the feed (typically under <channel>)
    for item in root.findall('.//item'):
        # Extract title from <title>
        title = item.findtext('title', default='')

        # Extract link from <link> and use it as main URL if available
        link = item.findtext('link', default='')

        # Extract publication date from <pubDate> and convert to epoch seconds.
        pub_date_str = item.findtext('pubDate', default='')
        if pub_date_str:
            try:
                # Parse the date string (e.g., "Sat, 17 May 2025 10:09:19 -0400")
                dt = datetime.strptime(pub_date_str, '%a, %d %b %Y %H:%M:%S %z')
                epoch_seconds = int(dt.timestamp())
            except Exception:
                # Fallback if date parsing fails
                epoch_seconds = 0
        else:
            epoch_seconds = 0

        # Extract description text from <description>
        description_elem = item.find('description')
        description_text = ''
        if description_elem is not None and description_elem.text:
            description_text = description_elem.text.strip()

        # Extract categories from multiple <category> elements
        categories = [cat.text for cat in item.findall('category') if cat.text]

        # Build the data_str dictionary with optional keys.
        snippet = (description_text[:100] + '...') if len(description_text) > 100 else description_text
        summary = description_text
        subject = categories[0] if categories else ''
        keywords = ', '.join(categories)

        data_str = {
            "snippet": snippet,
            "summary": summary,
            "description": description_text,
            "subject": subject,
            "keywords": keywords
        }

        # Build associated_urls dictionary.
        associated_urls = {}
        if link:
            associated_urls[link] = 'main'

        # Create and yield the DatedItemData instance.
        dated_item = DatedItemData(label=title, date=epoch_seconds)
        dated_item.data_str = data_str
        dated_item.associated_urls = associated_urls

        yield dated_item
