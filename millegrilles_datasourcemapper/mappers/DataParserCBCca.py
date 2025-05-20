# !/usr/bin/env python3
"""
An async generator that parses an RSS feed (in XML) and yields DatedItemData objects.
Each item’s title becomes the label, its pubDate is parsed into epoch seconds,
and optional URLs (from <link> or an embedded <image><url>…) are stored in associated_urls.
If a description exists it is saved under data_str (plus a snippet extracted from its first paragraph).
"""

import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_tz
import re
from typing import AsyncIterable

from millegrilles_datasourcemapper.DataParserUtilities import DatedItemData


async def parse(data: str) -> AsyncIterable[DatedItemData]:
    """
    Asynchronously parses the given XML string (an RSS feed) and yields DatedItemData objects.

    For each <item> element:
      - The text of the <title> element is used as the label.
      - The text of the <pubDate> element is parsed into an epoch timestamp.
      - If a <link> element exists, its URL is stored in associated_urls with type "main".
      - If an <image><url>…</url></image> subelement exists, that URL is stored as type "picture".
      - The text of the <description> element (if any) is saved under data_str['description'].
        Additionally, a “snippet” is extracted from the first paragraph (<p> … </p>) if found.
    """
    # Parse the XML tree
    root = ET.fromstring(data)

    # Helper function to convert an RFC-822 date (e.g. "Tue, 20 May 2025 13:55:38 -0400")
    # into epoch seconds.
    def parse_pubdate(pub_date_str: str) -> int:
        parsed_tuple = parsedate_tz(pub_date_str)
        if not parsed_tuple:
            raise ValueError("Invalid date format")
        dt = datetime(*parsed_tuple[:7])
        offset = parsed_tuple[9] or 0
        return int(dt.timestamp() - offset)

    # Iterate over each <item> element.
    for item in root.findall('.//item'):
        title_elem = item.find('title')
        pubdate_elem = item.find('pubDate')
        link_elem = item.find('link')
        description_elem = item.find('description')
        image_elem = item.find('image')  # check if an <image> subelement exists

        label_text = (title_elem.text or "").strip()
        pubdate_text = (pubdate_elem.text or "").strip() if pubdate_elem is not None else ""
        link_text = (link_elem.text or "").strip() if link_elem is not None else ""

        # Parse the publication date into epoch seconds.
        date_epoch = parse_pubdate(pubdate_text)

        # Build the associated URLs dictionary.
        associated_urls = {}
        if link_text:
            associated_urls[link_text] = 'main'
        if image_elem is not None:
            url_image_elem = image_elem.find('url')
            if url_image_elem is not None and url_image_elem.text:
                img_url = url_image_elem.text.strip()
                associated_urls[img_url] = 'picture'

        # Build the data_str dictionary.
        data_str = {}
        if description_elem is not None and description_elem.text:
            desc_text = description_elem.text.strip()
            data_str['description'] = desc_text
            # Optionally, extract a snippet from the first <p> tag (if present)
            p_match = re.search(r'<p>(.*?)</p>', desc_text, flags=re.DOTALL)
            if p_match:
                snippet = p_match.group(1).strip()
                data_str['snippet'] = snippet

        # Yield a new DatedItemData object.
        yield DatedItemData(label=label_text,
                            date=date_epoch,
                            associated_urls=associated_urls,
                            data_str=data_str)
