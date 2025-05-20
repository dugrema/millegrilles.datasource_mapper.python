from typing import AsyncIterable
import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime
from millegrilles_datasourcemapper.DataParserUtilities import DatedItemData


async def parse(data: str) -> AsyncIterable[DatedItemData]:
    # Parse the XML data using ElementTree.
    root = ET.fromstring(data)

    # Find all <item> elements. The items are expected to be direct children of <channel>.
    channel = root.find('channel')
    if channel is None:
        return  # No channel found, nothing to yield.

    # Loop over each item element.
    for item in channel.findall('item'):
        # Extract the title text as label. If not present, use an empty string.
        title_elem = item.find('title')
        label = title_elem.text.strip() if title_elem is not None and title_elem.text else ""

        # Extract the link text to be used as main URL.
        link_elem = item.find('link')
        link_url = link_elem.text.strip() if link_elem is not None and link_elem.text else ""

        # Extract description text. We'll map it to data_str under key "description".
        desc_elem = item.find('description')
        description_text = desc_elem.text.strip() if desc_elem is not None and desc_elem.text else ""

        # Extract pubDate text.
        pubdate_elem = item.find('pubDate')
        if pubdate_elem is None or not pubdate_elem.text:
            continue  # Skip this item if no publication date is provided.
        pub_date_str = pubdate_elem.text.strip()

        # Parse the publication date string into a datetime object.
        try:
            # Expected format: "Monday, May 19, 2025 - 13:36"
            dt = datetime.strptime(pub_date_str, "%A, %B %d, %Y - %H:%M")
        except Exception as e:
            # If parsing fails, skip this item.
            continue

        # Convert the datetime to epoch seconds (as an integer).
        date_epoch = int(dt.timestamp())

        # Create a DatedItemData object with mandatory attributes.
        dated_item = DatedItemData(label=label, date=date_epoch)

        # Set associated URLs if link is available.
        if link_url:
            dated_item.associated_urls = {link_url: 'main'}

        # Map description to data_str under key "description".
        if description_text:
            dated_item.data_str = {"description": description_text}

        # Yield the constructed DatedItemData object.
        yield dated_item
