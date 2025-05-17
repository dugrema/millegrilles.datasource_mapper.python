from typing import AsyncIterable
from datetime import datetime
import xml.etree.ElementTree as ET

from millegrilles_datasourcemapper.DataParserUtilities import GroupedDatedItemData, GroupData

async def parse(data: str) -> AsyncIterable[GroupedDatedItemData]:
    """
    Parses the provided RSS feed data and yields GroupedDatedItemData objects. 
    The function groups by <item> elements and then iterates over each <ht:news_item>
    within an item. For each news item, it extracts:
      - label from <ht:news_item_title>
      - date from the parent's <pubDate> (converted to epoch seconds)
      - associated_urls including a main link (from <link>) and a picture URL (if available in <ht:picture> or <ht:news_item_picture>)

    The group data is built from the parent <item> element.
    """

    # Parse the XML string into an ElementTree
    root = ET.fromstring(data)

    # Define namespaces used in the RSS feed
    ns = {
        'atom': "http://www.w3.org/2005/Atom",
        'ht': "https://trends.google.com/trending/rss"
    }

    # Loop over each <item> element (grouping by item)
    for item in root.findall(".//channel//item"):
        # Extract group label from the item's title
        group_label = item.findtext("title")
        approx_traffic = item.findtext("ht:approx_traffic", namespaces=ns)

        # Parse publication date from <pubDate>
        pub_date_str = item.findtext("pubDate")
        if pub_date_str:
            try:
                # The format is e.g., "Sat, 17 May 2025 11:20:00 -0700"
                dt = datetime.strptime(pub_date_str, "%a, %d %b %Y %H:%M:%S %z")
                epoch_date = int(dt.timestamp())
            except Exception:
                epoch_date = None
        else:
            epoch_date = None

        # Create the group data object for this item
        group_data = GroupData(label=group_label, pub_date=epoch_date, other={'approx_traffic': approx_traffic})

        # Extract picture URL from <ht:picture> element within the item
        picture_url = item.findtext("ht:picture", namespaces=ns)

        # Now, for each news_item inside this item, yield a GroupedDatedItemData object.
        for news_item in item.findall("ht:news_item", namespaces=ns):
            # Extract the title from <ht:news_item_title>
            news_title = news_item.findtext("ht:news_item_title", namespaces=ns)
            if not news_title:
                continue  # Skip this news item if no title is found

            # The date for each news item is taken from the parent's publication date.
            news_epoch_date = epoch_date

            # Build associated URLs dictionary
            associated_urls = {}

            # Extract main link from <link> element (if available)
            main_link = news_item.findtext("ht:news_item_url", namespaces=ns)
            if main_link:
                associated_urls[main_link] = 'main'

            # Also, check for a picture URL within the news_item (<ht:news_item_picture>)
            news_item_picture = news_item.findtext("ht:news_item_picture", namespaces=ns) or picture_url
            if news_item_picture:
                associated_urls[news_item_picture] = 'picture'

            # Create and yield the GroupedDatedItemData object
            item_obj = GroupedDatedItemData(label=news_title, date=news_epoch_date, group=group_data)
            item_obj.associated_urls = associated_urls

            yield item_obj
