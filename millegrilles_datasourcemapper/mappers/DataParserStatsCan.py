from typing import AsyncIterable, Optional, Union
import xml.etree.ElementTree as ET
from datetime import datetime
from millegrilles_datasourcemapper.DataParserUtilities import DatedItemData


async def parse(data: str) -> AsyncIterable[DatedItemData]:
    """
    Data parser for ATOM feeds of Statistics Canada.
    Created successfully in one-shot with phi4-reasoning:14b-q4_K_M
    :param data:
    :return:
    """

    # Define namespaces for Atom and XHTML elements.
    ns = {
        'atom': "http://www.w3.org/2005/Atom",
        'xhtml': "http://www.w3.org/1999/xhtml"
    }

    # Parse the XML data.
    root = ET.fromstring(data)

    # Find all <entry> elements within the feed.
    entries = root.findall('.//atom:entry', ns)

    for entry in entries:
        # Extract the title text. The <title> element is of type "xhtml", so its content may be inside a nested <div>.
        title_elem = entry.find('atom:title', ns)
        label_text = ""
        if title_elem is not None:
            # Try to get text from an inner XHTML div.
            div = title_elem.find('xhtml:div', ns)
            if div is not None and div.text:
                label_text = div.text.strip()
            elif title_elem.text:
                label_text = title_elem.text.strip()

        # Extract the updated date from <updated> and convert it to epoch seconds.
        updated_elem = entry.find('atom:updated', ns)
        epoch_time = 0
        if updated_elem is not None and updated_elem.text:
            try:
                dt = datetime.strptime(updated_elem.text, "%Y-%m-%dT%H:%M:%S%z")
                epoch_time = int(dt.timestamp())
            except Exception:
                epoch_time = 0

        # Extract the main URL from the <link> element.
        link_elem = entry.find('atom:link', ns)
        url_main = None
        if link_elem is not None and 'href' in link_elem.attrib:
            url_main = link_elem.attrib['href']

        # Build associated_urls dictionary. If a URL is found, mark it as "main".
        associated_urls = {}
        if url_main:
            associated_urls[url_main] = "main"

        # Extract summary text from <summary> (if available) and add to data_str.
        data_str = {}
        summary_elem = entry.find('atom:summary', ns)
        if summary_elem is not None:
            div_summary = summary_elem.find('xhtml:div', ns)
            if div_summary is not None and div_summary.text:
                summary_text = div_summary.text.strip()
                data_str["summary"] = summary_text

        # Create the DatedItemData object with mandatory label and date.
        item = DatedItemData(label=label_text, date=epoch_time)
        item.data_str = data_str
        item.associated_urls = associated_urls

        yield item