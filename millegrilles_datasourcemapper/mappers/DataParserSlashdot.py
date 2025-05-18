import xml.etree.ElementTree as ET
from datetime import datetime
from typing import AsyncIterable

from millegrilles_datasourcemapper.DataParserUtilities import DatedItemData

async def parse(data: str) -> AsyncIterable[DatedItemData]:
    """
    Parsing function for RSS 1.0 data.
    :param data: Input data, same structure as the provided sample.
    """

    ns = {
        "": "http://purl.org/rss/1.0/",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "dc": "http://purl.org/dc/elements/1.1/",
        "taxo": "http://purl.org/rss/1.0/modules/taxonomy/",
        "slash": "http://purl.org/rss/1.0/modules/slash/"
    }

    root = ET.fromstring(data)

    for item in root.findall('.//item', ns):
        title = item.find('title', ns).text
        date_str = item.find('dc:date', ns).text
        date = int(datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z').timestamp())

        data_str = {}
        data_str['description'] = item.find('description', ns).text
        subject = item.find('dc:subject', ns).text
        section = item.find('slash:section', ns).text
        department = item.find('slash:department', ns).text
        data_str['keywords'] = [section, subject, department]
        associated_urls = {}
        item_link = item.find('link', ns).text
        associated_urls[item_link] = 'main'

        data = DatedItemData(label=title, date=date, data_str=data_str, associated_urls=associated_urls)

        yield data
