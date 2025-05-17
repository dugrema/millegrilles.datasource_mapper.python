import tempfile
from xml.etree import ElementTree as ET

from typing import AsyncIterable
from millegrilles_datasourcemapper.DataParserUtilities import GroupedDatedItemData, GroupData, parse_date

async def parse(data: str) -> AsyncIterable[GroupedDatedItemData]:
    with tempfile.TemporaryFile('wt+') as temp_file:
        temp_file.write(data)
        temp_file.seek(0)
        parsed_content: ET = ET.parse(temp_file)

    ns_ht = 'https://trends.google.com/trending/rss'

    root = parsed_content.getroot()

    for channel_item in root.findall('./channel/item'):
        group_label = channel_item.find('title').text
        group_pub_date = parse_date(channel_item.find('pubDate').text)
        group_traffic = channel_item.find('{%s}approx_traffic' % ns_ht).text
        try:
            group_picture = channel_item.find('{%s}picture' % ns_ht).text
        except TypeError:
            group_picture = None

        group: GroupData = GroupData(label=group_label, pub_date=group_pub_date, other=None)
        if group_traffic:
            group['other'] = {'approx_traffic': group_traffic}

        news_items = channel_item.findall('{%s}news_item' % ns_ht)
        for news_item in news_items:
            label = news_item.find('{%s}news_item_title' % ns_ht).text

            associated_urls = dict()
            try:
                url = news_item.find('{%s}news_item_url' % ns_ht).text
                associated_urls[url] = 'main'
            except TypeError:
                pass

            try:
                picture = news_item.find('{%s}news_item_picture' % ns_ht).text
            except TypeError:
                picture = group_picture

            if picture is not None:
                associated_urls[picture] = 'picture'

            data_str = dict()
            try:
                source = news_item.find('{%s}news_item_source' % ns_ht).text
                data_str['source'] = source
            except TypeError:
                pass

            grouped_dated_item = GroupedDatedItemData(label=label, date=group_pub_date, group=group)
            grouped_dated_item.associated_urls = associated_urls
            grouped_dated_item.data_str = data_str

            yield grouped_dated_item
