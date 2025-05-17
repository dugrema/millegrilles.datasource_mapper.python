import binascii
import datetime
import json
import math
import tempfile

from typing import Optional, TypedDict, Union
from xml.etree import ElementTree as ET

from millegrilles_messages.messages.Hachage import hacher_to_digest


def hash_to_id(value: Union[str, dict, list]) -> str:
    """
    Creates a unique id from a value.
    :param value: Content to digest
    :return: String digest
    """
    if isinstance(value, dict) or isinstance(value, list):
        value = json.dumps(value, sort_keys=True)
    digest_value = hacher_to_digest(value, 'blake2s-256')
    return binascii.hexlify(digest_value).decode('utf-8')


class DatedItemData:
    """ Item with data associated to a specific date, for example a publication date. """

    def __init__(self, label: str, date: int):
        self.label = label
        """ Item label. This can be a title or short description """

        self.date = date
        """ Item date in epoch seconds, for example the publication date. """

        self.data_str: Optional[dict[str, str]] = None
        """ Dict of string values """

        self.data_number: Optional[dict[str, Union[int, float]]] = None
        """ Dict of number values """

        self.associated_urls: Optional[dict[str, str]] = None
        """ 
        Dict of urls (dict key) found in the item with the type of the url content (dict value).
        Examples of url content type: main, article, picture, video, reference, footnote. 
        """

    @property
    def data_id(self):
        items = [self.label, self.date, self.data_str, self.data_number]
        return hash_to_id(items)

    def get_cleartext(self) -> dict:
        return {
            'label': self.label,
            'date': self.date,
            'data_str': self.data_str,
            'data_number': self.data_number,
            'urls': self.associated_urls,
        }


class GroupData(TypedDict):
    label: Optional[str]
    """ Group label """

    pub_date: Optional[int]
    """ Group publication date as epoch seconds. """

    other: Optional[dict]
    """ Other group attributes """


class GroupedDatedItemData(DatedItemData):
    """ Item associated to a date and grouped with other items. """

    def __init__(self, label: str, date: int, group: GroupData):
        super().__init__(label, date)
        self.group = group

    def get_cleartext(self) -> dict:
        item = super().get_cleartext()
        item['group'] = self.group
        return item


async def parse_google_trends(data: str):
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


def parse_date(date_str: str) -> int:
    parsed_date = datetime.datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')
    return math.floor(parsed_date.timestamp())
