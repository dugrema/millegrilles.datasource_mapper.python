import binascii
import datetime
import json
import math
import tempfile

from typing import Optional, TypedDict
from xml.etree import ElementTree as ET

from millegrilles_datasourcemapper.DataStructures import ProcessJob
from millegrilles_messages.chiffrage.Mgs4 import chiffrer_document, chiffrer_mgs4_bytes_secrete
from millegrilles_messages.messages.Hachage import hacher_to_digest


class GroupData(TypedDict):
    label: Optional[str]
    approx_traffic: Optional[str]
    pub_date: Optional[int]


# class DataCollectorClearData(TypedDict):
#     label: str
#     url: str
#     pub_date: int
#     item_source: Optional[str]
#     picture_url: Optional[str]
#     picture_source: Optional[str]
#     group: GroupData


def hash_to_id(value: str) -> str:
    digest_value = hacher_to_digest(value, 'blake2s-256')
    return binascii.hexlify(digest_value).decode('utf-8')


class ScrapedGoogleTrendsNewsItem:

    def __init__(self, group: GroupData, title: str, url: str, date: Optional[datetime.datetime]):
        self.group = group
        self.label = title
        self.url = url
        self.date = date
        self.source: Optional[str] = None
        self.picture: Optional[str] = None
        self.picture_source: Optional[str] = None

    def __get_data_id(self):
        timestamp = math.floor(self.date.timestamp())
        items = [self.label, self.url, timestamp]
        items_str = json.dumps(items)
        return hash_to_id(items_str)

    def get_cleartext(self):
        return {
            'label': self.label,
            'url': self.url,
            'pub_date': math.floor(self.date.timestamp()),
            'item_source': self.source,
            'picture_url': self.picture,
            'picture_source': self.picture_source,
            'group': self.group,
        }

    def get_pub_date(self):
        return self.date

    def produce_data(self, job: ProcessJob, files: Optional[dict]):
        cleartext = self.get_cleartext()
        encrypted_data = chiffrer_mgs4_bytes_secrete(job.encryption_key, json.dumps(cleartext))[1]
        encrypted_data['cle_id'] = job.encryption_key_id

        data_item = {
            'data_id': self.__get_data_id(),
            'feed_id': job.feed['feed_id'],
            'feed_view_id': job.view['feed_view_id'],
            'encrypted_data': encrypted_data,
            'pub_date': math.floor(self.get_pub_date().timestamp() * 1000),   # Pub date in millisecs
            'group_id': hash_to_id(json.dumps(self.group)),
        }

        if self.picture and files:
            try:
                picture_info = files[self.picture]
                data_item['files'] = [{
                    'fuuid': picture_info['fuuid'],
                    'decryption': {
                        'cle_id': picture_info['cle_id'],
                        'format': picture_info['format'],
                        'nonce': picture_info['nonce'],
                        'compression': picture_info.get('compression'),
                    },
                }]
            except (IndexError, KeyError):
                pass  # No match

        return data_item


# class DataCollectorGoogleTrendsNewsItem:
#
#     def __init__(self, scraped_item: ScrapedGoogleTrendsNewsItem):
#         self.data = scraped_item
#
#     def get_data_id(self):
#         timestamp = math.floor(self.data.date.timestamp())
#         items = [self.data.label, self.data.url, timestamp]
#         items_str = json.dumps(items)
#         return hash_to_id(items_str)
#
#     def produce_data(self) -> DataCollectorClearData:
#         return {
#             'label': self.data.label,
#             'url': self.data.url,
#             'pub_date': math.floor(self.data.date.timestamp()),
#             'item_source': self.data.source,
#             'picture_url': self.data.picture,
#             'picture_source': self.data.picture_source,
#             'group': self.data.group,
#         }

async def parse_google_trends(data: str):
    with tempfile.TemporaryFile('wt+') as temp_file:
        temp_file.write(data)
        temp_file.seek(0)
        parsed_content: ET = ET.parse(temp_file)

    ns_ht = 'https://trends.google.com/trending/rss'

    root = parsed_content.getroot()

    pub_date: Optional[datetime.datetime] = None
    item_picture: Optional[str] = None
    item_picture_source: Optional[str] = None

    for item in root.findall('./channel/item'):
        group: GroupData = GroupData()
        for child in item:
            if child.tag == '{%s}news_item' % ns_ht:
                news_item_title: Optional[str] = None
                news_item_url: Optional[str] = None
                news_item_picture: Optional[str] = None
                news_item_source: Optional[str] = None

                for news_item in child:
                    if news_item.tag == '{%s}news_item_title' % ns_ht:
                        news_item_title = news_item.text
                    elif news_item.tag == '{%s}news_item_url' % ns_ht:
                        news_item_url = news_item.text
                    elif news_item.tag == '{%s}news_item_picture' % ns_ht:
                        news_item_picture = news_item.text
                    elif news_item.tag == '{%s}news_item_source' % ns_ht:
                        news_item_source = news_item.text

                news_item_scraped = ScrapedGoogleTrendsNewsItem(group, news_item_title, news_item_url, pub_date)
                news_item_scraped.source = news_item_source
                if news_item_picture:
                    news_item_scraped.picture = news_item_picture
                    news_item_scraped.picture_source = news_item_source
                else:
                    news_item_scraped.picture = item_picture
                    news_item_scraped.picture_source = item_picture_source

                # Build the data collector item
                yield news_item_scraped

            elif child.tag == 'title':
                group['label'] = child.text
            elif child.tag == 'pubDate':
                pub_date = parse_date(child.text)
                group['pub_date'] = math.floor(pub_date.timestamp())
            elif child.tag == '{%s}approx_traffic' % ns_ht:
                group['approx_traffic'] = child.text
            elif child.tag == '{%s}picture' % ns_ht:
                item_picture = child.text
            elif child.tag == '{%s}picture_source' % ns_ht:
                item_picture_source = child.text
            pass

def parse_date(date_str: str) -> datetime.datetime:
    return datetime.datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')
