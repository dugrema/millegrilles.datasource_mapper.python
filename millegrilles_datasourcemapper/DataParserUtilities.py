import binascii
import datetime
import json
import math

from typing import Optional, TypedDict, Union

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

    def __init__(self, label: str, date: int,
                 data_str: Optional[dict[str, str]] = None,
                 data_number: Optional[dict[str, Union[int, float]]] = None,
                 associated_urls: Optional[dict[str, str]] = None):
        self.label = label
        """ Item label. This can be a title or short description """

        self.date = date
        """ Item date in epoch seconds, for example the publication date. """

        self.data_str = data_str
        """ Dict of string values """

        self.data_number = data_number
        """ Dict of number values """

        self.associated_urls = associated_urls
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


def parse_date(date_str: str) -> int:
    parsed_date = datetime.datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')
    return math.floor(parsed_date.timestamp())
