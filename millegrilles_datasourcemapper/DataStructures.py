import datetime
import pathlib

from typing import Optional, TypedDict, Union

from millegrilles_messages.messages.MessagesModule import MessageWrapper


class AttachedFile(TypedDict):
    fuuid: str
    format: str
    nonce: str
    cle_id: Optional[str]
    compression: Optional[str]


class AttachedFileInterface:

    def encrypt_upload_file(self, secret_key: bytes, fp) -> AttachedFile:
        """
        Encrypts and uploads a file to the filehost
        :param secret_key: Secret encryption key
        :param fp: File handle at the proper position for reading content
        :return:
        """
        raise NotImplementedError('interface method - must override')

    async def download_file(self, fuuid: str, fp) -> int:
        """
        Downloads a file without decrypting it.
        :param fuuid:
        :param fp:
        :return:
        """
        raise NotImplementedError('interface method - must override')

    async def download_decrypt_file(self, fuuid: str, decrypted_key: Union[str, bytes], decryption_params: dict, fp) -> int:
        """
        Downloads and decrypts a file.
        :param fuuid:
        :param decrypted_key:
        :param decryption_params:
        :param fp:
        :return:
        """
        raise NotImplementedError('interface method - must override')

class AttachedFileCorrelation:

    def __init__(self, correlation: str):
        self.correlation = correlation
        self.fuuid: Optional[str] = None
        self.format: Optional[str] = None
        self.cle_id: Optional[str] = None
        self.nonce: Optional[str] = None
        self.compression: Optional[str] = None

    def to_attached_file(self) -> AttachedFile:
        if self.fuuid is None or self.format is None or self.nonce is None:
            raise ValueError('Missing at least one of fuuid, format or nonce')

        return {
            "fuuid": self.fuuid,
            "format": self.format,
            "nonce": self.nonce,
            "cle_id": self.cle_id,
            "compression": self.compression
        }

    def map_key(self) -> Optional[str]:
        """
        Use to produce a key: fuuid map that will be encrypted when the key is sensitive (e.g. user name, url, etc)
        :return:
        """
        return None

    def map_volatile(self, data: dict):
        self.fuuid = data.get('fuuid')
        self.format = data.get('format')
        self.nonce = data.get('nonce')
        self.cle_id = data.get('cle_id')
        self.compression = data.get('compression')


class CustomProcessOutput:

    def __init__(self):
        self.pub_date_start: Optional[datetime.datetime] = None
        self.pub_date_end: Optional[datetime.datetime] = None
        self.files: Optional[list[AttachedFileCorrelation]] = None


class DataCollectorTransaction(TypedDict):
    feed_id: str
    data_id: str
    save_date: int
    data_fuuid: str
    key_ids: list[str]
    pub_date_start: Optional[int]
    pub_date_end: Optional[int]
    attached_fuuids: Optional[list[str]]


class DataFeedFile(TypedDict):
    feed_id: str
    data_id: str
    save_date: int
    encrypted_data: dict
    pub_start_date: Optional[int]
    pub_end_date: Optional[int]
    files: Optional[list[AttachedFile]]
    encrypted_files_map: Optional[dict]


class Filehost:

    def __init__(self, filehost_id: str):
        self.filehost_id = filehost_id
        self.url_internal: Optional[str] = None
        self.url_external: Optional[str] = None
        self.tls_external: Optional[str] = None
        self.instance_id: Optional[str] = None
        self.deleted: Optional[bool] = None
        self.sync_active: Optional[bool] = None

    @staticmethod
    def load_from_dict(value: dict):
        filehost_id = value['filehost_id']
        filehost = Filehost(filehost_id)

        filehost.url_internal = value.get('url_internal')
        filehost.url_external = value.get('url_external')
        filehost.tls_external = value.get('tls_external')
        filehost.instance_id = value.get('instance_id')
        filehost.deleted = value.get('deleted')
        filehost.sync_active = value.get('sync_active')

        return filehost

    @staticmethod
    def init_new(filehost_id: str, instance_id: str, url: str):
        filehost = Filehost(filehost_id)
        filehost.url_internal = url
        filehost.instance_id = instance_id
        filehost.deleted = False
        filehost.sync_active = True
        return filehost


class ProcessJob:

    def __init__(self, message: MessageWrapper):
        self.message = message
        self.reset = False
        self.feed: Optional[dict] = None
        self.view: Optional[dict] = None
        self.decrypted_view_information: Optional[dict] = None
        self.encryption_key_id: Optional[str] = None
        self.encryption_key_str: Optional[str] = None
        self.encryption_key: Optional[bytes] = None
        self.data_file_path: Optional[pathlib.Path] = None

    def __copy__(self):
        job = ProcessJob(self.message)
        job.reset = self.reset
        job.feed = self.feed
        job.view = self.view
        job.decrypted_view_information = self.decrypted_view_information
        job.encryption_key_id = self.encryption_key_id
        job.encryption_key_str = self.encryption_key_str
        job.encryption_key = self.encryption_key
        job.data_file_path = self.data_file_path
        return job

    def copy(self):
        return self.__copy__()
