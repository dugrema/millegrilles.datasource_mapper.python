import logging

from typing import Any, Callable, Coroutine, Optional

from cryptography.x509 import ExtensionNotFound

from millegrilles_datasourcemapper.Context import DatasourceMapperContext
from millegrilles_datasourcemapper.DataSourceManager import DatasourceManager
from millegrilles_messages.messages.Constantes import KIND_EVENEMENT
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_messages.messages import Constantes
from millegrilles_messages.bus.BusContext import MilleGrillesBusContext
from millegrilles_messages.bus.PikaChannel import MilleGrillesPikaChannel
from millegrilles_messages.bus.PikaQueue import MilleGrillesPikaQueueConsumer, RoutingKey

class BusMessageHandler:

    def __init__(self, context: DatasourceMapperContext, datasource_manager: DatasourceManager):
        self.__logger = logging.getLogger(__name__+'.'+self.__class__.__name__)
        self.__context = context
        self.__datasource_manager = datasource_manager

    async def setup(self):
        channel_triggers = create_trigger_q_channel(self.__context, self.on_trigger)
        channel_exclusive = create_exclusive_q_channel(self.__context, self.on_exclusive_message)
        channel_volatile = create_volatile_channel(self.__context, self.on_volatile_message)

        await self.__context.bus_connector.add_channel(channel_triggers)
        await self.__context.bus_connector.add_channel(channel_exclusive)
        await self.__context.bus_connector.add_channel(channel_volatile)

    async def on_trigger(self, message: MessageWrapper) -> Optional[dict]:
        return None

    async def on_exclusive_message(self, message: MessageWrapper) -> Optional[dict]:

        # Authorization check - 3.protege/CoreTopologie
        enveloppe = message.certificat
        try:
            domaines = enveloppe.get_domaines
        except ExtensionNotFound:
            domaines = list()
        try:
            exchanges = enveloppe.get_exchanges
        except ExtensionNotFound:
            exchanges = list()

        # if 'CoreTopologie' in domaines and Constantes.SECURITE_PROTEGE in exchanges:
        #     pass  # CoreTopologie
        # else:
        #     return  # Ignore message

        action = message.routage['action']
        payload = message.parsed

        # if action == 'deleteFiles':
        #     return await self.__filehost_manager.delete_files(message)

        self.__logger.info("on_exclusive_message Ignoring unknown action %s" % action)
        return {'ok': False, 'code': 404, 'err': 'Unkown action'}


    async def on_volatile_message(self, message: MessageWrapper) -> Optional[dict]:
        message_kind = message.kind

        # Authorization check - 3.protege/DataCollector
        enveloppe = message.certificat
        try:
            domaines = enveloppe.get_domaines
        except ExtensionNotFound:
            domaines = list()
        try:
            exchanges = enveloppe.get_exchanges
        except ExtensionNotFound:
            exchanges = list()

        action = message.routage['action']

        try:
            if message_kind == Constantes.KIND_COMMANDE:
                if 'DataCollector' in domaines and Constantes.SECURITE_PROTEGE in exchanges and action == 'processFeedView':
                    return await self.__datasource_manager.process_feed_view(message, True)
            elif message_kind == Constantes.KIND_EVENEMENT:
                if 'DataCollector' in domaines and Constantes.SECURITE_PROTEGE in exchanges and action == 'feedDataUpdated':
                    return await self.__datasource_manager.process_feed_view(message, False)
        except Exception as e:
            # Unhandled error
            self.__logger.exception('Unhandled exception')
            return {'ok': False, 'code': 500, 'err': str(e)}

        self.__logger.info("on_volatile_message Ignoring unknown action %s" % action)
        return {'ok': False, 'code': 404, 'err': 'Unkown action'}


def create_trigger_q_channel(context: MilleGrillesBusContext, on_message: Callable[[MessageWrapper], Coroutine[Any, Any, None]]) -> MilleGrillesPikaChannel:
    # System triggers
    channel = MilleGrillesPikaChannel(context, prefetch_count=1)
    queue = MilleGrillesPikaQueueConsumer(context, on_message, 'datasource_mapper/triggers',
                                              arguments={'x-message-ttl': 90000}, allow_user_messages=True)
    channel.add_queue(queue)
    queue.add_routing_key(RoutingKey(Constantes.SECURITE_PROTEGE, f'evenement.ceduleur.{Constantes.EVENEMENT_PING_CEDULE}'))

    return channel


def create_exclusive_q_channel(context: MilleGrillesBusContext, on_message: Callable[[MessageWrapper], Coroutine[Any, Any, None]]) -> MilleGrillesPikaChannel:
    channel = MilleGrillesPikaChannel(context, prefetch_count=20)
    queue = MilleGrillesPikaQueueConsumer(context, on_message, None, exclusive=True, arguments={'x-message-ttl': 300000})
    channel.add_queue(queue)
    queue.add_routing_key(RoutingKey(Constantes.SECURITE_PROTEGE, 'commande.datasource_mapper.stopFeedViewRun'))

    return channel


def create_volatile_channel(context: MilleGrillesBusContext, on_message: Callable[[MessageWrapper], Coroutine[Any, Any, None]]) -> MilleGrillesPikaChannel:
    channel = MilleGrillesPikaChannel(context, prefetch_count=1)
    queue = MilleGrillesPikaQueueConsumer(context, on_message, 'datasource_mapper/volatile',
                                              arguments={'x-message-ttl': 30000}, allow_user_messages=True)
    channel.add_queue(queue)
    queue.add_routing_key(RoutingKey(Constantes.SECURITE_PROTEGE, 'commande.datasource_mapper.processFeedView'))
    queue.add_routing_key(RoutingKey(Constantes.SECURITE_PROTEGE, 'evenement.DataCollector.feedDataUpdated'))

    return channel
