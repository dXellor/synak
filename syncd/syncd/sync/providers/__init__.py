from syncd.sync.registry import registry
from syncd.sync.providers.p2p import P2PProvider
from syncd.sync.providers.client_server import ClientServerProvider

registry.register(P2PProvider)
registry.register(ClientServerProvider)
