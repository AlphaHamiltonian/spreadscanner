from source.utils import HttpSessionManager

class BaseExchangeConnector:
    """Base class for exchange connectors with common functionality"""
    def __init__(self, app, exchange_name):
        self.app = app
        self.exchange_name = exchange_name
        self.websocket_managers = {}
        
        # Create HTTP session with appropriate headers
        self.session_manager = HttpSessionManager(
            exchange_name,
            base_headers={
                'User-Agent': 'Mozilla/5.0',
                'Accept': 'application/json'
            },
            max_retries=3
        )
        self.session = self.session_manager.session

