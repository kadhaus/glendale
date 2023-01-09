from typing import Dict, Any, Generator, List, Optional

import json

from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from pathlib import Path
from requests import Response

from glendale.exceptions import AuthenticationDataIsOutError
from glendale.log import get_logger


class GoogleAPIClient:
    def __init__(self, config: Dict[str, Any]):
        self._logger = get_logger(__name__)
        self._scopes: List[str] = config['oauth2_scopes']
        self._endpoint: str = config['endpoint']

        self._api_keys_paths = self._api_key_paths_generator(config['api_keys_dir'])
        self._current_credentials: Optional[service_account.Credentials] = None
        self._session: Optional[AuthorizedSession] = None

    def close(self):
        if self._session:
            self._session.close()

    @staticmethod
    def _api_key_paths_generator(api_keys_dir: Path) -> Generator[Path, None, None]:
        for api_key_path in api_keys_dir.iterdir():
            if api_key_path.suffix != '.json':
                continue
            yield api_key_path

    def create_new_session(self) -> AuthorizedSession:
        if self._session is not None:
            self._session.close()
        creds = self.next_credentials
        if creds is None:
            raise AuthenticationDataIsOutError
        self._session = AuthorizedSession(creds)
        return self._session

    @property
    def next_credentials(self) -> Optional[service_account.Credentials]:
        try:
            api_key_path = self._api_keys_paths.__next__()
            credentials: service_account.Credentials = \
                service_account.Credentials.from_service_account_file(
                    api_key_path.as_posix(),
                )
            scoped_credentials = credentials.with_scopes(self._scopes)
            self._current_credentials = scoped_credentials
            return self._current_credentials
        except StopIteration:
            return None

    @property
    def current_credentials(self) -> Optional[service_account.Credentials]:
        if self._current_credentials is None:
            return self.next_credentials
        return self._current_credentials

    @property
    def session(self) -> AuthorizedSession:
        if self._session is None:
            return self.create_new_session()
        return self._session

    def index_request(self, url: str) -> Response:
        content = json.dumps({'url': url.strip(), 'type': "URL_UPDATED"})
        response = self.session.post(self._endpoint, data=content)
        return response
