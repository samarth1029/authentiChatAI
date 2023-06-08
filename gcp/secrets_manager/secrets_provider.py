"""
module provides credentials/secrets to child classes,
has methods for fetching credentials & authentication/validation
"""
from dotenv import load_dotenv

from gcp.secrets_manager import SecretsManagerClient


class MissingCredentials(Exception):
    """
    Credentials are missing, see the error output to find possible causes
    """

    pass


class SecretsProvider(SecretsManagerClient):
    credentials = None

    def __init__(self, credentials=None, project_id: str = None):
        super().__init__(project_id=project_id)
        load_dotenv()
        self.credentials_checked = False
        if not credentials:
            self.get_secrets()
        elif not self.credentials_checked:
            self.credentials = self.RunSecrets(**credentials)
            self._check_all_secrets()

    def get_secrets(self):
        self.credentials = self.RunSecrets(
            api_key=self.get_secret_data("DHL_API_KEY"),
            base_url=self.get_secret_data("DHL_BASE_URL"),
        )
        if self.credentials and self._check_all_secrets():
            return True

    def _check_all_secrets(self) -> bool:
        """
        check if all credentials needed are already fetched
        sets class member credentials_checked=True after verifying
        :return: bool
        """
        _missing = self.RunSecrets(**self.credentials.__dict__).check_run_secrets()
        if len(_missing):
            _missing_msg = f"The following credentials are missing: {_missing}"
            raise MissingCredentials(
                f"{_missing_msg}. Create credentials on GCP secrets manager for project {self.project_id}."
            )
        else:
            self.credentials_checked = True
            return True

    class RunSecrets:
        """
        class for abstracting secrets for a run of the package
        """
        def __init__(
            self,
            api_key,
            base_url
        ):
            self.api_key = api_key
            self.base_url = base_url

        def check_run_secrets(self):
            return [k for k, v in self.__dict__.items() if not v]
