"""
Base API secrets_base_client for Secret Manager APIs
"""
from __future__ import annotations

from google.api_core import exceptions
from google.cloud import secretmanager

from gcp.base_client import BaseClient


class SecretsManagerClient(BaseClient):
    def __init__(self, project_id: str = None):
        super().__init__(project_id=project_id)
        # Create the Secret Manager client.
        self.sm_client = secretmanager.SecretManagerServiceClient()

    def create_secret(self, secret_id: str) -> secretmanager.Secret:
        """
        create a new (blank) secret on GCP Secrets Manager API
        there is no value set for the secret-id
        :param secret_id: str
        :return: secretmanager.Secret
        """
        try:
            create_response = self.sm_client.create_secret(
                request={
                    "parent": f"projects/{self.project_id}",
                    "secret_id": secret_id,
                    "secret": {"replication": {"automatic": {}}},
                }
            )
            print(f"Secret {secret_id} created.")
            return create_response
        except exceptions.AlreadyExists:
            print(f"Secret {secret_id} already exists. Skipping...")

    def create_secret_version(
        self, secret_id: str, data: str
    ) -> secretmanager.SecretVersion:
        """
        create a new version of secret for given secret-id
        :param secret_id: str | id of secret to be updated with a new version
        :param data: str | value to be stored for secret-id
        :return: secretmanager.SecretVersion
        """
        # Build the resource name of the parent secret
        parent = self.sm_client.secret_path(self.project_id, secret_id)
        # Add the secret version.
        response = self.sm_client.add_secret_version(
            request={"parent": parent, "payload": {"data": data.encode("UTF-8")}}
        )
        # Print the new secret version name.
        print(f"Added secret version: {response.name}")
        return response

    def list_secrets(self) -> None:
        """
        List all secrets in the given project.
        """
        for secret in self.sm_client.list_secrets(
            request={"parent": f"projects/{self.project_id}"}
        ):
            print(f"Found secret: {secret.name}")

    def list_secret_versions(self, secret_id: str) -> None:
        """
        List all secret versions in the given secret and their metadata.
        :param secret_id: str | id of secret for which versions are listed
        """
        # Build the resource name of the parent secret.
        parent = self.sm_client.secret_path(self.project_id, secret_id)

        # List all secret versions.
        for version in self.sm_client.list_secret_versions(request={"parent": parent}):
            print(f"Found secret version: {version.name}")

    def get_secret_data(self, secret_id, version_id: str | int = "latest") -> str:
        """
        get value stored for given secret-id
        :param secret_id: str | id of secret to be accessed
        :param version_id: str|int | (optional) version of secret to be accessed
        :return: str
        """
        if isinstance(version_id, str) and version_id != "latest":
            version_id = "latest"
        secret_detail = (
            f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
        )
        response = self.sm_client.access_secret_version(request={"name": secret_detail})
        return response.payload.data.decode("UTF-8")

    def delete_secret(self, secret_id: str) -> None:
        """
        Delete the secret with the given name and all of its versions.
        :param secret_id: str | id of secret to be deleted
        :return None
        """

        try:
            # Build the resource name of the secret.
            name = self.sm_client.secret_path(self.project_id, secret_id)

            # Delete the secret.
            self.sm_client.delete_secret(request={"name": name})
        except Exception as e:
            print(f"Exception {e} caught. Cannot delete secret.")

    def destroy_secret_version(
        self, secret_id, version_id: str | int = "latest"
    ) -> secretmanager.SecretVersion:
        """
        Destroy the given secret version, making the payload irrecoverable. Other
        secrets versions are unaffected.
        :param secret_id: str | id of secret to be destroyed
        :param version_id: str|int | (optional) version of secret to be destroyed
        :return : secretmanager.SecretVersion
        """
        # Build the resource name of the secret version
        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"

        # Destroy the secret version.
        response = self.sm_client.destroy_secret_version(request={"name": name})

        print(f"Destroyed secret version: {response.name}")
        return response

    @staticmethod
    def secret_hash(secret_value: str) -> str:
        """
        hash given secret value
        :param secret_value: str
        :return: str | hashed value
        """
        import hashlib

        # return the sha224 hash of the secret value
        return hashlib.sha224(bytes(secret_value, "utf-8")).hexdigest()


if __name__ == "__main__":

    secrets_manager_client = SecretsManagerClient()
    _secret_id = "SERVER"
    _data = "test"
    _version_id = 1
    _create_response = secrets_manager_client.create_secret(_secret_id)
    _create_version_response = secrets_manager_client.create_secret_version(
        _secret_id, _data
    )
    _get_secret_data_response = secrets_manager_client.get_secret_data(
        _secret_id, _version_id
    )
    server = SecretsManagerClient().get_secret_data("SERVER")
    print(server)
