"""
class to use Google Cloud Storage APIs
"""

from gcp.base_client import BaseClient
from app.models.models import GcsUploadResults, GcpResponse
from google.cloud import storage
import os
import posixpath
import json
import yaml
from typing import Union, List


class StorageManager(BaseClient):
    def __init__(self, project_id: str = None):
        super().__init__(project_id)
        self.gcs_client = storage.Client()

    def list_buckets(self) -> list:
        """
        list all buckets in GCP project
        :return:
        """
        buckets = self.gcs_client.list_buckets()
        return list(buckets)

    def upload_file(
        self,
        file_path: str,
        bucket: str,
        save_in_subdir: str = None,
        delete_file_after_upload: bool = False,
        upload_all_files_in_dir: bool = False,
    ) -> Union[GcsUploadResults, List[GcsUploadResults]]:
        """
        upload file to GCS bucket, use sub-dir path (if provided)
        the sub-dir path can be used if the file is to be stored in a dir structure within the bucket
        :param file_path: str | path of file to be uploaded
        :param bucket: str | bucket-name
        :param save_in_subdir: str | (optional) sub-dir path
        :param delete_file_after_upload: bool | (optional) delete the file from local storage after upload?
        :param upload_all_files_in_dir: bool | (optional) if a dir is passed in file_path, then upload all files in dir?
        :return: Union[GcsUploadResults, List[GcsUploadResults]]
        """
        if os.path.isfile(file_path):
            try:
                bucket = self.gcs_client.bucket(bucket)
                blob = self.get_blob_for_file_path(bucket, file_path, save_in_subdir)
                blob.upload_from_filename(file_path)
                if delete_file_after_upload:
                    os.remove(file_path)
                    print(f"INFO: File-delete after upload is selected: {file_path} deleted.")
            except Exception as e:
                raise e
            return GcsUploadResults(
                project_id=self.project_id,
                bucket_id=bucket.name,
                response=GcpResponse(
                    **{
                        "status": "Success",
                        "message": f"{file_path} uploaded to {blob}, local file deleted: {delete_file_after_upload}",
                    },
                ),
            )
        elif os.path.isdir(file_path) and upload_all_files_in_dir:
            from pathlib import Path

            path_glob = Path(file_path).glob("**/*")
            return [
                self.upload_file(
                    file_path=_file.name,
                    bucket=bucket,
                    save_in_subdir=file_path,
                    delete_file_after_upload=True,
                )
                for _file in path_glob
                if _file.is_file()
            ]
        elif os.path.isdir(file_path) and not upload_all_files_in_dir:
            raise FileNotFoundError(
                f"File not found. {file_path} is a dir. "
                f"Set upload_all_files_in_dir=True to enable upload of files in dir."
            )

    @staticmethod
    def get_blob_for_file_path(bucket, json_file_path, save_in_subdir):
        from app.utils.file_utils import get_filename_from_path

        file_name = get_filename_from_path(json_file_path)
        if save_in_subdir:
            file_name = os.path.join(save_in_subdir, file_name).replace(
                os.sep, posixpath.sep
            )
        return bucket.blob(file_name)

    def get_storage_file_as_text(
        self, bucket: str, file_path: str, file_type: str = None, encoding: str = None
    ):
        """
        get file from GCS bucket as text
        :param bucket:
        :param file_path:
        :param file_type:
        :param encoding:
        :return:
        """
        encoding = encoding or "utf-8"
        try:
            bucket = self.gcs_client.get_bucket(bucket)
            blob = bucket.get_blob(file_path)
            if not file_type:
                return blob.download_as_text(encoding=encoding)
            elif "json" in file_type:
                return json.loads(blob.download_as_text(encoding=encoding))
            elif "yaml" in file_type or "yml" in file_type:
                return yaml.safe_load(blob.download_as_text(encoding=encoding))
        except Exception as e:
            raise e


if __name__ == "__main__":

    def _test_upload():
        from app.utils.file_utils import get_project_path

        json_file = (
            r"processed\_bBEsUyu6ZNcLPbxAcGAxXZRF9io_7gckOD2En3Oz_k_processed.json"
        )
        json_file = os.path.join(get_project_path("data"), json_file)

        gcs_man = StorageManager().upload_file(
            bucket="test-bucket-pramit",
            file_path=json_file,
            save_in_subdir="test/processed/",
            delete_file_after_upload=False,
        )
        print(gcs_man)

    def _test_list_buckets():
        buckets_list = StorageManager().list_buckets()
        print(buckets_list)

    def _test_get_gcs_files_as_text():
        result = StorageManager().get_storage_file_as_text(
            bucket="test-bucket-pramit",
            file_path="credentials/credentials.yml",
            file_type="yml",
        )
        print(result)

    _test_upload()
    _test_list_buckets()
    _test_get_gcs_files_as_text()
