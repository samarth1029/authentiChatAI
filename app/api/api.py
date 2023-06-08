from app import (
    __version__,
    __appname__,
    __email__,
    __author__,
)


class Api:
    @staticmethod
    def get_app_details() -> dict:
        return {
            "appname": __appname__,
            "version": __version__,
            "email": __email__,
            "author": __author__,
        }



