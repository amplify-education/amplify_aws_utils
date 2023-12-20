"""Module for representing S3 URIs"""
from urllib.parse import urlparse, ParseResult


class S3URI:
    """Class for representing S3 URI"""

    def __init__(self, uri: str):
        """
        Parses a given S3 URI into a bucket and key.
        :param uri: A valid S3 URI. Ex: s3://<bucket>/<key>
        """
        # allow_fragments handles cases where s3 objects might have `#`s in their key
        # https://stackoverflow.com/questions/42641315/s3-urls-get-bucket-name-and-path
        self._parsed: ParseResult = urlparse(url=uri, allow_fragments=False)

    @property
    def bucket(self) -> str:
        """The name of the bucket"""
        return self._parsed.netloc

    @property
    def key(self) -> str:
        """The key inside the bucket"""
        key = self._parsed.path.lstrip("/")
        if self._parsed.query:
            key += "?" + self._parsed.query
        return key

    @property
    def uri(self) -> str:
        """The original URI"""
        return self._parsed.geturl()

    def __repr__(self):
        return f"S3URI(uri='{self.uri}')"

    def __str__(self):
        return self.uri
