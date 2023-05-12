import httpx
import typing
import httpcore
from httpx import Timeout
from googletrans import Translator
from googletrans.gtoken import TokenAcquirer
from googletrans.constants import DEFAULT_USER_AGENT, DEFAULT_RAISE_EXCEPTION


class Translator_Leonardo(Translator):
    def __init__(self, service_urls=None, user_agent=DEFAULT_USER_AGENT,
                 raise_exception=DEFAULT_RAISE_EXCEPTION,
                 proxies: typing.Dict[str, httpcore.SyncHTTPTransport] = None,
                 timeout: Timeout = None,
                 http2=True):

        self.client = httpx.Client(http2=http2, verify=False)
        if proxies is not None:  # pragma: nocover
            self.client.proxies = proxies

        self.client.headers.update({
            'User-Agent': user_agent,
        })

        if timeout is not None:
            self.client.timeout = timeout

        self.service_urls = service_urls or ['translate.google.com']
        self.token_acquirer = TokenAcquirer(
            client=self.client, host=self.service_urls[0])
        self.raise_exception = raise_exception


def translate(text, service="google"):
    """
    With `googletrans==3.1.0a0`, it can translate multiple sentences of multiple languages on one call.
    """
    if service == "google":
        return _google_translate(text)
    else:
        raise Exception("Unsupported translation service")


def _google_translate(text):
    translator = Translator_Leonardo()
    translator.client_type = 'gtx'
    response = translator.translate(text)
    return [
        {"src": item.src, "dest": item.dest, "text": item.text} for item in response
    ]
