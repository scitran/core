import pytest
from api.auth.apikeys import APIKey

def test_api_key_preprocess():
    assert APIKey._preprocess_key("key")             == "key"
    assert APIKey._preprocess_key("preamble:key")    == "key"
    assert APIKey._preprocess_key("preamble:37:key") == "key"
    assert APIKey._preprocess_key("preamble::key")   == "key"
