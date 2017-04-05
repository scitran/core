import pytest
from api.auth.authproviders import APIKeyAuthProvider

def test_api_key_preprocess():
    assert APIKeyAuthProvider._preprocess_key("key")             == "key"
    assert APIKeyAuthProvider._preprocess_key("preamble:key")    == "key"
    assert APIKeyAuthProvider._preprocess_key("preamble:37:key") == "key"
    assert APIKeyAuthProvider._preprocess_key("preamble::key")   == "key"
