import pytest

from api import util

@pytest.fixture(scope='function', params=[
    #range header content     expected_output
    ('bytes=1-5',       [(1, 5)]),
    ('bytes=-5',        [(-5, None)]),
    ('bytes=5-',        [(5, None)]),
    ('bytes=-',         util.ParseError),
    ('bytes=3',         util.ParseError),
    ('bytes=a-b',         util.ParseError),
    ('by-',             util.ParseError),
    ('bytes=5+5',   util.ParseError),
    ('bytes=5=',   util.ParseError),
    ('b=1-5',        util.ParseError),
    ('bytes=1-5, 6-10, 10-15',   [(1, 5), (6, 10), (10, 15)]),
    ('bytes=-5, 6-, 10-15',   [(-5, None), (6, None), (10, 15)]),
    ('bytes=15, 6-10, 10-15',   util.ParseError),
    ('bytes=15, -6--10, 10-15',   util.ParseError),
    ('bytes=1-5; 6-10; 10-15',   util.ParseError),
])
def parse_range_header_fixture(request):
    header, expected_output = request.param
    return header, expected_output


def test_parse_range_header(parse_range_header_fixture):
    input, expected_output = parse_range_header_fixture

    if expected_output == util.ParseError:
        with pytest.raises(expected_output):
            util.parse_range_header(input)
    else:
        assert util.parse_range_header(input) == expected_output


def test_hrsize():
    assert util.hrsize(999) == '999B'
    assert util.hrsize(1000) == '1.0KB'
    for i, suffix in enumerate('KMGTPEZY'):
        assert util.hrsize(2**(10*i + 10)) == '1.0{}B'.format(suffix)
        assert util.hrsize(2**(10*i + 10) * 10) == '10{}B'.format(suffix)
    assert util.hrsize(2**80 * 1000) == '1000YB'


def test_mongo_sanitize_fields():
    obj = object()

    input_fields = {
        1: 1,
        2.0: 2.0,
        'foo.bar$baz': 'foo.bar$baz',
        'list': [1, 2.0, 'foo.bar$baz', obj],
        'obj': obj,
    }

    expected_fields = {
        '1': 1,
        '2_0': 2.0,
        'foo_bar-baz': 'foo.bar$baz',
        'list': [1, 2.0, 'foo_bar-baz', obj],
        'obj': obj,
    }

    assert util.mongo_sanitize_fields(input_fields) == expected_fields


def test_deep_update():
    d = {
        'old': 1,
        'both': 1,
        'dict': {
            'old': 1,
            'both': 1,
        },
    }

    u = {
        'both': 2,
        'new': 2,
        'dict': {
            'both': 2,
            'new': 2,
        },
    }

    util.deep_update(d, u)

    assert d == {
        'old': 1,
        'both': 2,
        'new': 2,
        'dict': {
            'old': 1,
            'both': 2,
            'new': 2,
        },
    }


def test_enum():
    # create test enum class
    TestEnum = util.Enum('TestEnum', {
        'foo': 1,
        'bar': 2,
    })

    # test __eq__
    assert TestEnum.foo == TestEnum.foo
    assert TestEnum.foo == 'foo'
    assert TestEnum.foo == u'foo'

    # test __ne__
    assert TestEnum.foo != TestEnum.bar
    assert TestEnum.foo != 'bar'
    assert TestEnum.foo != u'bar'

    # test __str__
    assert str(TestEnum.foo) == 'foo'
