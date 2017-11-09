
import copy
from api.jobs import gears

# DISCUSS: this basically asserts that the log helper doesn't throw, which is of non-zero but questionable value.
# Could instead be marked for pytest et. al to ignore coverage? Desirability? Compatibility?
def test_fill_defaults():

    gear_config = {
        'key_one':      {'default': 1},
        'key_two':      {'default': 2},
        'key_three':    {'default': 3},
        'key_no_de':    {}
    }

    gear = {
        'gear': {
            'config': gear_config
        }
    }

    # test sending in complete config does not change
    config = {
        'key_one': 4,
        'key_two': 5,
        'key_three': 6
    }

    result = gears.fill_gear_default_values(gear, config)
    assert result['key_one'] == 4
    assert result['key_two'] == 5
    assert result['key_three'] == 6

    # test sending in empty config
    result = gears.fill_gear_default_values(gear, {})
    assert result['key_one'] == 1
    assert result['key_two'] == 2
    assert result['key_three'] == 3

    # test sending in None config
    result = gears.fill_gear_default_values(gear, None)
    assert result['key_one'] == 1
    assert result['key_two'] == 2
    assert result['key_three'] == 3

    # test sending in semi-complete config
    config = {
        'key_one': None,
        'key_two': []
        #'key_three': 6 # missing
    }

    result = gears.fill_gear_default_values(gear, config)
    assert result['key_one'] == None
    assert result['key_two'] == []
    assert result['key_three'] == 3
