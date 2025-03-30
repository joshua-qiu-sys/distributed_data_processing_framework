import pytest
from pathlib import Path
import os
from collections import namedtuple
from src.utils.cfg_reader import BaseCfgReader, IniCfgReader, YamlCfgReader

CfgFileFixture = namedtuple('CfgFileFixture', ['cfg_path', 'cfg_content', 'expected'])
JinjaCfgFileFixture = namedtuple('CfgFileFixture', ['cfg_path', 'cfg_content', 'cfg_vars', 'expected'])

def _generate_test_file(file_path: Path, content: str) -> None:
    file_path_prefix = os.path.dirname(file_path)

    if not os.path.exists(file_path_prefix):
        os.makedirs(file_path_prefix)

    with open(file_path, 'w') as f:
        f.write(content)

def _remove_test_file(file_path: Path) -> None:
    os.remove(file_path)

@pytest.fixture(scope='function')
def plain_cfg_file(request) -> CfgFileFixture:

    plain_cfg_path = Path('tmp', 'plain.conf')

    test_plain_cfg_content = """
        config_id = 1
        config_description = test configuration
    """

    test_cfg_expected = """
        config_id = 1
        config_description = test configuration
    """

    _generate_test_file(file_path=plain_cfg_path, content=test_plain_cfg_content)

    plain_cfg_file_fixture = CfgFileFixture(cfg_path=plain_cfg_path, cfg_content=test_plain_cfg_content, expected=test_cfg_expected)

    def cleanup():
        _remove_test_file(file_path=plain_cfg_path)

    request.addfinalizer(cleanup)

    return plain_cfg_file_fixture

@pytest.fixture(scope='function')
def ini_cfg_file(request) -> CfgFileFixture:

    ini_cfg_path = Path('tmp', 'conf.ini')

    test_ini_cfg_content = """
        [DEFAULT]
        config_id = 1
        config_description = test configuration 1
        [OTHER]
        config_id = 2
        config_description = test configuration 2
    """

    test_ini_cfg_expected = {
        'DEFAULT': {
            'config_id': '1',
            'config_description': 'test configuration 1'
        },
        'OTHER': {
            'config_id': '2',
            'config_description': 'test configuration 2'
        }
    }

    _generate_test_file(file_path=ini_cfg_path, content=test_ini_cfg_content)

    ini_cfg_file_fixture = CfgFileFixture(cfg_path=ini_cfg_path, cfg_content=test_ini_cfg_content, expected=test_ini_cfg_expected)

    def cleanup():
        _remove_test_file(file_path=ini_cfg_path)

    request.addfinalizer(cleanup)

    return ini_cfg_file_fixture

@pytest.fixture(scope='function')
def ini_jinja_cfg_file(request) -> CfgFileFixture:

    ini_jinja_cfg_path = Path('tmp', 'jinja_conf.ini')

    test_ini_jinja_cfg_content = """
        [DEFAULT]
        config_id = 1
        config_description = {{ config_description }}
        [OTHER]
        config_id = 2
        config_description = test configuration 2
    """

    test_cfg_vars = {
        'config_description': 'test configuration 1'
    }

    test_ini_jinja_cfg_expected = {
        'DEFAULT': {
            'config_id': '1',
            'config_description': 'test configuration 1'
        },
        'OTHER': {
            'config_id': '2',
            'config_description': 'test configuration 2'
        }
    }

    _generate_test_file(file_path=ini_jinja_cfg_path, content=test_ini_jinja_cfg_content)

    ini_jinja_cfg_file_fixture = JinjaCfgFileFixture(cfg_path=ini_jinja_cfg_path, cfg_content=test_ini_jinja_cfg_content, cfg_vars=test_cfg_vars, expected=test_ini_jinja_cfg_expected)

    def cleanup():
        _remove_test_file(file_path=ini_jinja_cfg_path)

    request.addfinalizer(cleanup)

    return ini_jinja_cfg_file_fixture

@pytest.fixture(scope='function')
def yml_cfg_file(request) -> CfgFileFixture:

    yml_cfg_path = Path('tmp', 'conf.yml')

    test_yml_cfg_content = """
        default:
            config_id: 1
            config_description: test configuration 1
        other:
            config_id: 2
            config_description: test configuration 2
    """

    test_yml_cfg_expected = {
        'default': {
            'config_id': 1,
            'config_description': 'test configuration 1'
        },
        'other': {
            'config_id': 2,
            'config_description': 'test configuration 2'
        }
    }

    _generate_test_file(file_path=yml_cfg_path, content=test_yml_cfg_content)

    yml_cfg_file_fixture = CfgFileFixture(cfg_path=yml_cfg_path, cfg_content=test_yml_cfg_content, expected=test_yml_cfg_expected)

    def cleanup():
        _remove_test_file(file_path=yml_cfg_path)

    request.addfinalizer(cleanup)

    return yml_cfg_file_fixture

@pytest.fixture(scope='function')
def yml_jinja_cfg_file(request) -> CfgFileFixture:

    yml_jinja_cfg_path = Path('tmp', 'jinja_conf.yml')

    test_yml_jinja_cfg_content = """
        default:
            config_id: 1
            config_description: {{ config_description }}
        other:
            config_id: 2
            config_description: test configuration 2
    """

    test_yml_jinja_cfg_vars = {
        'config_description': 'test configuration 1'
    }

    test_yml_jinja_cfg_expected = {
        'default': {
            'config_id': 1,
            'config_description': 'test configuration 1'
        },
        'other': {
            'config_id': 2,
            'config_description': 'test configuration 2'
        }
    }

    _generate_test_file(file_path=yml_jinja_cfg_path, content=test_yml_jinja_cfg_content)

    yml_jinja_cfg_file_fixture = JinjaCfgFileFixture(cfg_path=yml_jinja_cfg_path, cfg_content=test_yml_jinja_cfg_content, cfg_vars=test_yml_jinja_cfg_vars, expected=test_yml_jinja_cfg_expected)

    def cleanup():
        _remove_test_file(file_path=yml_jinja_cfg_path)

    request.addfinalizer(cleanup)

    return yml_jinja_cfg_file_fixture

def test_base_cfg_reader_read_file(plain_cfg_file: CfgFileFixture):

    plain_cfg_file_path = plain_cfg_file.cfg_path
    plain_cfg_file_content = plain_cfg_file.cfg_content
    plain_cfg_expected = plain_cfg_file.expected

    base_cfg_reader = BaseCfgReader()
    actual_cfg = base_cfg_reader.read_file(file_path=plain_cfg_file_path)
    expected_cfg = plain_cfg_expected
    assert actual_cfg == expected_cfg

def test_ini_cfg_reader_read_cfg(ini_cfg_file: CfgFileFixture):

    ini_cfg_file_path = ini_cfg_file.cfg_path
    ini_cfg_file_content = ini_cfg_file.cfg_content
    ini_cfg_expected = ini_cfg_file.expected

    ini_cfg_reader = IniCfgReader()
    actual_cfg = ini_cfg_reader.read_cfg(file_path=ini_cfg_file_path, interpolation=None)
    expected_cfg = ini_cfg_expected
    assert actual_cfg == expected_cfg

def test_ini_jinja_cfg_reader_read_cfg(ini_jinja_cfg_file: JinjaCfgFileFixture):

    ini_jinja_cfg_file_path = ini_jinja_cfg_file.cfg_path
    ini_jinja_cfg_file_content = ini_jinja_cfg_file.cfg_content
    ini_jinja_cfg_cfg_vars = ini_jinja_cfg_file.cfg_vars
    ini_jinja_cfg_expected = ini_jinja_cfg_file.expected

    ini_jinja_cfg_reader = IniCfgReader()
    actual_cfg = ini_jinja_cfg_reader.read_jinja_templated_cfg(file_path=ini_jinja_cfg_file_path, interpolation=None, cfg_vars=ini_jinja_cfg_cfg_vars)
    expected_cfg = ini_jinja_cfg_expected
    assert actual_cfg == expected_cfg

def test_yml_cfg_reader_read_cfg(yml_cfg_file: CfgFileFixture):

    yml_cfg_file_path = yml_cfg_file.cfg_path
    yml_cfg_file_content = yml_cfg_file.cfg_content
    yml_cfg_expected = yml_cfg_file.expected

    yml_cfg_reader = YamlCfgReader()
    actual_cfg = yml_cfg_reader.read_cfg(file_path=yml_cfg_file_path)
    expected_cfg = yml_cfg_expected
    assert actual_cfg == expected_cfg

def test_yml_jinja_cfg_reader_read_cfg(yml_jinja_cfg_file: JinjaCfgFileFixture):

    yml_jinja_cfg_file_path = yml_jinja_cfg_file.cfg_path
    yml_jinja_cfg_file_content = yml_jinja_cfg_file.cfg_content
    yml_jinja_cfg_cfg_vars = yml_jinja_cfg_file.cfg_vars
    yml_jinja_cfg_expected = yml_jinja_cfg_file.expected

    yml_jinja_cfg_reader = YamlCfgReader()
    actual_cfg = yml_jinja_cfg_reader.read_jinja_templated_cfg(file_path=yml_jinja_cfg_file_path, cfg_vars=yml_jinja_cfg_cfg_vars)
    expected_cfg = yml_jinja_cfg_expected
    assert actual_cfg == expected_cfg