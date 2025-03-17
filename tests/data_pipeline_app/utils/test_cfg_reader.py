import pytest
from pathlib import Path
import os
from collections import namedtuple
from data_pipeline_app.utils.cfg_reader import BaseCfgReader, IniCfgReader, YamlCfgReader

CfgFileFixture = namedtuple('CfgFileFixture', ['cfg_path', 'cfg_content'])

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
    _generate_test_file(file_path=plain_cfg_path, content=test_plain_cfg_content)

    plain_cfg_file_fixture = CfgFileFixture(cfg_path=plain_cfg_path, cfg_content=test_plain_cfg_content)

    def cleanup():
        _remove_test_file(file_path=plain_cfg_path)

    request.addfinalizer(cleanup)

    return plain_cfg_file_fixture

def test_base_cfg_reader_read_file(plain_cfg_file):

    plain_cfg_file_path = plain_cfg_file.cfg_path
    plain_cfg_file_content = plain_cfg_file.cfg_content

    base_cfg_reader = BaseCfgReader()
    actual_cfg = base_cfg_reader.read_file(file_path=plain_cfg_file_path)
    expected_cfg = plain_cfg_file_content
    assert actual_cfg == expected_cfg