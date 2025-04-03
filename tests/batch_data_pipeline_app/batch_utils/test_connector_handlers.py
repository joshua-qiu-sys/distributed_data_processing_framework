import pytest
from typing import List
from collections import namedtuple
from batch_data_pipeline_app.batch_utils.connector_handlers import ConnectorCfgHandler, ConnectorSelectionHandler
from batch_data_pipeline_app.batch_utils.connectors import LocalFileConnector, PostgreSQLConnector

RawConnectorCfgFixture = namedtuple('RawConnectorCfgFixture', ['connector_type', 'direction', 'raw_cfg', 'expected'])
raw_conn_cfg_fixtures = [
    RawConnectorCfgFixture(connector_type='local_file',
                        direction='source',
                        raw_cfg={'connector_type': 'local_file', 'dataset_name': 'dataset1', 'file_type': 'parquet', 'file_path': 'data/raw/dataset1', 'read_props': {'mergeSchema': False}},
                        expected={'file_path': 'data/raw/dataset1', 'file_type': 'parquet', 'read_props': {'mergeSchema': False}}
    ),
    RawConnectorCfgFixture(connector_type='local_file',
                        direction='sink',
                        raw_cfg={'connector_type': 'local_file', 'file_type': 'parquet', 'file_path': 'data/processed/dataset2', 'write_mode': 'overwrite', 'write_props': {'compression': 'snappy'}},
                        expected={'file_path': 'data/processed/dataset2', 'file_type': 'parquet', 'write_mode': 'overwrite', 'write_props': {'compression': 'snappy'}}
    ),
    RawConnectorCfgFixture(connector_type='postgres',
                        direction='source',
                        raw_cfg={'connector_type': 'postgres', 'conn_id': 'DEFAULT', 'schema': 'dev', 'db_table': 'dev.dataset1', 'read_props': {'numPartitions': 10}},
                        expected={'db_conf_conn_id': 'DEFAULT', 'schema': 'dev', 'db_table': 'dev.dataset1', 'read_props': {'numPartitions': 10}}
    ),
    RawConnectorCfgFixture(connector_type='postgres',
                        direction='sink',
                        raw_cfg={'connector_type': 'postgres', 'conn_id': 'DEFAULT', 'schema': 'dev', 'db_table': 'dev.dataset1', 'write_mode': 'overwrite', 'write_props': {'numPartitions': 10, 'batchSize': 10000}},
                        expected={'db_conf_conn_id': 'DEFAULT', 'schema': 'dev', 'db_table': 'dev.dataset1', 'write_mode': 'overwrite', 'write_props': {'numPartitions': 10, 'batchSize': 10000}}
    )
]

ProcessedConnectorCfgFixture = namedtuple('ProcessedConnectorCfgFixture', ['connector_type', 'processed_cfg'])
processed_conn_cfg_fixtures = [
    ProcessedConnectorCfgFixture(connector_type='local_file',
                                 processed_cfg={'file_path': 'data/raw/dataset1', 'file_type': 'parquet', 'read_props': {'mergeSchema': False}}
    ),
    ProcessedConnectorCfgFixture(connector_type='local_file',
                                 processed_cfg={'file_path': 'data/raw/dataset1', 'file_type': 'parquet', 'read_props': {'mergeSchema': False}}
    )
]

@pytest.fixture(scope='module', params=raw_conn_cfg_fixtures)
def raw_connector_cfg(request) -> RawConnectorCfgFixture:
    return request.param

@pytest.fixture(scope='module', params=processed_conn_cfg_fixtures)
def processed_connector_cfg(request) -> ProcessedConnectorCfgFixture:
    return request.param

def test_prepare_processed_cfg_for_connector(raw_connector_cfg: RawConnectorCfgFixture):
    connector_type = raw_connector_cfg.connector_type
    direction = raw_connector_cfg.direction
    raw_cfg = raw_connector_cfg.raw_cfg
    expected_connector_cfg = raw_connector_cfg.expected
    
    connector_cfg_handler = ConnectorCfgHandler(direction=direction, raw_connector_cfg=raw_cfg)
    actual_connector_cfg = connector_cfg_handler.prepare_processed_cfg_for_connector()
    assert actual_connector_cfg == expected_connector_cfg

def test_get_req_connector(processed_connector_cfg: ProcessedConnectorCfgFixture):
    connector_type = processed_connector_cfg.connector_type
    processed_connector_cfg = processed_connector_cfg.processed_cfg

    connector_select_handler = ConnectorSelectionHandler(connector_type=connector_type, processed_connector_cfg=processed_connector_cfg)
    connector = connector_select_handler.get_req_connector()

    connector_map = {
        'local_file': LocalFileConnector,
        'postgres': PostgreSQLConnector
    }

    assert isinstance(connector, connector_map[connector_type])