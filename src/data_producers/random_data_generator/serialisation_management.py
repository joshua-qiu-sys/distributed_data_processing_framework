from confluent_kafka.serialization import SerializationContext
from typing import Dict
from src.data_producers.random_data_generator.serialisations import AbstractSerialisation, ConfluentKafkaStringSerialisation, ConfluentKafkaAvroSerialisation
from src.data_producers.random_data_generator.schema_registry_connectors import AbstractSchemaRegistryClient
from src.utils.constructors import AbstractFactory, AbstractFactoryRegistry
from src.utils.cfg_management import AbstractCfgManager

ACCEPTED_SERIALISATIONS = {
    'confluent_kafka': {
        'string': ConfluentKafkaStringSerialisation,
        'avro': ConfluentKafkaAvroSerialisation
    }   
}

class SerialisationFactoryRegistry(AbstractFactoryRegistry):
    def __init__(self):
        super().__init__()

    def lookup_registry(self, serialisation_library: str, serialisation_type: str) -> AbstractSerialisation:
        if not self.is_registered(serialisation_library=serialisation_library, serialisation_type=serialisation_type):
            return KeyError(f'Serialisation type "{serialisation_type}" of serialisation library {serialisation_library} not found in factory registry')
        serialisation = self._registry[serialisation_library][serialisation_type]
        return serialisation

    def is_registered(self, serialisation_library: str, serialisation_type: str) -> bool:
        if serialisation_library in self._registry.keys():
            if serialisation_type in self._registry[serialisation_library].keys():
                return True
            else:
                return False
        else:
            return False

    def register(self, serialisation_library: str, serialisation_type: str, serialisation: AbstractSerialisation) -> None:
        if not self.is_registered(serialisation_library=serialisation_library, serialisation_type=serialisation_type):
            
            if serialisation_library not in self._registry.keys():
                self._registry[serialisation_library] = {
                    serialisation_type: serialisation
                }
            elif len(self._registry[serialisation_library]) == 0:
                self._registry[serialisation_library][serialisation_type] = serialisation
            else:
                serialisation_library_entry = self._registry[serialisation_library]
                serialisation_library_entry.update({
                    serialisation_type: serialisation
                })
                self._registry.update(serialisation_library_entry)

    def register_defaults(self, default_serialisation_dict: Dict = ACCEPTED_SERIALISATIONS) -> None:

        for serialisation_library in default_serialisation_dict.keys():
            for serialisation_type, serialisation in default_serialisation_dict[serialisation_library].items():
                self.register(serialisation_library=serialisation_library, serialisation_type=serialisation_type, serialisation=serialisation)

    def deregister(self, serialisation_library: str, serialisation_type: str) -> None:
        if not self.is_registered(serialisation_library=serialisation_library, serialisation_type=serialisation_type):
            return KeyError(f'Serialisation type "{serialisation_type}" of serialisation library {serialisation_library} not found in factory registry')
        self._registry[serialisation_library].pop(serialisation_type)
        
    def reset_registry(self):
        self._registry.clear()
    
class SerialisationFactory(AbstractFactory):
    def __init__(self, factory_registry: SerialisationFactoryRegistry):
        super().__init__(factory_registry)

    def create(self, serialisation_library: str, serialisation_type: str) -> AbstractSerialisation:
        if not self.factory_registry.is_registered(serialisation_library=serialisation_library, serialisation_type=serialisation_type):
            raise KeyError(f'Serialisation type "{serialisation_type}" of serialisation library {serialisation_library} not found in factory registry')
        serialisation_cls = self.factory_registry.lookup_registry(serialisation_library=serialisation_library, serialisation_type=serialisation_type)
        serialisation = serialisation_cls()
        return serialisation
    
class SerialisationCfgManager(AbstractCfgManager):
    def __init__(self,
                 key_serialisation_factory_cfg: Dict,
                 val_serialisation_factory_cfg: Dict,
                 key_serialiser_cfg: Dict,
                 key_deserialiser_cfg: Dict,
                 key_serialisation_cfg: Dict,
                 key_deserialisation_cfg: Dict,
                 val_serialiser_cfg: Dict,
                 val_deserialiser_cfg: Dict,
                 val_serialisation_cfg: Dict,
                 val_deserialisation_cfg: Dict):
        
        self.key_serialisation_factory_cfg = key_serialisation_factory_cfg
        self.val_serialisation_factory_cfg = val_serialisation_factory_cfg
        self.key_serialiser_cfg = key_serialiser_cfg
        self.key_deserialiser_cfg = key_deserialiser_cfg
        self.key_serialisation_cfg = key_serialisation_cfg
        self.key_deserialisation_cfg = key_deserialisation_cfg
        self.val_serialiser_cfg = val_serialiser_cfg
        self.val_deserialiser_cfg = val_deserialiser_cfg
        self.val_serialisation_cfg = val_serialisation_cfg
        self.val_deserialisation_cfg = val_deserialisation_cfg

    def get_key_serialisation_factory_cfg(self) -> Dict:
        return self.key_serialisation_factory_cfg
    
    def get_val_serialisation_factory_cfg(self) -> Dict:
        return self.val_serialisation_factory_cfg
    
    def get_key_serialiser_cfg(self) -> Dict:
        return self.key_serialiser_cfg
    
    def get_key_deserialiser_cfg(self) -> Dict:
        return self.key_deserialiser_cfg
    
    def get_key_serialisation_cfg(self) -> Dict:
        return self.key_serialisation_cfg
    
    def get_key_deserialisation_cfg(self) -> Dict:
        return self.key_deserialisation_cfg
    
    def get_val_serialiser_cfg(self) -> Dict:
        return self.val_serialiser_cfg
    
    def get_val_deserialiser_cfg(self) -> Dict:
        return self.val_deserialiser_cfg
    
    def get_val_serialisation_cfg(self) -> Dict:
        return self.val_serialisation_cfg
    
    def get_val_deserialisation_cfg(self) -> Dict:
        return self.val_deserialisation_cfg

class SerialisationHandler:
    def __init__(self,
                 schema_registry_client: AbstractSchemaRegistryClient,
                 serialisation_factory: SerialisationFactory,
                 serialisation_cfg_manager: SerialisationCfgManager):
        
        self.schema_registry_client = schema_registry_client
        self.serialisation_factory = serialisation_factory
        self.serialisation_cfg_manager = serialisation_cfg_manager
        self.key_serialisation = None
        self.val_serialisation = None

    def setup_serialisation(self) -> None:
        key_serialisation = self.serialisation_factory.create(**self.serialisation_cfg_manager.get_key_serialisation_factory_cfg())
        self.key_serialisation = key_serialisation
        val_serialisation = self.serialisation_factory.create(**self.serialisation_cfg_manager.get_val_serialisation_factory_cfg())
        self.val_serialisation = val_serialisation

    def get_key_serialisation(self) -> AbstractSerialisation:
        return self.key_serialisation
    
    def get_val_serialisation(self) -> AbstractSerialisation:
        return self.val_serialisation