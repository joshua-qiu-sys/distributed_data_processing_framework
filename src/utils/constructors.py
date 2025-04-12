from abc import ABC, abstractmethod

class AbstractFactoryRegistry(ABC):
    def __init__(self):
        self._registry = {}

    @abstractmethod
    def lookup_registry():
        raise NotImplementedError

    @abstractmethod
    def is_registered():
        raise NotImplementedError

    @abstractmethod
    def register():
        raise NotImplementedError
    
    @abstractmethod
    def register_defaults():
        raise NotImplementedError
    
    @abstractmethod
    def deregister():
        raise NotImplementedError
    
    @abstractmethod
    def reset_registry():
        raise NotImplementedError
    
class AbstractFactory(ABC):
    def __init__(self, factory_registry: AbstractFactoryRegistry):
        self.factory_registry = factory_registry

    @abstractmethod
    def create():
        raise NotImplementedError