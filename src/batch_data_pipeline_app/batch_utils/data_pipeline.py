from abc import ABC, abstractmethod

class AbstractDataPipeline(ABC):

    @abstractmethod
    def execute_pipeline(self):
        raise NotImplementedError
        
    @abstractmethod
    def extract(self):
        raise NotImplementedError
    
    @abstractmethod
    def load(self):
        raise NotImplementedError
        