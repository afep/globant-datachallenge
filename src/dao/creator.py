from abc import ABC, abstractmethod

class Creator(ABC):
    """
    The Creator class declares the factory method that is supposed to return an
    object of a Product class. The Creator's subclasses usually provide the
    implementation of this method.
    """

    @abstractmethod
    def factory_orm_insert_data(self):
        """
        Method definition to implement insert data 
        """
        pass
