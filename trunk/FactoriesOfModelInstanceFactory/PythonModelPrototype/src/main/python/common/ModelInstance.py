
import abc
from common.ModelBase import ModelBase
import json

class ModelInstance(ModelBase): 
    """ initialize the model instance (the super class for 
    all the python models) with the supplied model info JSON
    popped off the front of supplied addModel inputs """

    def __init__(self, modelInfo):
        self.jsonModelInfo = modelInfo #string rep 
        self.modelInfo = [] #json.loads(modelInfo) #json => dictionary
        partitionHash = modelInfo["partitionHash"]
        init(partitionHash)

    #def ModelName(self):
    def ModelName(self):
        """Answer the model's name."""
        nm = modelInfo["ModelName"]
        return nm

    def Version(self):
        """Answer the model's version."""
        ver = modelInfo["Version"]
        return ver

    def Tenant(self):
        """Answer the model's owner."""
        tenant = modelInfo["Tenant"]
        return tenant

    def OwnerId(self):
        """Answer the model's owner."""
        return Tenant()

    def TransId(self):
        """Answer the transaction id for this model instance."""
        transId = modelInfo["TransId"]
        return transId

    def init(self, partitionHash):
        """Instance initialization. Once per instance."""
        self.partitionHash = partitionHash

    @abc.abstractmethod
    def execute(self, outputDefault):
        """if outputDefault is true we will output the default value if nothing matches, otherwise null."""

    def isModelInstanceReusable(self):
        """Can the instance created for this model be reused on subsequent transactions?"""
        return super().isModelInstanceReusable()
