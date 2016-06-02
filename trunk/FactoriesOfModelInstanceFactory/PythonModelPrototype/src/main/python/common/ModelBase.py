from abc import ABCMeta

class ModelBase:
    __metaclass__ = ABCMeta

    @abc.abstractmethod
    def ModelName(self):
        """Answer the model's name."""

    @abc.abstractmethod
    def ModelName(self):
        """Answer the model's version."""

    @abc.abstractmethod
    def TenantId(self):
        """Answer the model's owner."""

    @abc.abstractmethod
    def OwnerId(self):
        """Answer the model's owner."""

    @abc.abstractmethod
    def TransId(self):
        """Answer the transaction id for this model instance."""

    @abc.abstractmethod
    def init(self, partitionHash):
        """Instance initialization. Once per instance."""

    @abc.abstractmethod
    def execute(self, outputDefault):
        """if outputDefault is true we will output the default value if nothing matches, otherwise null."""

    @abc.abstractmethod
    def isModelInstanceReusable(self):
        """Can the instance created for this model be reused on subsequent transactions?"""
        return True


