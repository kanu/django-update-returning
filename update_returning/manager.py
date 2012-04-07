from django.db.models.manager import Manager
from query import UpdateReturningQuerySet

__all__ = ('UpdateReturningManager','UpdateReturningDefaultManager')

class UpdateReturningManager(Manager):
    """ A manager that uses the UpdateReturningQuerySet. """
    def get_query_set(self):
        return UpdateReturningQuerySet(self.model,using=self._db)
    
    def update_returning(self, *args, **kwargs):
        return self.get_query_set().update_returning(*args, **kwargs)
        
        
class UpdateReturningDefaultManager(UpdateReturningManager):
    """ A manager that uses the UpdateReturningQuerySet that will be used for
    accessing related objects.
    """
    use_for_related_fields = True