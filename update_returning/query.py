from django.db import transaction
from django.db.models.query import QuerySet, ValuesListQuerySet, ValuesQuerySet
from django.db.models.query_utils import deferred_class_factory

from django.db.models.sql import UpdateQuery
from django.db.models.sql.compiler import SQLUpdateCompiler, SQLCompiler
from django.db.models.sql.constants import MULTI
from django.db import connections

class SQLUpdateReturningCompiler(SQLUpdateCompiler):
    def as_sql(self):
        sql, params = super(SQLUpdateReturningCompiler,self).as_sql()
        sql = sql.rstrip() + ' RETURNING ' + ', '.join(self.get_returning_columns())
        return sql, params
        
    def get_returning_columns(self):
        return self.get_columns(False)[0]
    
    def execute_sql(self, result_type):
        return super(SQLUpdateCompiler,self).execute_sql(result_type)
    
class UpdateReturningQuery(UpdateQuery):
    
    compiler_class = SQLUpdateReturningCompiler
    
    def get_compiler(self, using=None, connection=None):
        """ we need our own compiler """
        if using is None and connection is None:
            raise ValueError('Need either using or connection')
        if using:
            connection = connections[using]
        return self.compiler_class(self, connection, using)
    
class UpdateReturningMethods(object):
    """
    Extends querysets with methods to return rows from sql updates.
    """
    

    def _clone(self, klass=None, setup=False, **kwargs):
        """ Changing a given klass to the matching update_returning one. """

        overwrites = {
            'QuerySet' : UpdateReturningQuerySet,
            'ValuesQuerySet' : UpdateReturningValuesQuerySet,
            'ValuesListQuerySet' : UpdateReturningValuesListQuerySet,
        }

        if klass and klass.__name__ in overwrites:
            klass = overwrites[klass.__name__]

        return super(UpdateReturningMethods,self)._clone(klass,setup,**kwargs)

    def update_returning(self, **kwargs):
        """
        An update that returns the rows that have been updated as an iterator.
        The type of the return objects can be handled by preciding queryset methods like
        in normal querysets.
        Preciding methods that change the type of result items are "only", "defer", "values_list"
        and "values", if none those is used the result items will full model instances. 
        For example a model.objects.values_list('id',flat=True).update_returning(published=True)
        will return a iterator with the ids of the changed objects.
        """
        self._for_write = True

        query = self.query.clone(UpdateReturningQuery)
        query.add_update_values(kwargs)
        
        if not transaction.is_managed(using=self.db):
            transaction.enter_transaction_management(using=self.db)
            forced_managed = True
        else:
            forced_managed = False
        try:
            cursor = query.get_compiler(self.db).execute_sql(MULTI)
            if forced_managed:
                transaction.commit(using=self.db)
            else:
                transaction.commit_unless_managed(using=self.db)
        finally:
            if forced_managed:
                transaction.leave_transaction_management(using=self.db)

        self._result_cache = None

        result_factory = self._returning_update_result_factory()
        
        for rows in cursor:
            for row in rows:
                yield result_factory(row)


    def update_returning_list(self,**kwargs):
        return list(self.update_returning(**kwargs))
        
    def _returning_update_result_factory(self):
        return lambda x:x

class UpdateReturningQuerySet(UpdateReturningMethods, QuerySet):
    
    def _returning_update_result_factory(self):
        """ returns a mapper function to convert the iterated rows into model instances
        or defered models instance depending on the use of "only" or "defer"
        """
        fill_cache = False # always False for now!
        only_load = self.query.get_loaded_field_names()
        fields = self.model._meta.fields

        load_fields = []
        if only_load:
            for field, model in self.model._meta.get_fields_with_model():
                if model is None:
                    model = self.model
                try:
                    if field.name in only_load[model]:
                        # Add a field that has been explicitly included
                        load_fields.append(field.name)
                except KeyError:
                    # Model wasn't explicitly listed in the only_load table
                    # Therefore, we need to load all fields from this model
                    load_fields.append(field.name)
        skip = None
        if load_fields:
            skip = set()
            init_list = []
            for field in fields:
                if field.name not in load_fields:
                    skip.add(field.attname)
                else:
                    init_list.append(field.attname)
            model_cls = deferred_class_factory(self.model,skip)

        assert self._for_write, "_for_write must be True"
        db = self.db 

        if skip:
            factory = lambda row: model_cls(**dict(zip(init_list,row)))
        else:
            model = self.model
            factory = lambda row: model(*row)

        def mapper(row):
            obj = factory(row)
            obj._state.db = db
            obj._state.adding = False
            return obj

        return mapper
            

class UpdateReturningValuesQuerySet(UpdateReturningMethods, ValuesQuerySet):
    def _returning_update_result_factory(self):
        field_names = self.field_names
        return lambda x:dict(zip(field_names,x))

class UpdateReturningValuesListQuerySet(UpdateReturningMethods, ValuesListQuerySet):
    
    def _returning_update_result_factory(self):
        if self.flat and len(self._fields) == 1:
            return lambda x:x[0]
        else:
            return tuple
