=======================
Django update_returning
=======================

This module has a model manager with an update_returning method that returns an iterator over the changed rows of a sql update.
It only works for Postgresql as this offers a RETURNING clause in an UPDATE query.
http://www.postgresql.org/docs/8.4/interactive/sql-update.html

You can use the values, values_list, only and defer methods ahead of the update_returning call to set the type of returned rows.

Basic usage::

	from udpate_returning import UpdateReturningManager

	class AModel(models.Model):
	
		name = models.CharField(blank=True, max_length=100)
		flag = models.NullBooleanField(default=True)

		objects = UpdateReturningManager()

	# updates according to the filter and update parameters and returns the changed objects.
	updated = AModel.objects.filter(flag__isnull=True).update_returning(flag=False)	

	# updates according to the filter and update parameters and returns the ids of the changed objects.
	updated = AModel.object.filter(flag__isnull=True).values_list('id',flat=True).update_returning(flag=False)





