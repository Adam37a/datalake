from django.db import models

class Adzuna(models.Model):
    country = models.CharField(max_length=2)
    date = models.DateField()
    salary = models.FloatField()

    class Meta:
        managed = False
        db_table = 'adzuna'

class Glassdoor(models.Model):
    name = models.CharField(max_length=255)
    description = models.TextField()
    competences = models.TextField(db_column='compétences')

    class Meta:
        managed = False
        db_table = 'glassdoor'

