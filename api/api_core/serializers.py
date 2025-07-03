from rest_framework import serializers
from .models import Adzuna, Glassdoor

class AdzunaSerializer(serializers.ModelSerializer):
    class Meta:
        model = Adzuna
        fields = ['country', 'date', 'salary']

class GlassdoorSerializer(serializers.ModelSerializer):
    class Meta:
        model = Glassdoor
        fields = ['name', 'description', 'competences']
