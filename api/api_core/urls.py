from django.urls import path
from api_core.views.adzuna import MedianSalaryView
from api_core.views.glassdoor import CompetenceFrequencyView

urlpatterns = [
    path('adzuna/median-salary/', MedianSalaryView.as_view(), name='median-salary'),
    path('glassdoor/competences/', CompetenceFrequencyView.as_view(), name='competences-frequency'),
]
