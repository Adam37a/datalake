from django.urls import path
from api_core.views.adzuna import MedianSalaryView
from api_core.views.glassdoor import CompetenceFrequencyView
from api_core.views.google_trends import GoogleTrendsListView

urlpatterns = [
    path('adzuna/median-salary/', MedianSalaryView.as_view(), name='median-salary'),
    path('glassdoor/competences/', CompetenceFrequencyView.as_view(), name='competences-frequency'),
    path('google-trends/', GoogleTrendsListView.as_view(), name='google-trends-list'),
]
