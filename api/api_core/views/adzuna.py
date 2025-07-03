from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from api_core.models import Adzuna
import statistics

class MedianSalaryView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        salaries = Adzuna.objects.values_list('salary', flat=True)
        median_salary = statistics.median(salaries) if salaries else None
        return Response({"median_salary": median_salary})
