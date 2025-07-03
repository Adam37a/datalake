from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from api_core.models import Glassdoor

class CompetenceFrequencyView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        # Lire toutes les valeurs de la colonne 'competences'
        queryset = Glassdoor.objects.values_list('competences', flat=True)
        counter = {}

        for entry in queryset:
            if entry:
                # Décomposer les compétences séparées par des virgules
                skills = [skill.strip().lower() for skill in entry.split(',')]
                for skill in skills:
                    counter[skill] = counter.get(skill, 0) + 1

        # Trier par fréquence décroissante
        sorted_skills = dict(sorted(counter.items(), key=lambda x: x[1], reverse=True))
        return Response(sorted_skills)
