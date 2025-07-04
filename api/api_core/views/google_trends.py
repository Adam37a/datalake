from django.db import connection
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated

class GoogleTrendsListView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        geo_name = request.query_params.get('geoName', None)

        # Construire la requête SQL
        sql = 'SELECT * FROM "google_trends"'
        params = []

        if geo_name:
            sql += ' WHERE "geoName" = %s'
            params.append(geo_name)

        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            columns = [col[0] for col in cursor.description]
            data = [dict(zip(columns, row)) for row in cursor.fetchall()]

        return Response(data)
