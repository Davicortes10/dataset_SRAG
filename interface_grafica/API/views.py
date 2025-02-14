from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
import json


# Create your views here.
dados_armazenados = None

@csrf_exempt
def armazenar_dados(request):
    global dados_armazenados
    if request.method == "POST":
        # Recebe os dados enviados pelo modelo de ML
        dados_armazenados = json.loads(request.body)
        return JsonResponse({"message": "Dados recebidos com sucesso!"}, status=200)

    elif request.method == "GET":
        # Retorna os dados armazenados para o Tkinter
        if dados_armazenados:
            return JsonResponse(dados_armazenados, status=200)
        else:
            return JsonResponse({"error": "Nenhum dado disponível."}, status=404)

    return JsonResponse({"error": "Método não permitido."}, status=405)

@csrf_exempt
def enviar_dados(request):
    if request.method == "GET":
        if dados_armazenados:
            return JsonResponse(dados_armazenados, status=200)
        else:
            return JsonResponse({"error": "Nenhum dado disponível."}, status=404)
    return JsonResponse({"error": "Método não permitido."}, status=405)