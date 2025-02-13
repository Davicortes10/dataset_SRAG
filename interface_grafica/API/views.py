from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
import json


# Create your views here.
dados_armazenados = {}

@csrf_exempt
def armazenar_dados(request):
    if request.method == 'POST':
        # Lê os dados enviados no corpo da requisição
        dados = json.loads(request.body)
        dados_armazenados['dados_ml'] = dados
        print(f"Dados recebidos: {dados}")
        return JsonResponse({"status": "sucesso", "dados": dados}, status=201)
    else:
        return JsonResponse({"erro": "Método não permitido"}, status=405)

@csrf_exempt
def obter_dados(request):
    if request.method == 'GET':
        # Lê os dados enviados no corpo da requisição

        dados = dados_armazenados.get('dados_ml', {})
        return  JsonResponse(dados)
    else:
        return JsonResponse({"erro": "Método não permitido"}, status=405)