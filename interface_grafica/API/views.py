from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
import json


# Create your views here.
# Armazenamento em memória (ou substitua por banco de dados, se preferir)
dados_metrica = {}
dados_graficos = {}

@csrf_exempt
def armazenar_dados(request):
    """
    View que recebe as métricas enviadas pela classe ClassificacaoEvolucao.
    """
    if request.method == 'POST':
        try:
            dados = json.loads(request.body)
            global dados_metrica
            dados_metrica = dados  # Armazena em memória
            return JsonResponse({"mensagem": "Métricas armazenadas com sucesso!"}, status=200)
        except Exception as e:
            return JsonResponse({"erro": str(e)}, status=400)
    return JsonResponse({"erro": "Método não permitido. Use POST."}, status=405)

@csrf_exempt
def armazenar_graficos(request):
    """
    View que recebe os dados dos gráficos enviados pela classe ClassificacaoEvolucao.
    """
    if request.method == 'POST':
        try:
            dados = json.loads(request.body)
            global dados_graficos
            dados_graficos = dados  # Armazena em memória
            return JsonResponse({"mensagem": "Dados de gráficos armazenados com sucesso!"}, status=200)
        except Exception as e:
            return JsonResponse({"erro": str(e)}, status=400)
    return JsonResponse({"erro": "Método não permitido. Use POST."}, status=405)

def fornecer_dados(request):
    """
    View que fornece as métricas armazenadas para o Tkinter.
    """
    if request.method == 'GET':
        if dados_metrica:
            return JsonResponse(dados_metrica, status=200)
        return JsonResponse({"erro": "Nenhuma métrica disponível."}, status=404)
    return JsonResponse({"erro": "Método não permitido. Use GET."}, status=405)

def fornecer_graficos(request):
    """
    View que fornece os dados dos gráficos armazenados para o Tkinter.
    """
    if request.method == 'GET':
        if dados_graficos:
            return JsonResponse(dados_graficos, status=200)
        return JsonResponse({"erro": "Nenhum dado de gráfico disponível."}, status=404)
    return JsonResponse({"erro": "Método não permitido. Use GET."}, status=405)