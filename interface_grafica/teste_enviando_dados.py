import requests

url = "https://e8e9-186-216-47-142.ngrok-free.app/api/armazenar_dados/"  # Substitua pelo URL do ngrok

dados = {
    "chave": "valor",
    "idade": 25,
    "nome": "Teste"
}

resposta = requests.post(url, json=dados)

