import numpy as np
import requests
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.utils import to_categorical
from sklearn.metrics import classification_report,confusion_matrix, accuracy_score



class ClassificacaoEvolucao:
    def __init__(self, dataset):
        self.dataset = dataset
        self.scaler = StandardScaler()
        self.dados = ()

    def filtrar_dados(self):
        """Remove registros com EVOLUCAO = 9 (Ignorado)."""
        return self.dataset[self.dataset['EVOLUCAO'] != 9]

    def preprocessamento(self, df):
        """Realiza normalização e codificação da variável alvo."""
        X = self.scaler.fit_transform(df[['NU_IDADE_N', 'QTD_FATOR_RISC', 'QTD_SINT']])
        y = to_categorical(df['EVOLUCAO'] - 1)  # Ajustando índices para one-hot encoding
        return X, y

    def dividir_dados(self, X, y):
        """Divide os dados em treino e teste."""
        return train_test_split(X, y, test_size=0.2, random_state=42)

    def criar_modelo(self, input_dim):
        """Cria e retorna o modelo de rede neural."""
        model = Sequential([
            Dense(64, activation='relu', input_shape=(input_dim,)),  # Camada oculta 1
            Dense(32, activation='relu'),  # Camada oculta 2
            Dense(3, activation='softmax')  # Camada de saída (3 classes)
        ])
        model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
        return model

    def treinar_modelo(self, model, X_train, y_train):
        """Treina a rede neural."""
        early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
        history = model.fit(
            X_train, y_train,
            epochs=100, batch_size=32, validation_split=0.2,
            callbacks=[early_stopping], verbose=1
        )
        return history

    def avaliar_modelo(self, model, X_test, y_test):
        """Avalia o modelo e gera métricas de desempenho."""
        y_pred = model.predict(X_test)
        y_pred_classes = np.argmax(y_pred, axis=1)
        y_test_classes = np.argmax(y_test, axis=1)

        # Exibir métricas
        print("Acurácia:", accuracy_score(y_test_classes, y_pred_classes))
        print("\n🔍 Relatório de Classificação:\n", classification_report(
            y_test_classes, y_pred_classes,
            target_names=['MELHORA DE QUADRO', 'ÓBITO', 'ÓBITO POR OUTRAS CAUSAS']
        ))
        # Calcular métricas
        acuracia = accuracy_score(y_test_classes, y_pred_classes)
        relatorio_classificacao = classification_report(
            y_test_classes,
            y_pred_classes,
            target_names=['MELHORA DE QUADRO', 'ÓBITO', 'ÓBITO POR OUTRAS CAUSAS'],
            output_dict=True  # Para obter o relatório como um dicionário
        )
        matriz_confusao = confusion_matrix(y_test_classes, y_pred_classes).tolist()
        self.enviar_dados({
            "acuracia": acuracia,
            "relatorio_classificacao": relatorio_classificacao,
            "matriz_confusao": matriz_confusao
        })

    def enviar_dados(self,dados):
        """Envia as métricas para o endpoint Django."""
        url = "https://7c96-186-216-47-142.ngrok-free.app/api/armazenar_dados/"  # Substitua pela URL do seu endpoint
        try:
            response = requests.post(url, json=dados)
            if response.status_code == 200:
                print("Dados enviados com sucesso!")
                print("Resposta do servidor:", response.json())
            else:
                print(f"Erro ao enviar dados. Status code: {response.status_code}")
                print("Resposta:", response.text)
        except Exception as e:
            print("Erro ao enviar os dados:", e)




    def executar_classificacao(self):
        """Executa todo o pipeline de classificação da evolução do paciente."""
        df = self.filtrar_dados()
        X, y = self.preprocessamento(df)
        X_train, X_test, y_train, y_test = self.dividir_dados(X, y)

        model = self.criar_modelo(X_train.shape[1])
        self.treinar_modelo(model, X_train, y_train)
        self.avaliar_modelo(model, X_test, y_test)

