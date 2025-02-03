# 🏥 Mineração de Dados sobre SRAG (Síndrome Respiratória Aguda Grave)

Este projeto tem como objetivo **analisar e extrair insights valiosos** do dataset da **Síndrome Respiratória Aguda Grave (SRAG)**. Utilizamos **técnicas de mineração de dados**, **aprendizado de máquina** e **estatística descritiva** para entender padrões, prever riscos e fornecer suporte à tomada de decisão.

---

## **📊 Objetivos do Projeto**
✅ Identificar padrões em pacientes com SRAG.  
✅ Limpar e **pré-processar** os dados para remover inconsistências.  
✅ Criar **modelos preditivos** para análise de risco.  
✅ Gerar **visualizações interativas** para facilitar a interpretação dos dados.  
✅ Integrar os dados com um **banco de dados GCP** para melhor armazenamento e escalabilidade.

---

## **🛠️ Tecnologias Utilizadas**
🚀 **Linguagem**: Python  
📊 **Bibliotecas Principais**:
- `pandas` → Manipulação e análise de dados.
- `numpy` → Operações matemáticas e vetorização.
- `scikit-learn` → Modelagem preditiva e aprendizado de máquina.
- `matplotlib` / `seaborn` → Visualização de dados.
- `pyspark` → Processamento de grandes volumes de dados.
- `sqlalchemy` → Integração com banco de dados (GCP MySQL).
- `playwright` → Automação de download de datasets.
- `pymysql` → Conexão com banco de dados MySQL.

---

## **📂 Estrutura do Projeto**
📁 `dataset/` → Armazena os arquivos brutos e tratados do SRAG.  
📁 `pre_processamento/` → Scripts para limpeza e transformação dos dados.   
📜 `README.md` → Documentação do projeto.  

---

## **🔄 Pipeline de Processamento**
1️⃣ **Obtenção dos Dados** → Download e carregamento do dataset.  
2️⃣ **Pré-Processamento** → Tratamento de valores nulos, conversão de tipos e remoção de outliers.  
3️⃣ **Análise Exploratória** → Estatísticas descritivas e visualizações.  
4️⃣ **Modelagem Preditiva** → Algoritmos de aprendizado de máquina para prever gravidade dos casos.  
5️⃣ **Integração com Banco de Dados** → Armazenamento no **MySQL do Google Cloud Platform (GCP)**.  
6️⃣ **Geração de Relatórios** → Insights sobre a evolução da SRAG.

---

## **🖥️ Como Executar o Projeto**
1️⃣ **Clone o repositório**:
```sh
git clone https://github.com/seu_usuario/projeto-srag.git
cd projeto-srag
