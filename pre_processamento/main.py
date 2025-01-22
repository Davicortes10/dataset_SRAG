from pre_processamento import PreprocessDataset

bot = PreprocessDataset("C:/Users/aleanse/Downloads/INFLUD20-01-05-2023.csv")
bot.processar_tipos_colunas()
bot.atualizar_Data_Lake()