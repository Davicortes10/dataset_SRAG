from pre_processamento import PreprocessDataset
from data_lake import Data_Lake

bot = PreprocessDataset()
datalake = Data_Lake("/home/davicortes_oliveira1/dataset_SRAG/pre_processamento/srag.csv","srag_datalake")
