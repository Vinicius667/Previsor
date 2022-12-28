# Previsor_Operacao_Comercial
Previsor de data de início da operação comercial das usinas. 


1. O arquivo 0_Formatar_BD.ipynb  foi utilizado para fazer o tramento das informações provenientes do bdworgs.
2. O arquivo 1_merge_BDs.ipynb foi utilizado para juntar as informações provenientes do bdworgs e SKATE
3. O arquivo 2_Previsor.ipynb foi utilizado para fazer tratamento de dados na base, além de calcular os coeficientes do previsor.
4. O arquivo 3_Calcular_Previsao.ipynb é o arquivo que é utilizado para calcular previsões OC.
5. O arquivo download_DB.py contém uma função que baixa as informações do SKATE. É utilizado no script 3_Calcular_Previsao.ipynb.
6. O arquivo requirements_python_3_10_4.txt contém uma lista as bibliotecas necessárias para executar os scripts.