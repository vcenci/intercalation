O programa foi escrito em Python, versão 3.12.2, sem o uso de libs externas.

[Dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store?select=2019-Oct.csv)

O dataset é particionado e classificado em dois arquivos de até 5 milhões de registros. 
Um dos arquivos contém os dados do usuário, classificado pelo user_id, o outro contém os dados do produto, classificado pelo product_id.
Ao intercalar os arquivos é gerada uma chave única sequencial, para criação do índice parcial para cada um dos arquivos de dados gerados. 
A intercalação acontece através de uma fila de prioridade.
O último byte de cada arquivo de dados é reservado para definir se o dado foi removido ou não. 
Não foi implementada a limpeza dos arquivos removidos. O arquivo de índices permanece inalterado com as remoções.
A inclusão de registros acontece de forma ordenada, e os índices são recriados a cada inclusão, de acordo com os seguintes passos:
1 - Pesquisa binária no arquivo de índices, para determinar onde o novo registro deve ser adicionado.
2 - Cálculo do offset, no arquivo de dados, para a inserção ordenada.
3 - A partir do offset, calcula-se quantos bytes precisam ser movidos para abrir espaço para a adição.
4 - Leitura de 5 milhões de registros para a memória, que são movidos para a direita, para abrir espaçõ para o novo registro.
5 - Incrementação das chaves movidas.
6 - Escrita do novo registro no espaço destinado.
7 - Reescrita dos índices, de forma exaustiva.

Através do arquivo de índices podemos saber quantas interações um usuário teve com o site, e também qual foi o usuário com mais interações. O mesmo vale para o índice de produtos.
Foi criada uma função para retornar o produto e o usuário que mais aparecem no dataset. Essa função pode ser ajustada para levar em consideração ações específicas, como compra ou visualização .
