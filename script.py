import sqlalchemy
import pandas as pd

# 1. Extract

# criação da engine do sql alchemy para a tabela de origem
engine_origin = sqlalchemy.create_engine(
    'postgresql+pg8000://natalia:123456@localhost:5432/origin',
    client_encoding='utf8',
)

# extração das tabelas para dataframes do pandas
fato_pedidos = pd.read_sql(sql='SELECT * FROM fato_pedidos', con=engine_origin)
dim_produtos = pd.read_sql(sql='SELECT * FROM dim_produtos', con=engine_origin)
dim_lojas = pd.read_sql(sql='SELECT * FROM dim_lojas', con=engine_origin)

# 2. Transform

# merge
# left join do dataframe fatos_pedidos com o dataframe dim_produtos, em uma novo dataframe chamada fato_dw
fato_dw = pd.merge(
    left=fato_pedidos,
    right=dim_produtos,
    how='left',
    left_on='produto',
    right_on='id',
)
# left join da tabela recém-criada fato_dw com a tabela dim_lojas, sobrescrevendo a fato_dw
fato_dw = pd.merge(
    left=fato_dw, right=dim_lojas, how='left', left_on='loja', right_on='id'
)

# remoção das colunas desnecessárias na tabela
fato_dw.drop(columns=['id', 'id_x', 'id_y', 'produto_x', 'loja'], inplace=True)

# ordenação das colunas
fato_dw = fato_dw[
    ['produto_y', 'valor', 'data', 'estado', 'cidade', 'logradouro']
]

# alteração dos títulos das colunas
fato_dw.rename(
    columns={
        'produto_y': 'Produto',
        'valor': 'Preço',
        'data': 'Data',
        'estado': 'Estado',
        'cidade': 'Cidade',
        'logradouro': 'Endereço',
    },
    inplace=True,
)

# 3. Load

# criação da engine do sql alchemy para a tabela de destino
engine_destiny = sqlalchemy.create_engine(
    'postgresql+pg8000://natalia:123456@localhost:5432/destiny',
    client_encoding='utf8',
)

# calculo do chunksize => o chunksize é a quantidade de linhas que será carregada por vez na tabela de destino durante a fase de load
# o chunksize não deve passar de 2097 células então, para determinar a quantidade de linhas, dividimos 2097 pelo número de colunas
cs = 2097 // len(fato_dw.columns)

# o chunksize não pode passar de 1000, por isso, fazemos uma verificação
cs = 1000 if cs > 1000 else cs

# exportação do dataframe do pandas para a tabela de destino
fato_dw.to_sql(
    name='pedidos',
    con=engine_destiny,
    index=False,
    if_exists='replace',  # apenas para testes, em casos reais de script rodando continuarmente utilizariamos if_exists='append'
    chunksize=cs,
)
