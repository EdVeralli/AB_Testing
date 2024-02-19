import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from matplotlib import pyplot as plt 
import math
import scipy
import pickle as pkl
import re
import datetime
pd.set_option("display.max_colwidth", None)
import warnings
warnings.filterwarnings('ignore')
import os
current_directory = os.getcwd()

print("Directorio actual de trabajo:", current_directory)

# Cambia el directorio de trabajo
new_directory = 'C:\GCBA\AB_Testing\data'
os.chdir(new_directory)
current_directory = os.getcwd()
print("Nuevo directorio de trabajo:", current_directory)

#df = pd.read_csv("oss-065-02-07.csv", encoding='utf-8',sep=',')

"""
TESTERS
"""
print("TESTERS")
# usuarios que mandaron @test_on @test_off @reset_user
testers=pd.read_csv("testers.csv")
testers=testers._col0.values
rule_ne = "PLBWX5XYGQ2B3GP7IN8Q-nml045fna3@b.m-1669990832420"

"""
CLICKS
"""
print("CLICKS")
# search + response
clicks = pd.read_csv("clicks-02-07.csv")
#clicks = clicks[clicks["session_id"] == usuario].reset_index(drop = True) 
search = clicks.copy()
search.head(1)

print(search.shape)
search.drop_duplicates(["session_id", "ts", "id", "message", "mostrado", "response_message"], inplace = True)
print(search.shape)
search.ts = pd.to_datetime(search.ts)
search["usuario"] = search.session_id.str[:20]
search = search[~search.usuario.isin(testers)]

search.head()

search[["results_showable", "results_score", "mostrado_name"]].head()

n_oneshot = search.session_id.nunique()
n_oneshot

"""
BUTTONS
"""
print("BUTTONS")
#buttons
botones = pd.read_csv("buttons-02-07.csv")

one = botones.copy()
one.head(1)

one.dtypes


one["usuario"] = one.session_id.str[:20]
one = one[~one.usuario.isin(testers)]
os = one[np.logical_and(one.one_shot == True, one.type.isin(["oneShotSearch"]))]
os.ts = pd.to_datetime(os.ts)
os["fecha"] = os.ts.dt.date

n_clicks = os.session_id.nunique()
n_clicks

"""
LISTA BLANCA
"""
print("Lista Blanca")
#mos = pd.read_csv("../datos/2023/nuevo_boti/Actualizacion Lista Blanca - 2023 - Whitelist intents - ON 05_11.csv") 
mos = pd.read_csv('Actualizacion_Lista_Blanca.csv')
rules_mos = mos["Nombre de la intención"].str.strip().values

"""
TRANSFORMACIONES
"""
print("TRANSFORMACIONES")
search1 = search.copy()
os1 = os.copy()

search.head()

"""
UMBRAL
"""
print("UMBRAL")

# nos quedamos con solo las filas de search1 en donde mostrado = True
search1 = search1[search1.results_showable == True] 

search1.sort_values(by=['ts', 'session_id', 'results_score'], ascending=[True, True, False], inplace=True)
search1.reset_index(inplace = True, drop = True)

# crea una columna con el número de orden de los scores de mayor a menor para cada ts y session id
search1["orden"] = search1.groupby(["session_id", "ts"]).cumcount() + 1

orden_1_rows = list(search1[search1['orden'] == 1]['results_score'])
orden_2_rows = list(search1[search1['orden'] == 2]['results_score'])

# En los datos de prueba al usar un .head() del archivo me quedaba una lista mas larga que la otra, si no recuerdo mal con datos reales no es necesario pero tampoco cuesta mucho
# el chequeo
if len(orden_1_rows) > len(orden_2_rows):
    orden_1_rows = orden_1_rows[:len(orden_2_rows)] 
if len(orden_1_rows) < len(orden_2_rows):
    orden_2_rows = orden_2_rows[:len(orden_1_rows)]

res = []
i = 0
for i in range(0, len(orden_1_rows)):
    if len(orden_1_rows) != len(orden_2_rows):
        print("Error, el ,largo de las listas no es el mismo")
        break
    else: 
        # evito divisiones por 0
        if (orden_1_rows[i] - orden_2_rows[i]) <= 0:
            res.append(0)
        else:
            res.append((orden_1_rows[i] - orden_2_rows[i]) / orden_2_rows[i])
        i += 1
    

# crea un diccionario para mapear combinaciones únicas de 'ts' y 'session_id' a sus valores 'res' correspondientes

ts_session_id_mapping = dict(zip(zip(search1['ts'], search1['session_id']), res))

# Añade una nueva columna 'score_oss' al DataFrame basada en el mapeo
search1['score_oss'] = search1.apply(lambda row: ts_session_id_mapping.get((row['ts'], row['session_id']), None), axis=1)


# se modifican los valores de 'type' y 'one_shot' en el DataFrame 'one' basados en la columna 'score_oss'
one.loc[(search1['score_oss'] > 0.65) & (search1['score_oss'] < 0.75), 'type'] = 'oneShotSearch'
one.loc[(search1['score_oss'] > 0.65) & (search1['score_oss'] < 0.75), 'one_shot'] = True

n_oneshot = botones[botones["type"] == "oneShotSearch"].session_id.nunique()
n_oneshot

n_oneshot = one[one["type"] == "oneShotSearch"].session_id.nunique()
n_oneshot

one.to_csv("oss-065-02-07.csv", sep=';')
print("creado el archivo oss-065-02-07.csv")
                  


