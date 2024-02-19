
import pandas as pd
import numpy as np
import sys
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




# usuarios que mandaron @test_on @test_off @reset_user
# Obtén el directorio actual de trabajo
import os
current_directory = os.getcwd()

print("Directorio actual de trabajo:", current_directory)

# Cambia el directorio de trabajo
new_directory = '/home/eduardo/GCBA/AB_Testing/data'
os.chdir(new_directory)

# Verifica que el cambio se haya realizado
current_directory = os.getcwd()
print("Nuevo directorio de trabajo:", current_directory)
testers=pd.read_csv('testers.csv')
#testers=testers.f0_.values



rule_ne='PLBWX5XYGQ2B3GP7IN8Q-nml045fna3@b.m-1669990832420'



mm=pd.read_csv('mm-01-29.csv')

"""
Hora y dia del deploy one shot:
08/01 - 10:05 a.m.
Cambio de constante al 20%: 
10/01 - 11:52 a.m.

Cambio al 40%:
16/01 - 9:30 a.m.
# In[5]:
"""


mm.creation_time=pd.to_datetime(mm.creation_time)



mm1=mm[np.logical_or(mm.vars_value<21, np.logical_and(mm.vars_value<41, mm.creation_time>=np.datetime64('2024-01-10 14:52:00')))]




mm2=mm[~np.logical_or(mm.vars_value<21, np.logical_and(mm.vars_value<41, mm.creation_time>=np.datetime64('2024-01-10 14:52:00')))]




mm1.rule_name.nunique()



mm2.rule_name.nunique()




# Preprocesamiento de DataFrames mm1 y mm2:
# 1. Ajusta la columna 'creation_time' convirtiéndola a formato de fecha y redondeándola al segundo más cercano.
# 2. Elimina duplicados basándose en columnas específicas.
# 3. Crea una nueva columna 'usuario' extrayendo los primeros 20 caracteres de 'session_id'.
# 4. Filtra las filas donde el 'usuario' no está en la lista de 'testers'.
# 5. Reinicia los índices para ambos DataFrames.


mm1.creation_time=pd.to_datetime(mm1.creation_time)
mm1.creation_time=mm1.creation_time.dt.ceil('s')
mm1.drop_duplicates(['session_id', 'creation_time', 'msg_from', 'rule_name'], inplace=True)
mm2.creation_time=pd.to_datetime(mm2.creation_time)
mm2.creation_time=mm2.creation_time.dt.ceil('s')
mm2.drop_duplicates(['session_id', 'creation_time', 'msg_from', 'rule_name'], inplace=True)
mm1['usuario']=mm1.session_id.str[:20]
mm2['usuario']=mm2.session_id.str[:20]
mm1=mm1[~mm1.usuario.isin(testers)]
mm2=mm2[~mm2.usuario.isin(testers)]
mm1.reset_index(inplace=True, drop=True)
mm2.reset_index(inplace=True, drop=True)



mm2.usuario.nunique()



mm1.usuario.nunique()



# search + response = clicks

# Procesamiento de datos del archivo 'AB_Testing_boti_CLICKS.csv':
# 1. Lee el archivo CSV y carga los datos en el DataFrame 'search'.
# 2. Elimina duplicados basándose en múltiples columnas especificadas.
# 3. Convierte la columna 'ts' a formato de fecha y hora.
# 4. Crea una nueva columna 'usuario' extrayendo los primeros 20 caracteres de 'session_id'.
# 5. Filtra las filas donde el 'usuario' no está en la lista de 'testers'.
# 6. Filtra las filas donde la condición 'RuleBuilder:'+mostrado es igual a 'response_intent_id' y elimina duplicados basándose en 'id'.
# 7. Crea una nueva columna 'fecha' que contiene solo la parte de la fecha de la columna 'ts'.

search=pd.read_csv('clicks-01-29.csv')
search.drop_duplicates(['session_id', 'ts', 'id', 'message', 'mostrado', 'response_message'], inplace=True)
search.ts=pd.to_datetime(search.ts)
search['usuario']=search.session_id.str[:20]
search=search[~search.usuario.isin(testers)]
#clicks
searchcl=search['RuleBuilder:'+search.mostrado==search.response_intent_id].drop_duplicates('id')
search['fecha']=search.ts.dt.date



search.head()



#buttons
one=pd.read_csv('buttons-01-29.csv')



# Procesamiento de datos en el DataFrame 'one':  ONESHOTS
# 1. Crea una nueva columna 'usuario' extrayendo los primeros 20 caracteres de 'session_id'.
# 2. Filtra las filas en 'one' donde el 'usuario' no está en la lista de 'testers'.
# 3. Genera un subconjunto os con las filas en 'one' donde la condición 'one_shot' es verdadera y el tipo es 'oneShot' o 'oneShotSearch'.
# 4. Convierte la columna 'ts' del subconjunto 'os' a formato de fecha y hora.
# 5. Crea una nueva columna 'fecha' en el subconjunto 'os' que contiene solo la parte de la fecha de la columna 'ts'.

one['usuario']=one.session_id.str[:20]
one=one[~one.usuario.isin(testers)]
os=one[np.logical_and(one.one_shot==True, one.type.isin(['oneShot', 'oneShotSearch']))]
os.ts=pd.to_datetime(os.ts)
os['fecha']=os.ts.dt.date




mos=pd.read_csv('Actualizacion_Lista_Blanca.csv')
rules_mos=mos['Nombre de la intención'].str.strip().values


# ### transformaciones



# sacamos mensajes seguidos de boti
# Proceso de limpieza en el DataFrame 'mm1':
# 1. Reinicia los índices del DataFrame 'mm1'.
# 2. Identifica y crea una lista 'drop' con índices a eliminar donde 'msg_from' y 'session_id' son iguales en filas consecutivas.
# 3. Elimina las filas identificadas en la lista 'drop' del DataFrame 'mm1'.
# 4. Reinicia los índices del DataFrame 'mm1' después de la eliminación.

mm1.reset_index(inplace=True, drop=True)
drop=[i if mm1.loc[i].msg_from==mm1.loc[i+1].msg_from and mm1.loc[i].session_id==mm1.loc[i+1].session_id else None for i in mm1.index[:-1]]
drop=list(set(drop))
drop.remove(None)

mm1.drop(drop, inplace=True)
mm1.reset_index(inplace=True, drop=True)



mm1.head()



# sacamos mensajes seguidos de boti
# Proceso de limpieza en el DataFrame 'mm2':
# 1. Reinicia los índices del DataFrame 'mm2'.
# 2. Identifica y crea una lista 'drop' con índices a eliminar donde 'msg_from' y 'session_id' son iguales en filas consecutivas.
# 3. Elimina las filas identificadas en la lista 'drop' del DataFrame 'mm2'.
# 4. Reinicia los índices del DataFrame 'mm2' después de la eliminación.

mm2.reset_index(inplace=True, drop=True)
drop=[i if mm2.loc[i].msg_from==mm2.loc[i+1].msg_from and mm2.loc[i].session_id==mm2.loc[i+1].session_id else None for i in mm2.index[:-1]]
drop=list(set(drop))
drop.remove(None)

mm2.drop(drop, inplace=True)
mm2.reset_index(inplace=True, drop=True)


# ### modelo nuevo, primera instancia


# Análisis de respuestas por usuario en el DataFrame 'mm1':
# 1. Filtra y estructura datos relevantes en el DataFrame 'mmtex1'.
# 2. Realiza operaciones en DataFrames adicionales ('letra1', 'search1', 'os1', 'primera_instancia1', etc.).
# 3. Combina y clasifica las respuestas en 'value1primera'.
# 4. Calcula porcentajes de respuestas por categoría para cada usuario en 'respuestas_por_usuario'.
# 5. Calcula promedios de porcentajes para distintas categorías en 'promedios1'.
# 6. Almacena resultados finales en 'res_primera_instancia1'.


mm=mm1.copy()
mm.reset_index(inplace=True, drop=True)
mmtex1=mm[np.logical_and(mm.msg_from=='user', mm.message_type=='Text')][['session_id', 'id', 'creation_time', 'msg_from', 'message_type', 'message', 'usuario']]
mmtex1['rule_name']=[r if su==sb and f=='bot' else None for r, su, sb, f in zip(mm.loc[mmtex1.index+1].rule_name.values, mmtex1.session_id.values, mm.loc[mmtex1.index+1].session_id.values, mm.loc[mmtex1.index+1].msg_from.values)]
letra1=mmtex1[mmtex1.rule_name=='No entendió letra no existente en WA']
letra1.rename(columns={'id':'message_id'}, inplace=True)
search1=search[search.session_id.isin(mm1.session_id.values)]
os1=os[os.session_id.isin(mm1.session_id.values)]
primera_instancia1=search[~search.message_id.isin(pd.concat([search1['RuleBuilder:'+search1.mostrado==search1.response_intent_id].message_id, os1.message_id]).values)].drop_duplicates('id')
primera_instancia1 = primera_instancia1.rename(columns = {"results_score": "score"})
ne1=primera_instancia1.groupby('id').max()[['session_id', 'message_id', 'score']]
ne1=ne1[ne1.score<=5.36]
primera_instancia1=primera_instancia1[~primera_instancia1.id.isin(ne1.index)]
os1=os1.drop_duplicates('id')[['session_id', 'message_id']]
click1=search1['RuleBuilder:'+search1.mostrado==search1.response_intent_id].drop_duplicates('id')[['session_id', 'message_id']]
abandonos1=primera_instancia1[primera_instancia1.response_message.isna()][['session_id', 'message_id']]
nada1=primera_instancia1[primera_instancia1.response_intent_id=='RuleBuilder:PLBWX5XYGQ2B3GP7IN8Q-alfafc@gmail.com-1536777380652'][['session_id', 'message_id']]
texto1=primera_instancia1[np.logical_and(primera_instancia1.response_intent_id!='RuleBuilder:PLBWX5XYGQ2B3GP7IN8Q-alfafc@gmail.com-1536777380652', ~primera_instancia1.response_message.isna())][['session_id', 'message_id']]
letra1=letra1[['session_id', 'message_id']]
os1['categoria']='one'
click1['categoria']='click'
abandonos1['categoria']='abandono'
nada1['categoria']='nada'
texto1['categoria']='texto'
ne1['categoria']='ne'
letra1['categoria']='letra'
value1primera=pd.concat([os1, click1, abandonos1, nada1, texto1, ne1, letra1])
value1primera['usuario']=value1primera.session_id.str[:20]
value1primera=value1primera[value1primera.usuario.isin(mm1.usuario.values)]

respuestas_por_usuario=value1primera.groupby(['usuario','categoria'], as_index=False).count()[['usuario','categoria', 'message_id']].pivot_table('message_id', ['usuario'], 'categoria')
respuestas_por_usuario.fillna(0, inplace=True)
respuestas_por_usuario=respuestas_por_usuario.reset_index(drop=False).reindex(['usuario',  'one', 'click', 'texto', 'abandono', 'nada', 'ne', 'letra'], axis=1)
respuestas_por_usuario['porcentaje_abandono']=[respuestas_por_usuario.loc[i].abandono / respuestas_por_usuario.loc[i][['one', 'click', 'texto', 'abandono', 'nada', 'ne', 'letra']].sum() for i in respuestas_por_usuario.index]
respuestas_por_usuario['porcentaje_click']=[respuestas_por_usuario.loc[i].click / respuestas_por_usuario.loc[i][['one', 'click', 'texto', 'abandono', 'nada', 'ne', 'letra']].sum() for i in respuestas_por_usuario.index]
respuestas_por_usuario['porcentaje_one']=[respuestas_por_usuario.loc[i].one / respuestas_por_usuario.loc[i][['one', 'click', 'texto', 'abandono', 'nada', 'ne', 'letra']].sum() for i in respuestas_por_usuario.index]
respuestas_por_usuario['porcentaje_texto']=[respuestas_por_usuario.loc[i].texto / respuestas_por_usuario.loc[i][['one', 'click', 'texto', 'abandono', 'nada', 'ne', 'letra']].sum() for i in respuestas_por_usuario.index]
respuestas_por_usuario['porcentaje_nada']=[respuestas_por_usuario.loc[i].nada / respuestas_por_usuario.loc[i][['one', 'click', 'texto', 'abandono', 'nada', 'ne', 'letra']].sum() for i in respuestas_por_usuario.index]
respuestas_por_usuario['porcentaje_ne']=[respuestas_por_usuario.loc[i]['ne'] / respuestas_por_usuario.loc[i][['one', 'click', 'texto', 'abandono', 'nada', 'ne', 'letra']].sum() for i in respuestas_por_usuario.index]
respuestas_por_usuario['porcentaje_letra']=[respuestas_por_usuario.loc[i]['letra'] / respuestas_por_usuario.loc[i][['one', 'click', 'texto', 'abandono', 'nada', 'ne', 'letra']].sum() for i in respuestas_por_usuario.index]
res_primera_instancia1=respuestas_por_usuario.copy()

promedios1={'abandonos': round(respuestas_por_usuario['porcentaje_abandono'].mean(), 3),     
                  'click': round(respuestas_por_usuario['porcentaje_click'].mean(), 3),
                  'one': round(respuestas_por_usuario['porcentaje_one'].mean(), 3),
                  'texto': round(respuestas_por_usuario['porcentaje_texto'].mean(), 3),
                  'nada': round(respuestas_por_usuario['porcentaje_nada'].mean(), 3),
            'letra': round(respuestas_por_usuario['porcentaje_letra'].mean(), 3),
           'ne': round(respuestas_por_usuario['porcentaje_ne'].mean(), 3)}



# Análisis de respuestas por usuario en el DataFrame 'mm2':
# 1. Filtra y estructura datos relevantes en el DataFrame 'mmtex2'.
# 2. Realiza operaciones en DataFrames adicionales ('letra2', 'search2', 'os2', 'primera_instancia2', etc.).
# 3. Combina y clasifica las respuestas en 'value2primera'.
# 4. Calcula porcentajes de respuestas por categoría para cada usuario en 'respuestas_por_usuario'.
# 5. Calcula promedios de porcentajes para distintas categorías en 'promedios2'.
# 6. Almacena resultados finales en 'res_primera_instancia2'.

mm=mm2.copy()
mm.reset_index(inplace=True, drop=True)
mmtex2=mm[np.logical_and(mm.msg_from=='user', mm.message_type=='Text')][['session_id', 'id', 'creation_time', 'msg_from', 'message_type', 'message', 'usuario']]
mmtex2['rule_name']=[r if su==sb and f=='bot' else None for r, su, sb, f in zip(mm.loc[mmtex2.index+1].rule_name.values, mmtex2.session_id.values, mm.loc[mmtex2.index+1].session_id.values, mm.loc[mmtex2.index+1].msg_from.values)]
letra2=mmtex2[mmtex2.rule_name=='No entendió letra no existente en WA']
letra2.rename(columns={'id':'message_id'}, inplace=True)
search2=search[search.session_id.isin(mm2.session_id.values)]
os2=os[os.session_id.isin(mm2.session_id.values)]
primera_instancia2=search[~search.message_id.isin(pd.concat([search2['RuleBuilder:'+search2.mostrado==search2.response_intent_id].message_id, os2.message_id]).values)].drop_duplicates('id')
primera_instancia2 = primera_instancia2.rename(columns = {"results_score": "score"})
ne2=primera_instancia2.groupby('id').max()[['session_id', 'message_id', 'score']]
ne2=ne2[ne2.score<=5.36]
primera_instancia2=primera_instancia2[~primera_instancia2.id.isin(ne2.index)]
os2=os2.drop_duplicates('id')[['session_id', 'message_id']]
click2=search2['RuleBuilder:'+search2.mostrado==search2.response_intent_id].drop_duplicates('id')[['session_id', 'message_id']]
abandonos2=primera_instancia2[primera_instancia2.response_message.isna()][['session_id', 'message_id']]
nada2=primera_instancia2[primera_instancia2.response_intent_id=='RuleBuilder:PLBWX5XYGQ2B3GP7IN8Q-alfafc@gmail.com-1536777380652'][['session_id', 'message_id']]
texto2=primera_instancia2[np.logical_and(primera_instancia2.response_intent_id!='RuleBuilder:PLBWX5XYGQ2B3GP7IN8Q-alfafc@gmail.com-1536777380652', ~primera_instancia2.response_message.isna())][['session_id', 'message_id']]
letra2=letra2[['session_id', 'message_id']]
os2['categoria']='one'
click2['categoria']='click'
abandonos2['categoria']='abandono'
nada2['categoria']='nada'
texto2['categoria']='texto'
ne2['categoria']='ne'
letra2['categoria']='letra'
value2primera=pd.concat([os2, click2, abandonos2, nada2, texto2, ne2, letra2])
value2primera['usuario']=value2primera.session_id.str[:20]
value2primera=value2primera[value2primera.usuario.isin(mm2.usuario.values)]

respuestas_por_usuario=value2primera.groupby(['usuario','categoria'], as_index=False).count()[['usuario','categoria', 'message_id']].pivot_table('message_id', ['usuario'], 'categoria')
respuestas_por_usuario.fillna(0, inplace=True)
respuestas_por_usuario=respuestas_por_usuario.reset_index(drop=False).reindex(['usuario',  'one', 'click', 'texto', 'abandono', 'nada', 'ne', 'letra'], axis=1)
respuestas_por_usuario['porcentaje_abandono']=[respuestas_por_usuario.loc[i].abandono / respuestas_por_usuario.loc[i][['one', 'click', 'texto', 'abandono', 'nada', 'ne', 'letra']].sum() for i in respuestas_por_usuario.index]
respuestas_por_usuario['porcentaje_click']=[respuestas_por_usuario.loc[i].click / respuestas_por_usuario.loc[i][['one', 'click', 'texto', 'abandono', 'nada', 'ne', 'letra']].sum() for i in respuestas_por_usuario.index]
respuestas_por_usuario['porcentaje_one']=[respuestas_por_usuario.loc[i].one / respuestas_por_usuario.loc[i][['one', 'click', 'texto', 'abandono', 'nada', 'ne', 'letra']].sum() for i in respuestas_por_usuario.index]
respuestas_por_usuario['porcentaje_texto']=[respuestas_por_usuario.loc[i].texto / respuestas_por_usuario.loc[i][['one', 'click', 'texto', 'abandono', 'nada', 'ne', 'letra']].sum() for i in respuestas_por_usuario.index]
respuestas_por_usuario['porcentaje_nada']=[respuestas_por_usuario.loc[i].nada / respuestas_por_usuario.loc[i][['one', 'click', 'texto', 'abandono', 'nada', 'ne', 'letra']].sum() for i in respuestas_por_usuario.index]
respuestas_por_usuario['porcentaje_ne']=[respuestas_por_usuario.loc[i]['ne'] / respuestas_por_usuario.loc[i][['one', 'click', 'texto', 'abandono', 'nada', 'ne', 'letra']].sum() for i in respuestas_por_usuario.index]
respuestas_por_usuario['porcentaje_letra']=[respuestas_por_usuario.loc[i]['letra'] / respuestas_por_usuario.loc[i][['one', 'click', 'texto', 'abandono', 'nada', 'ne', 'letra']].sum() for i in respuestas_por_usuario.index]
res_primera_instancia2=respuestas_por_usuario.copy()

promedios2={'abandonos': round(respuestas_por_usuario['porcentaje_abandono'].mean(), 3),     
                  'click': round(respuestas_por_usuario['porcentaje_click'].mean(), 3),
                  'one': round(respuestas_por_usuario['porcentaje_one'].mean(), 3),
                  'texto': round(respuestas_por_usuario['porcentaje_texto'].mean(), 3),
                  'nada': round(respuestas_por_usuario['porcentaje_nada'].mean(), 3),
            'letra': round(respuestas_por_usuario['porcentaje_letra'].mean(), 3),
           'ne': round(respuestas_por_usuario['porcentaje_ne'].mean(), 3)}



promedios1 #20%



promedios2 #80%


"""
promedios1 80%
{'abandonos': 0.062,
 'click': 0.44,
 'one': 0.229,
 'texto': 0.111,
 'nada': 0.067,
 'letra': 0.076,
 'ne': 0.015}

promedios2 20%
{'abandonos': 0.001,
 'click': 0.029,
 'one': 0.757,
 'texto': 0.008,
 'nada': 0.003,
 'letra': 0.2,
 'ne': 0.002}
# In[25]:

"""

def categoria(m, t, r):
    #mensaje, tipo de mensaje, rulename
    try:
        if t=='Button-click' and 'Cambiar de tema' in m:
            return 'cambiar'
        elif t=='Button-click' and r=='Menú show buttons':
            return 'otros'
        elif t=='Button-click' and 'No era nada de eso' in m:
            return 'x'
        elif t=='Button-click':
            return 'boton'
        elif re.match(r'^a$|^b$|^c$|^d$', m,  re.IGNORECASE) and r=='Infracciones * Apertura':
            return 'boton'
        elif re.match(r'^a$|^b$|^c$|^d$', m,  re.IGNORECASE) and r=='Busca donde está permitido estacionar':
            return 'boton'
        #elif m=='__image__' and r=='Denuncia Vial - Validación Vehículo':
         #   return 'boton'
        #elif re.match(r'[0-9]{7,8}', m) and r=='Licencia prorroga  > Consultar':
         #   return 'boton'
        elif re.match(r'(^x$)|(x?\.? ?buscaba otra cosa)', m,  re.IGNORECASE):
            return 'x'
        else:
            return 'texto'
    except:
        return 'otros'



# Análisis de interacciones del usuario en el DataFrame 'mm1':
# 1. Filtra y estructura datos relevantes para conversaciones con botones ('conv_cl').
# 2. Crea un DataFrame 'conv' para conversaciones de un solo disparo.
# 3. Calcula categorías ('categoria') y porcentajes ('per') de interacciones en 'conv'.
# 4. Combina datos de 'conv_cl' y 'conv' en 'usuario1'.
# 5. Calcula categorías y usuarios en 'usuario1'.
# 6. Almacena resultados finales en 'usuario1'.

mm=mm1.copy()
mm.reset_index(inplace=True, drop=True)
mmu=mm[mm.msg_from=='user']
mmu.reset_index(inplace=True, drop=True)
original=mmu[mmu.id.isin(searchcl.message_id.values)] 
boton=mmu.loc[original.index+1]
respuesta=mmu.loc[original.index+2]
conv_cl=pd.DataFrame(data={'session_id': original.session_id.values, 'creation_time': original.creation_time.values, 'original':original.message.values, 
                           'intent': mm.loc[mm[mm.id.isin(boton.id.values)].index+1].rule_name.values,
                           'bot1_id': [m if v else None for m, v in zip(mm.loc[mm[mm.id.isin(boton.id.values)].index+1].id.values, original.session_id.values==mm.loc[mm[mm.id.isin(boton.id.values)].index+1].session_id.values)],
                   'respuesta_intermedia': [m if v else None for m, v in zip(boton.message.values, original.session_id.values==boton.session_id.values)], 
                   'respuesta': [m if v else None for m, v in zip(respuesta.message.values, original.session_id.values==respuesta.session_id.values)],
                    'respuesta_type': [m if v else None for m, v in zip(respuesta.message_type.values, original.session_id.values==respuesta.session_id.values)],
                     'respuesta_rule': [m if v else None for m, v in zip(mm.loc[mm[mm.id.isin(respuesta.id.values)].index+1].rule_name.values, original.session_id.values==mm.loc[mm[mm.id.isin(respuesta.id.values)].index+1].session_id.values)]})


mm=mm1.copy()
mm.reset_index(inplace=True, drop=True)
mmu=mm[mm.msg_from=='user']
mmu.reset_index(inplace=True, drop=True)
original=mmu[mmu.id.isin(os.message_id.values)]
respuesta=mmu.loc[original.index+1]
conv=pd.DataFrame(data={'session_id': original.session_id.values, 'creation_time': original.creation_time.values, 'original':original.message.values, 
                        'intent': mm.loc[mm[mm.id.isin(original.id.values)].index+1].rule_name.values,
                        'bot1_id': [m if v else None for m, v in zip(mm.loc[mm[mm.id.isin(original.id.values)].index+1].id.values, original.session_id.values==mm.loc[mm[mm.id.isin(original.id.values)].index+1].session_id.values)],
                   'respuesta': [m if v else None for m, v in zip(respuesta.message.values, original.session_id.values==respuesta.session_id.values)],
                    'respuesta_type': [m if v else None for m, v in zip(respuesta.message_type.values, original.session_id.values==respuesta.session_id.values)],
                     'respuesta_rule': [m if v else None for m, v in zip(mm.loc[mm[mm.id.isin(respuesta.id.values)].index+1].rule_name.values, original.session_id.values==respuesta.session_id.values)]})


conv['categoria']=[categoria(m,t,r) if m is not None else 'abandono' for m,t,r in zip(conv.respuesta, conv.respuesta_type, conv.intent)]
per=conv.groupby('categoria', as_index=False).count()[['categoria', 'bot1_id']]
per['per']=per.bot1_id/per.bot1_id.sum()
usuario1=pd.concat([conv_cl[['session_id', 'creation_time', 'original', 'intent', 'bot1_id', 'respuesta', 'respuesta_type', 'respuesta_rule']], conv])
usuario1['categoria']=[categoria(m,t,r) if m is not None else 'abandono' for m,t,r in zip(usuario1.respuesta, usuario1.respuesta_type, usuario1.intent)]
usuario1['usuario']=usuario1.session_id.str[:20]
usuario1['id']=usuario1.bot1_id


# Análisis de interacciones del usuario en el DataFrame 'mm2':
# 1. Filtra y estructura datos relevantes para conversaciones con botones ('conv_cl').
# 2. Crea un DataFrame 'conv' para conversaciones de un solo disparo.
# 3. Calcula categorías ('categoria') y porcentajes ('per') de interacciones en 'conv'.
# 4. Combina datos de 'conv_cl' y 'conv' en 'usuario2'.
# 5. Calcula categorías y usuarios en 'usuario2'.
# 6. Almacena resultados finales en 'usuario2'.

mm=mm2.copy()
mm.reset_index(inplace=True, drop=True)
mmu=mm[mm.msg_from=='user']
mmu.reset_index(inplace=True, drop=True)
original=mmu[mmu.id.isin(searchcl.message_id.values)] 
boton=mmu.loc[original.index+1]
respuesta=mmu.loc[original.index+2]
conv_cl=pd.DataFrame(data={'session_id': original.session_id.values, 'creation_time': original.creation_time.values, 'original':original.message.values, 
                           'intent': mm.loc[mm[mm.id.isin(boton.id.values)].index+1].rule_name.values,
                           'bot1_id': [m if v else None for m, v in zip(mm.loc[mm[mm.id.isin(boton.id.values)].index+1].id.values, original.session_id.values==mm.loc[mm[mm.id.isin(boton.id.values)].index+1].session_id.values)],
                   'respuesta_intermedia': [m if v else None for m, v in zip(boton.message.values, original.session_id.values==boton.session_id.values)], 
                   'respuesta': [m if v else None for m, v in zip(respuesta.message.values, original.session_id.values==respuesta.session_id.values)],
                    'respuesta_type': [m if v else None for m, v in zip(respuesta.message_type.values, original.session_id.values==respuesta.session_id.values)],
                     'respuesta_rule': [m if v else None for m, v in zip(mm.loc[mm[mm.id.isin(respuesta.id.values)].index+1].rule_name.values, original.session_id.values==mm.loc[mm[mm.id.isin(respuesta.id.values)].index+1].session_id.values)]})


mm=mm2.copy()
mm.reset_index(inplace=True, drop=True)
mmu=mm[mm.msg_from=='user']
mmu.reset_index(inplace=True, drop=True)
original=mmu[mmu.id.isin(os.message_id.values)]
respuesta=mmu.loc[original.index+1]
conv=pd.DataFrame(data={'session_id': original.session_id.values, 'creation_time': original.creation_time.values, 'original':original.message.values, 
                        'intent': mm.loc[mm[mm.id.isin(original.id.values)].index+1].rule_name.values,
                        'bot1_id': [m if v else None for m, v in zip(mm.loc[mm[mm.id.isin(original.id.values)].index+1].id.values, original.session_id.values==mm.loc[mm[mm.id.isin(original.id.values)].index+1].session_id.values)],
                   'respuesta': [m if v else None for m, v in zip(respuesta.message.values, original.session_id.values==respuesta.session_id.values)],
                    'respuesta_type': [m if v else None for m, v in zip(respuesta.message_type.values, original.session_id.values==respuesta.session_id.values)],
                     'respuesta_rule': [m if v else None for m, v in zip(mm.loc[mm[mm.id.isin(respuesta.id.values)].index+1].rule_name.values, original.session_id.values==respuesta.session_id.values)]})


conv['categoria']=[categoria(m,t,r) if m is not None else 'abandono' for m,t,r in zip(conv.respuesta, conv.respuesta_type, conv.intent)]
per=conv.groupby('categoria', as_index=False).count()[['categoria', 'bot1_id']]
per['per']=per.bot1_id/per.bot1_id.sum()
usuario2=pd.concat([conv_cl[['session_id', 'creation_time', 'original', 'intent', 'bot1_id', 'respuesta', 'respuesta_type', 'respuesta_rule']], conv])
usuario2['categoria']=[categoria(m,t,r) if m is not None else 'abandono' for m,t,r in zip(usuario2.respuesta, usuario2.respuesta_type, usuario2.intent)]
usuario2['usuario']=usuario2.session_id.str[:20]
usuario2['id']=usuario2.bot1_id

"""
20%: usuario1
80%: usuario2
"""
# ### resultados



# Análisis de resultados por usuario:
# - Para cada usuario (usuario1 y usuario2):
#   1. Calcula la frecuencia de respuestas por categoría ('abandono', 'boton', 'otros', 'texto', 'x', 'cambiar').
#   2. Calcula porcentajes de respuestas por categoría para cada usuario.
#   3. Almacena los resultados en un DataFrame ('respuestas_por_usuario') y en una lista de promedios ('promedios').

resultados=[]
promedios=[]
for usuario in [usuario1, usuario2]:
    respuestas_por_usuario=usuario.groupby(['usuario','categoria'], as_index=False).count()[['usuario','categoria', 'id']].pivot_table('id', ['usuario'], 'categoria')
    respuestas_por_usuario.fillna(0, inplace=True)
    respuestas_por_usuario=respuestas_por_usuario.reset_index(drop=False).reindex(['usuario', 'abandono', 'boton', 'otros', 'texto', 'x', 'cambiar'], axis=1)
    respuestas_por_usuario['porcentaje_abandono']=[respuestas_por_usuario.loc[i].abandono / respuestas_por_usuario.loc[i][['abandono', 'boton', 'otros', 'texto', 'x',  'cambiar']].sum() for i in respuestas_por_usuario.index]
    respuestas_por_usuario['porcentaje_boton']=[respuestas_por_usuario.loc[i].boton / respuestas_por_usuario.loc[i][['abandono', 'boton', 'otros', 'texto', 'x',  'cambiar']].sum() for i in respuestas_por_usuario.index]
    respuestas_por_usuario['porcentaje_otros']=[respuestas_por_usuario.loc[i].otros / respuestas_por_usuario.loc[i][['abandono', 'boton', 'otros', 'texto', 'x',  'cambiar']].sum() for i in respuestas_por_usuario.index]
    respuestas_por_usuario['porcentaje_texto']=[respuestas_por_usuario.loc[i].texto / respuestas_por_usuario.loc[i][['abandono', 'boton', 'otros', 'texto', 'x',  'cambiar']].sum() for i in respuestas_por_usuario.index]
    respuestas_por_usuario['porcentaje_x']=[respuestas_por_usuario.loc[i].x / respuestas_por_usuario.loc[i][['abandono', 'boton', 'otros', 'texto', 'x',  'cambiar']].sum() for i in respuestas_por_usuario.index]
    respuestas_por_usuario['porcentaje_cambiar']=[respuestas_por_usuario.loc[i].cambiar / respuestas_por_usuario.loc[i][['abandono', 'boton', 'otros', 'texto', 'x',  'cambiar']].sum() for i in respuestas_por_usuario.index]
    resultados.append(respuestas_por_usuario)
    promedios.append({'abandonos': round(respuestas_por_usuario['porcentaje_abandono'].mean(), 3),     
                      'botones': round(respuestas_por_usuario['porcentaje_boton'].mean(), 3),
                      'otros': round(respuestas_por_usuario['porcentaje_otros'].mean(), 3),
                      'texto': round(respuestas_por_usuario['porcentaje_texto'].mean(), 3),
                      'x': round(respuestas_por_usuario['porcentaje_x'].mean(), 3),
                      'cambiar de tema': round(respuestas_por_usuario['porcentaje_cambiar'].mean(), 3)})


pd.DataFrame(promedios, index=['nuevo-con-oss', 'nuevo-sin-oss'])[['abandonos', 'botones', 'texto', 'x', 'cambiar de tema', 'otros']]



sin_oss1={k:v*100 for k,v in promedios2.items()} 
sin_oss2={k: (promedios2['click']+promedios2['one'])*v*100 for k, v in promedios[1].items()}
con_oss1={k:v*100 for k,v in promedios1.items()} 
con_oss2={k: (promedios1['click']+promedios1['one'])*v*100 for k, v in promedios[0].items()}


sin_oss1


con_oss1



sin_oss2




con_oss2


# In[35]:


(len(res_primera_instancia1.usuario.unique()), len(res_primera_instancia2.usuario.unique()))


# ### diferencias




def difz(p1, p2, n1, n2):
    # test estadístico, alfa=0,05
    z=(p1-p2)/math.sqrt(p1*(1-p1)/n1+p2*(1-p2)/n2)
    if z>1.96 or z<-1.96:
        return ('*', z)
    else:
        return z


# Comparación de porcentajes normalizados entre dos conjuntos de datos ('sin_oss' y 'con_oss'):
# - Para diferentes categorías de respuestas:
#   1. Calcula porcentajes de respuestas para cada categoría.
#   2. Utiliza la función 'difz' para calcular la diferencia normalizada entre porcentajes.
#   3. Imprime los resultados para cada categoría ('mostrable / click+one', 'ne', 'letra', 'texto', 'abandonos', 'nada', 'botones', 'x', 'cambiar de tema', 'otros' si está presente).

n1=res_primera_instancia1.usuario.nunique()
n2=res_primera_instancia2.usuario.nunique()
print('mostrable / click+one')
p1=(sin_oss1['click']+sin_oss1['one'])/100
p2=(con_oss1['click']+con_oss1['one'])/100
print(difz(p1, p2, n1, n2))
print('ne')
p1=sin_oss1['ne']/100
p2=con_oss1['ne']/100
print(difz(p1, p2, n1, n2))
print('letra')
p1=sin_oss1['letra']/100
p2=con_oss1['letra']/100
print(difz(p1, p2, n1, n2))
print('texto')
p1=(sin_oss1['texto']+sin_oss2['texto'])/100
p2=(con_oss1['texto']+con_oss2['texto'])/100
print(difz(p1, p2, n1, n2))
print('abandonos')
p1=(sin_oss1['abandonos']+sin_oss2['abandonos'])/100
p2=(con_oss1['abandonos']+con_oss2['abandonos'])/100
print(difz(p1, p2, n1, n2))
print('nada de eso')
p1=sin_oss1['nada']/100
p2=con_oss1['nada']/100
print(difz(p1, p2, n1, n2))
print('botones')
p1=sin_oss2['botones']/100
p2=con_oss2['botones']/100
print(difz(p1, p2, n1, n2))
print('x')
p1=sin_oss2['x']/100
p2=con_oss2['x']/100
print(difz(p1, p2, n1, n2))
print('cambiar de tema')
p1=sin_oss2['cambiar de tema']/100
p2=con_oss2['cambiar de tema']/100
print(difz(p1, p2, n1, n2))
try:
    print('otros')
    p1=sin_oss2['otros']/100
    p2=con_oss2['otros']/100
    print(difz(p1, p2, n1, n2))
except:
    pass


# ### metricas generales


"""
Este código realiza análisis y cálculos estadísticos sobre un conjunto de datos representado por el DataFrame `df1`.
Realiza las siguientes acciones:
1. Copia el DataFrame original `mm2` en `df1`.
2. Identifica y elimina líneas que marcan el inicio y fin de interacciones con operadores.
3. Comienza a guardar métricas sobre la interacción del usuario:
   - Número total de sesiones y su duración.
   - Promedio, mediana y percentiles de interacciones por sesión.
   - Porcentaje de mensajes no entendidos.
4. Analiza intenciones repetidas llegando por texto:
   - Cálculo del promedio y mediana de intenciones repetidas.
5. Examina mensajes repetidos del usuario por sesión:
   - Cálculo del promedio y mediana de mensajes repetidos.
   - Determina el máximo mensaje repetido por usuario en una sesión.
6. Presenta los resultados en un DataFrame llamado `df`.
Nota: Algunas líneas de código están comentadas (`#`) debido a la falta de información sobre ciertas variables (por ejemplo, `save_path`).
"""

df1 = mm2.copy()
df1 = df1.rename(columns = {"max_score": "score"})
# Elimina las lineas donde comienza y termina una interaccion con un operador

op1 = df1.loc[df1.msg_from == 'operator'][['session_id','msg_from','creation_time']]

max_sid1 = op1.groupby('session_id').creation_time.max()

min_sid1 = op1.groupby('session_id').creation_time.min()

sid1 = []
for a in min_sid1.keys():
    for b in range(op1[np.logical_and(op1['session_id'] == a,op1['creation_time'] == min_sid1[a])].index[0],
                   op1[np.logical_and(op1['session_id'] == a,op1['creation_time'] == max_sid1[a])].index[0]+1):
        sid1.append(b)
        
df1 = df1.drop(sid1)

# Comienza a guardar metricas
inter1 = df1[df1.msg_from == 'user'].groupby(['session_id']).msg_from.count().sort_values(ascending = False)
data1 = {'cant_sesiones':len(inter1),
         'cant_interacciones':inter1.sum(), 
         'interacciones_promedio':inter1.mean(), 
         'interacciones_mediana':inter1.median(), 
         'interacciones_primer_cuartil':inter1.quantile(0.25), 
         'interacciones_tercer_cuartil':inter1.quantile(0.75),
         'interacciones_p95':inter1.quantile(0.95)
        }

ne1 = len(df1[df1.score < 5.36])/len(df1[np.logical_and(df1.msg_from == 'user', df1.message_type == 'Text')])*100

data1['no_entendidos_pc'] = ne1

#Cantidad de intenciones repetidas llegando por texto
dfint1 = df1.dropna(subset=['original_user_message']).reset_index(drop = True)
dfint1.creation_time = dfint1.loc[dfint1.msg_from.isin(['bot','user']),'creation_time'] 
int_texto1 = dfint1[~dfint1.original_user_message.str.contains('{')].drop_duplicates(['session_id','creation_time']).groupby(['session_id','rule_name'])[['rule_name']].count().rename(columns = {'rule_name':'cantidad'})
int_texto1 = int_texto1.reset_index()
data1['intents_repetidos_texto_promedio'] = int_texto1.cantidad.mean()
data1['intents_repetidos_texto_mediana'] = int_texto1.cantidad.median()

#Cantidad de mensajes repetidos del usuario por sesión
df1.loc[np.logical_and(df1['msg_from'] == 'user', df1['message_type'] == 'Text'),'message'] = df1.loc[np.logical_and(df1['msg_from'] == 'user', df1['message_type'] == 'Text'),'message'].str.lower()
messrep1 = (df1[np.logical_and(df1['msg_from'] == 'user', df1['message_type'].isin(['Text']))].groupby(['session_id','message']).message_type.count().sort_values(ascending = False)).reset_index().rename(columns = {'message_type':'count'})
data1['mensajes_repetidos_promedio'] = messrep1[messrep1['count'] > 1]['count'].mean()
data1['mensajes_repetidos_mediana'] = messrep1[messrep1['count'] > 1]['count'].median()

#Maximo mensje repetido de cada usuario en una sesion
usrmsgrep1 = messrep1.sort_values(by = ['session_id','count'], ascending = False).drop_duplicates('session_id', keep = 'first').sort_values('count', ascending = False)

data1['mensajes_repetidos_maximo_por_usuaruio_promedio'] = usrmsgrep1[usrmsgrep1['count'] > 1]['count'].mean()
data1['mensajes_repetidos_maximo_por_usuaruio_mediana'] = usrmsgrep1[usrmsgrep1['count'] > 1]['count'].mean()

#resultados
df = pd.DataFrame(index = data1.keys(), data= {'df1':data1.values()})
#df.to_csv(f'{save_path}/indicadores_generales_{datetime.now()}.csv')
df


df




print("final")
sys.exit()

# ### dfs por categoría   RECONSTRUIR PARA LOS DF ACTUALES


"""
Este bloque de código realiza la exportación de datos filtrados en archivos CSV según la categoría de interacciones y el usuario asociado. A continuación, se detallan las acciones realizadas:
1. Exporta las interacciones de la categoría 'x' del `usuario1` a un archivo CSV llamado 'original_xr.csv'.
2. Exporta las interacciones de la categoría 'texto' del `usuario1` a un archivo CSV llamado 'original_textor.csv'.
3. Exporta las interacciones de la categoría 'abandono' del `usuario1` a un archivo CSV llamado 'original_abandonor.csv'.
4. Exporta las interacciones de la categoría 'abandono' del `usuario2` a un archivo CSV llamado 'nuevo_abandono2r.csv'.
5. Exporta las interacciones de la categoría 'texto' del `usuario2` a un archivo CSV llamado 'nuevo_texto2r.csv'.
6. Exporta las interacciones de la categoría 'otros' del `usuario2` a un archivo CSV llamado 'nuevo_errorr.csv'.
7. Exporta las interacciones de la categoría 'x' del `usuario2` a un archivo CSV llamado 'nuevo_xr.csv'.
8. Filtra las interacciones de `primera_instancia` según una condición y exporta los resultados a un archivo CSV llamado 'nuevo_nada_de_esor.csv'.
9. Filtra las interacciones de `primera_instancia` según otra condición y exporta los resultados a un archivo CSV llamado 'nuevo_texto1r.csv'.
10. Filtra las interacciones de `primera_instancia` según una condición de abandono y exporta los resultados a un archivo CSV llamado 'nuevo_abandono1.csv'.
"""

usuario1[usuario1.categoria=='x'][['session_id', 'creation_time', 'id', 'original']].to_csv('./entregables/original_xr.csv', index=False)

usuario1[usuario1.categoria=='texto'][['session_id', 'creation_time', 'id', 'original']].to_csv('./entregables/original_textor.csv', index=False)

usuario1[usuario1.categoria=='abandono'][['session_id', 'creation_time', 'id', 'original']].to_csv('./entregables/original_abandonor.csv', index=False)

usuario2[usuario2.categoria=='abandono'][['session_id', 'creation_time', 'id', 'original']].to_csv('./entregables/nuevo_abandono2r.csv', index=False)

usuario2[usuario2.categoria=='texto'][['session_id', 'creation_time', 'id', 'original']].to_csv('./entregables/nuevo_texto2r.csv', index=False)

usuario2[usuario2.categoria=='otros'][['session_id', 'creation_time', 'id', 'original']].to_csv('./entregables/nuevo_errorr.csv', index=False)

usuario2[usuario2.categoria=='x'][['session_id', 'creation_time', 'id', 'original']].to_csv('./entregables/nuevo_xr.csv', index=False)

#PREGUNTARLE A CRIS SI ESTA OK QUE SEA primera_instancia1 y en el proximo 2, porque estaban sin numero y no exitian

primera_instancia1[np.logical_and(primera_instancia1.response_intent_id=='RuleBuilder:PLBWX5XYGQ2B3GP7IN8Q-alfafc@gmail.com-1536777380652', primera_instancia1.usuario.isin(mm2.usuario.values) )][['session_id', 'ts','message_id',  'message']].to_csv('./entregables/nuevo_nada_de_esor.csv', index=False)

nt1=primera_instancia1[np.logical_and(primera_instancia1.response_intent_id!='RuleBuilder:PLBWX5XYGQ2B3GP7IN8Q-alfafc@gmail.com-1536777380652', ~primera_instancia1.response_message.isna())]
nt1=nt1[nt1.usuario.isin(mm2.usuario.values)]
nt1[['session_id', 'ts', 'message_id', 'message']].to_csv('./entregables/nuevo_texto1r.csv', index=False)

na1=primera_instancia1[primera_instancia1.response_message.isna()]
na1[na1.usuario.isin(mm2.usuario.values)][['session_id', 'message_id', 'ts', 'message']].to_csv('./entregables/nuevo_abandono1.csv', index=False)


# In[ ]:


"""
Este bloque de código realiza la exportación de datos filtrados en archivos CSV según la categoría de interacciones y el usuario asociado. A continuación, se detallan las acciones realizadas:

1. Exporta las interacciones de la categoría 'x' del `usuario1` a un archivo CSV llamado 'original_x.csv'.
2. Exporta las interacciones de la categoría 'texto' del `usuario1` a un archivo CSV llamado 'original_texto.csv'.
3. Exporta las interacciones de la categoría 'abandono' del `usuario1` a un archivo CSV llamado 'original_abandono.csv'.
4. Exporta las interacciones de la categoría 'abandono' del `usuario2` a un archivo CSV llamado 'nuevo_abandono2.csv'.
5. Exporta las interacciones de la categoría 'texto' del `usuario2` a un archivo CSV llamado 'nuevo_texto2.csv'.
6. Exporta las interacciones de la categoría 'x' del `usuario2` a un archivo CSV llamado 'nuevo_x.csv'.
7. Filtra las interacciones de `primera_instancia` según una condición y exporta los resultados a un archivo CSV llamado 'nuevo_nada_de_eso.csv'.
8. Filtra las interacciones de `primera_instancia` según otra condición y exporta los resultados a un archivo CSV llamado 'nuevo_texto1.csv'.
9. Filtra las interacciones de `primera_instancia` según una condición de abandono y exporta los resultados a un archivo CSV llamado 'nuevo_abandono1.csv'.
"""

usuario1[usuario1.categoria=='x'][['session_id', 'creation_time', 'original']].to_csv('./entregables/original_x.csv', index=False)

usuario1[usuario1.categoria=='texto'][['session_id', 'creation_time',  'original']].to_csv('./entregables/original_texto.csv', index=False)

usuario1[usuario1.categoria=='abandono'][['session_id', 'creation_time',  'original']].to_csv('./entregables/original_abandono.csv', index=False)

usuario2[usuario2.categoria=='abandono'][['session_id', 'creation_time', 'original']].to_csv('./entregables/nuevo_abandono2.csv', index=False)

usuario2[usuario2.categoria=='texto'][['session_id', 'creation_time', 'original']].to_csv('./entregables/nuevo_texto2.csv', index=False)

usuario2[usuario2.categoria=='x'][['session_id', 'creation_time',  'original']].to_csv('./entregables/nuevo_x.csv', index=False)

primera_instancia2[np.logical_and(primera_instancia2.response_intent_id=='RuleBuilder:PLBWX5XYGQ2B3GP7IN8Q-alfafc@gmail.com-1536777380652', primera_instancia2.usuario.isin(mm2.usuario.values) )][['session_id', 'ts', 'message']].to_csv('./entregables/nuevo_nada_de_eso.csv', index=False)

nt1=primera_instancia2[np.logical_and(primera_instancia2.response_intent_id!='RuleBuilder:PLBWX5XYGQ2B3GP7IN8Q-alfafc@gmail.com-1536777380652', ~primera_instancia2.response_message.isna())]
nt1=nt1[nt1.usuario.isin(mm2.usuario.values)]
nt1[['session_id', 'ts',  'message']].to_csv('./entregables/nuevo_texto1.csv', index=False)

na1=primera_instancia2[primera_instancia2.response_message.isna()]
na1[na1.usuario.isin(mm2.usuario.values)][['session_id','ts', 'message']].to_csv('./entregables/nuevo_abandono1.csv', index=False)


# In[ ]:




