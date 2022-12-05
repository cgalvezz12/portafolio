'''
Responsable: Vicente Morales
Creadores: Vicente Morales, Walter Guzman, Benjamin Ull
 '''
'''
Este código está programado con el fin de optimizar el proceso de obtención y envío de cartolas de clientes 
directos de LVAM. Para cumplir con este objetivo se han creado funciones que permiten el envío de cartolas
a pedido según solicite el cliente y el envío automático según períodos establecidos por el cliente. Se ha
establecido una conexión directa con SQL Server, donde se obtienen todos los datos necesarios para generar
la cartola que corresponda.
'''

'''
Primero, se cargan todos los paquetes y librerías utilizadas para el desarrollo
'''
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from random import randint
from statistics import mean
from reportlab.pdfbase import pdfmetrics
import os
import paramiko
import traceback
import sys
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.units import inch 
import reportlab.rl_config
from reportlab.lib.colors import Color, pink, black, red, blue, green, gray,midnightblue,fidblue
reportlab.rl_config.warnOnMissingFontGlyphs = 0
import pandas as pd
import pyodbc
from datetime import datetime
from datetime import timedelta
from datetime import date
import calendar
import class_proceso_datamart as DM_proc
import openpyxl
from pandas import ExcelWriter


'''
A continuación se definen las funciones utilizadas para cargar las distintas bases de datos que se
necesitan para generar una cartola. Son funciones que realizan consultas a SQL
'''

def clientes_directos (cnxn2,query_fondos_rd):
    """
    Esta función ejecuta una consulta en SQL Server, que obtiene los datos necesarios para saber qué clientes directos de LVAM están vigentes,
    a cuáles corresponde enviarles automáticamente la cartola en algún período que puede ser diario,
    semanal o solo mensual y qué clientes solicitan la cartola "a pedido" con períodos variables al analista
    """
    Query_clientes = query_fondos_rd + \
    """SELECT ruta.[rut]
      ,ruta.[nombre]
      ,ruta.[direccion]
      ,ruta.[comuna]
      ,ruta.[ciudad]
      ,ruta.[ruta_destino_protocolo]
      ,ruta.[ruta_servidor_destino]
      ,ruta.[ruta_puerto]
      ,ruta.[ruta_direccion_destino]
      ,ruta.[ruta_usuario]
      ,ruta.[ruta_clave]
      ,ruta.[ruta_usuario_consulta]
      ,ruta.[ruta_clave_consulta]
      ,ctas.[rut]
      ,ctas.[cuenta]
      ,ctas.[ind_vigente]
      ,ctas.[envio_automatico]
      ,ctas.[ind_gen_pendiente]
      ,ctas.[fecha_ini]
      ,ctas.[fecha_fin]
      ,ctas.[gen_excel]
      FROM dbo.DM_ClientesDirectos_RutaCartolas ruta
      LEFT JOIN dbo.DM_ClientesDirectos_Cuentas ctas
      ON ruta.rut = ctas.rut"""

    tabla = pd.read_sql_query(Query_clientes, cnxn2)
    tabla.set_axis(['Rut', 'nombre', 'direccion', 'comuna', 'ciudad',
       'ruta_destino_protocolo', 'ruta_servidor_destino', 'ruta_puerto',
       'ruta_direccion_destino', 'ruta_usuario', 'ruta_clave',
       'ruta_usuario_consulta', 'ruta_clave_consulta', 'rut', 'Cuenta_par',
       'ind_vigente', 'envio_automatico', 'ind_gen_pendiente', 'fecha_ini',
       'fecha_fin','gen_excel'], axis="columns",inplace=True)

    return tabla



def query_valor_cuota(Fecha, Codigo_Fdo, Codigo_Ser,query_fondos_rd,cnxn3):
    Query_vc = query_fondos_rd + \
    "SELECT [Fecha]\n"\
    ",[Fondo] as Fondo \n"\
    ",[Serie] as Serie\n"\
    ",[Valor_Cuota]\n"\
    ",[Moneda_base] \n"\
    "FROM [Golf_Analisis].[dbo].[DM_SERIES] ser \n"\
    "left join [Golf_Analisis].[dbo].[DM_Fondos] fd on fd.[Nemotecnico] = ser.[Fondo] \n"\
    "Where Fecha = '"+ Fecha + "' and Fondo = '"+Codigo_Fdo+"' and Serie = '"+Codigo_Ser+"'\n"
    table = pd.read_sql_query(Query_vc, cnxn3)
    return table



def run_query_saldo(rut, fecha_in,query_fondos_rd,cnxn5):
    '''
    Esta función ejecuta una consulta en SQL Server, que permite obtener los datos necesarios para calcular el
    saldo de los clientes
    '''
    Query_saldo = query_fondos_rd + \
        "SELECT [Rut_Par] \n" \
        ",[Cuenta_Par] \n" \
        ",[Rut_Aut] \n"\
        ",[Fecha]    \n"\
        ",[Fec_Ing]  \n"\
        ",[Fec_Prog]  \n"\
        ",[Fec_Efec]   \n"\
        ",[Tipo_Movto] \n"\
        ",[Fol_Solicitud] \n"\
        ",[Est_Movto] \n"\
        ",[Codigo_Fdo] \n"\
        ",[Codigo_Ser] \n"\
        ",[Num_Cuotas] \n"\
        ",[Val_Cuota] \n" \
        ",[Monto] \n"\
    "FROM [BDFM].[dbo].[FM_MOVTOS_PARTICIPES] \n"\
    "where LTRIM(Rut_Par)='"+ rut +"' and Fec_Prog<'" + fecha_in +"' and Fec_Efec>='" + fecha_in +"' \n"
    table = pd.read_sql_query(Query_saldo, cnxn5)
    return table


def run_query(rut, fecha_in, fecha_fin, cuenta,query_fondos_rd,cnxn5):
    """
    Esta función ejecuta una consulta en SQL Server, que obtiene los datos necesarios para generar una cartola en base a los input de rut,
    fecha inicial y fecha final del período requerido
    """
    fecha_in2 = last_weekday(datetime.strptime(fecha_in, "%Y-%m-%d")) 
    fecha_in2 = datetime.strftime(fecha_in2, "%Y-%m-%d")

    Query_cart = query_fondos_rd + \
    "select    par.Fecha as Fecha_zis,                                         \n" \
    "          mvto.Fec_Prog as Fecha,\n" \
    "          par.Codigo_Fdo,  \n" \
    "          par.Codigo_Ser,  \n" \
    "          par.Rut_Par,  \n" \
    "          par.Cuenta_Par,  \n" \
    "          ISNULL(mvto.[Val_Cuota], 0) AS Val_Cuota2, \n" \
    "          mvto.Glosa, \n"\
    "          cd.[Codigo_Cod] as Tipo_inversion,\n" \
    "          mvto.Ind_trf, \n"\
    "          mvto.Fol_Solicitud, \n"\
    "          SUM(par.Num_Cuotas) Saldo_en_Cuotas, \n" \
    "          ISNULL(mvto.[Tipo_Movto], '') AS Tipo_Movto, \n" \
    "          ISNULL(mvto.[Num_Cuotas], 0) AS Movto_en_Cuotas, \n" \
    "          mvto.Est_Movto,                               \n" \
    "          --ISNULL(mvto.[Val_Cuota], 0) AS Valor_Cuota, \n" \
    "          ISNULL(mvto.[Monto], 0) AS Monto \n" \
    "from [BDFM].[dbo].[FM_ZHIS_FONDOS_PARTICIPES] par \n" \
    "left join [BDFM].[dbo].[FM_MOVTOS_PARTICIPES] mvto ON (dateadd(day,1,par.Fecha) = mvto.Fec_Prog  or par.Fecha = mvto.Fec_Prog) \n" \
    "                                                  and par.Rut_Par = mvto.Rut_Par  \n" \
    "                                                  and par.Cuenta_Par = mvto.Cuenta_Par  \n" \
    "                                                  and par.Codigo_Fdo = mvto.Codigo_Fdo  \n" \
    "                                                  and par.Codigo_Ser = mvto.Codigo_Ser \n" \
    "left join [BDFM].[dbo].[FM_CODIGOS] cd ON mvto.Tipo_Cartera = cd.ID \n" \
    "where LTRIM(par.[Rut_Par]) = '"+ rut +"'   \n" \
    "and (par.fecha between '" + fecha_in2 +"' and '" + fecha_fin +"') and par.Cuenta_par = '" + cuenta +"' and (mvto.Fec_Prog between '" + fecha_in + "' and '" + fecha_fin + "' or mvto.Fec_Prog is null)\n"\
    "group by par.Fecha,  \n" \
    "          mvto.Fec_Prog, \n" \
    "          par.Codigo_Fdo,  \n" \
    "          par.Codigo_Ser,  \n" \
    "          par.Rut_Par,  \n" \
    "          par.Cuenta_Par, \n" \
    "          mvto.Glosa, \n"\
    "          mvto.Ind_trf, \n"\
    "          mvto.Fol_Solicitud, \n"\
    "          cd.[Codigo_Cod], \n"\
    "          ISNULL(mvto.[Tipo_Movto], ''),  \n" \
    "          ISNULL(mvto.[Num_Cuotas], 0),  \n" \
    "          ISNULL(mvto.[Val_Cuota], 0),  \n" \
    "          mvto.Est_Movto,             \n" \
    "          ISNULL(mvto.[Monto], 0)  \n" \
    " ORDER BY par.Codigo_Fdo, par.Codigo_Ser, par.Fecha  \n" 
    
    table = pd.read_sql_query(Query_cart, cnxn5)
    table = table[table["Est_Movto"]!="A"]
    
    return table  

def nombre_fondo(fondo, cnxn3):
    Query_fondo = query_fondos_rd + \
    """
    SELECT [Nombre]
    FROM [GOLF_ANALISIS].[dbo].[DM_Fondos]
    Where Nemotecnico = '{fondo}'""".format(fondo=fondo)
    
    table=pd.read_sql_query(Query_fondo,cnxn3)
    
    return table

def Desc_error():
    """
    Se define esta funcion con el fin de obtener los errores que entrega python que se generen cuando 
    se ejecuten las distintas funciones del código
    """
    ex_type, ex_value, ex_traceback = sys.exc_info()
    trace_back = traceback.extract_tb(ex_traceback)
    stack_trace = []
    for trace in trace_back:
        stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))
    Msje_error = ['\t Exception Type: {} \n'.format(str(ex_type.__name__))
        , '\t Exception message: {} \n'.format(str(ex_value))
        , '\t Trace back: {} \n\n\n'.format(str(stack_trace))]
    return Msje_error


"""
A continuación se definen las funciones que dan formato y generan la cartola, están las funciones formato,
cantidad_de_páginas y gencartola. Se ordenan los datos de formato de la cartola en orden decreciente y los
movimientos en orden cronológico y decreciente.
"""
def formato(registro,Valor_Cuota,c, DF, ruta_imagen ,Rut, Fecha_inicio, Fecha_final, Saldo_inicial, Saldo_final, Nombre, Direccion, Comuna, Ciudad, Moneda):
    """
    Se define esta funcion con el fin de generar el formato de la nueva hoja, en aquellos casos que la cartola 
    tenga muchos movimientos y se necesite generar una nueva hoja para un mismo fondo y serie.
    """
    w, h = A4
    ###registro corresponde a las variables de cada cliente que se utilizan para generar la cartola###
    Cuenta= str(registro["Cuenta_Par"])
    Fondo= registro["Codigo_Fdo"]
    Cuota=registro["Codigo_Ser"]
    c.setFont('Helvetica', 12.7)
    c.setFillColorRGB(0.02, 0.02, 0.36)
    c.drawString(25, h-85, "Cartola de Movimientos y Saldos")
    c.setStrokeColorRGB(0.02, 0.02, 0.36)
    c.line(25,h-90,430,h-90)
    c.setFillColor("black")
    c.setFont('Times-Roman', 9)
    c.drawString(25, h-105, "LARRAINVIAL ASSET MANAGEMENT ADM. GENERAL DE FONDOS S.A. ")
    c.setFont('Times-Roman', 8)
    c.setFillColor("grey")
    c.drawString(25, h-117, "Fondo:")
    c.setFont('Times-Bold', 8)
    c.setFillColor("black")
    c.drawString(90, h-117, Fondo)
    c.setFont('Times-Roman', 8)
    c.setFillColor("grey")
    c.drawString(25, h-129, "Periodo:")
    c.drawString(90, h-129, Fecha_inicio+" al "+ Fecha_final)
    c.drawString(25, h-141, "Agente:")
    c.drawString(25, h-153, "Agencia:")
    c.drawString(25, h-165, "Canal:")
    c.setStrokeColorRGB(0.02, 0.32, 0.56)
    c.setFont('Times-Bold', 7.8)
    c.drawString(55, h-183, str(Nombre))
    c.drawString(55, h-195, str(Direccion))
    c.setFont('Times-Bold', 7)
    #c.drawString(30, h-207, str(Comuna)) <- descomentar cuando este lista la variable comuna
    #c.drawString(130, h-207, " \ " + str(Ciudad)) <- descomentar cuando este lista la variable Ciudad
    c.line(25,h-175,25,h-212)
    c.line(25,h-212,250,h-212)
    c.setFillColorRGB(0.02, 0.02, 0.36)
    c.setFont("Helvetica-Bold", 7)
    c.drawString(365, h-183, "Rut")
    c.drawString(365, h-195, "Cuenta")
    c.drawString(365, h-207, "Cuota")
    c.drawString(435, h-183, ":")
    c.drawString(435, h-195, ":")
    c.drawString(435, h-207, ":")
    c.setFont("Times-Bold", 7)
    c.setFillColor("grey")
    c.drawString(480, h-183, Rut)
    c.drawString(507, h-195, Cuenta)
    c.drawString(510, h-207, Cuota)
    c.line(350,h-175,350,h-212)
    c.line(350,h-212,550,h-212)
    c.drawImage(ruta_imagen,w - 150, h-140, width=120, height=100)
    c.setFillColorRGB(0.02, 0.02, 0.36)
    c.rect(25,600,w-50,20, fill=1)
    rt = Color(0.5, 0.5, 0.5, alpha=0.25 )
    c.setFillColor(rt)
    c.rect(25,577,w-50,20, fill=1, stroke=0)
    c.setFillColor("midnightblue")
    c.setFont('Times-Roman', 8.5)
    c.drawString(30, 590, "Fecha")
    c.drawString(90, 590, "Operación")
    c.drawString(140, 590, "Tipo Mvto.")
    c.drawString(210, 590, "Tipo")
    c.drawString(205, 580, "Inversión")
    if Moneda=="USD":
        c.drawString(260, 590, "Monto  (US$)")
    else:
        c.drawString(260, 590, "Monto  ($)")
    c.drawString(340, 590, "Valor")
    c.drawString(340, 580, "Cuota")
    c.drawString(400, 590, "Mvto.")
    c.drawString(395, 580, "en Cuotas")
    c.drawString(460, 590, "Saldo")
    c.drawString(455, 580, "en Cuotas")
    c.drawString(510, 590, "Rut")
    c.drawString(510, 580, "Autorizado")
    c.setFillColor("grey")
    c.setFont("Helvetica-Bold", 6.5)
    c.drawString(55, 568, "Saldo inicial")
    c.drawString(360, 568, str(formatnum(Saldo_inicial)))
    DF['Fecha'] = pd.to_datetime(DF['Fecha'], format='%Y-%m-%d')
    df1 = DF[DF["Rut_Par"]==Rut]
    df2 = df1[df1["Fecha"]>=datetime.strptime(Fecha_inicio, '%Y-%m-%d')]
    df3 = df2[df2["Movto_en_Cuotas"]>0]
    Serie=registro["Codigo_Ser"] 
    df3 = df3[df3["Codigo_Fdo"]==Fondo]
    df3=df3[df3["Codigo_Ser"]== Serie]
    df3=df3.drop_duplicates(["Fol_Solicitud"])
    c.setStrokeColor(midnightblue)
    c.line(25,602,25,80)
    c.line(w-25,602,w-25,80)
    c.line(25,80,w-25,80)
    c.setStrokeColor(fidblue)
    c.line(25,100,w-25,100)
    c.setFillColor("midnightblue")
    c.drawString(30, 87, "Valor Cuota al")
    if Moneda=="USD":
        c.drawString(440, 87, "(US$)")
    else:
        c.drawString(440, 87, "($)")
    c.setFillColor("grey")
    c.setFont("Helvetica-Bold", 6.5)
    c.drawString(100, 87, Fecha_final)
    if Valor_Cuota==0:
        c.drawString(260, 87, "--")
        c.drawString(335, 87, "Saldo")
        c.drawString(510, 87, "--")  
    else:
        c.drawString(260, 87, str(formatnum(Valor_Cuota)))
        c.drawString(335, 87, "Saldo")
        if Moneda=="USD":
            c.drawString(510, 87, str(formatnum(Saldo_final)))
        else:
            c.drawString(510, 87, str(formatnum2(Saldo_final)))  
    c.setFillColor("grey")
    c.setFont("Helvetica-Bold", 5.3)
    c.drawString(30, 74, "Nota : Rogamos a usted(es) hacernos llegar su conformidad u observaciones sobre las operaciones y saldos referidos en este documento, dentro del plazo de 10 días contados desde esta fecha. Trancurrido este")
    c.drawString(30, 67, "plazo sin recibir su respuesta, entenderemos que usted(es) otorgan su conformidad. ")

        
def cantidad_de_paginas(DF,Rut,Fecha_inicio,Fecha_final,Cuenta):
    """
    La funcion cantidad_de_paginas, permite obtener la cantidad de paginas total de la cartola utilizando:
    DF, que corresponde a la bbdd de todos los movimientos del cliente indicado según las variables:
    Rut, una fecha de inicio y una final
    """
    dfaux=DF[DF["Rut_Par"]==Rut]
    if Cuenta != "":
        dfaux=dfaux[dfaux["Cuenta_Par"]==Cuenta]

    dfaux["Fecha_zis"]=pd.to_datetime(dfaux["Fecha_zis"], format='%Y-%m-%d')
    dfaux2 = dfaux
    dfaux2["mov"]=1
    dfaux2=dfaux2.groupby(by=["Codigo_Fdo", "Codigo_Ser"],as_index=False).agg({"mov":"sum"})
    dfaux = dfaux[dfaux["Movto_en_Cuotas"]>0]
    dfaux = dfaux.drop_duplicates(["Fol_Solicitud"])
    dfaux["mov"]=1
    dfaux=dfaux.groupby(by=["Codigo_Fdo", "Codigo_Ser"],as_index=False).agg({"mov":"sum"})
    dfaux["mov2"]=dfaux["mov"]//36+1*(dfaux["mov"]>0)
    paginas=dfaux["mov2"].sum()+len(dfaux2)-len(dfaux)
    return paginas


def gencartola(registro,DF, Valor_Cuota, c, Fecha_inicio, Fecha_final, ruta_imagen, Rut, Saldo_inicial, Saldo_final, Moneda):
  
    """
    la funcion gencartola, permite generar una cartola para un rut y una cuenta indicada en un rango de fechas.
    Distinguiendo según el Fondo, su serie y cuenta del cliente, generando hojas distintas para cada tupla fondo,
    serie, cuenta.
    Recibe como input: registro que incluye el fondo y la serie 
    DF que incluye todos los movimientos 
    dato que incluye el valor cuota correspondiente
    c es un archivo reportlab sobre el cual se seguiran generando hojas de la cartola
    Fecha_inicio, Fecha_final, ruta_imagen, Rut, Saldo_inicial, Saldo_final son parametros que seran incluidos en el PDF
    """
      
    w, h = A4
    Fondo = registro["Codigo_Fdo"]
    Rutdb = registro["Rut_Par"].strip()
    Cuenta= str(registro["Cuenta_Par"])
    dbrut=cd[cd["Rut"]==Rutdb]
    dbrut=dbrut[dbrut["Cuenta_par"]==Cuenta]
    Nombre=dbrut.iloc[0]["nombre"]
    Direccion=dbrut.iloc[0]["direccion"]
    Comuna=dbrut.iloc[0]["comuna"]
    Ciudad=dbrut.iloc[0]["ciudad"]
    Cuota=registro["Codigo_Ser"]     
    c.setFont('Helvetica', 12.7)
    c.setFillColorRGB(0.02, 0.02, 0.36)
    c.drawString(25, h-85, "Cartola de Movimientos y Saldos")
    c.setStrokeColorRGB(0.02, 0.02, 0.36)
    c.line(25,h-90,430,h-90)
    c.setFillColor("black")
    c.setFont('Times-Roman', 9)
    c.drawString(25, h-105, "LARRAINVIAL ASSET MANAGEMENT ADM. GENERAL DE FONDOS S.A. ")
    c.setFont('Times-Roman', 8)
    c.setFillColor("grey")
    c.drawString(25, h-117, "Fondo:")
    c.setFont('Times-Bold', 8)
    c.setFillColor("black")
    c.drawString(90, h-117, Fondo)
    c.setFont('Times-Roman', 8)
    c.setFillColor("grey")
    c.drawString(25, h-129, "Periodo:")
    c.drawString(90, h-129, Fecha_inicio+" al "+Fecha_final)
    c.drawString(25, h-141, "Agente:")
    c.drawString(25, h-153, "Agencia:")
    c.drawString(25, h-165, "Canal:")
    c.setStrokeColorRGB(0.02, 0.32, 0.56)
    c.setFont('Times-Bold', 7.8)
    c.drawString(55, h-183, str(Nombre))
    c.drawString(55, h-195, str(Direccion))
    c.setFont('Times-Bold', 7)
    #c.drawString(30, h-207, str(Comuna)) <- descomentar cuando este lista la variable comuna
    #c.drawString(130, h-207, " \ " + str(Ciudad)) <- descomentar cuando este lista la variable ciudad
    c.line(25,h-175,25,h-212)
    c.line(25,h-212,250,h-212)
    c.setFillColorRGB(0.02, 0.02, 0.36)
    c.setFont("Helvetica-Bold", 7)
    c.drawString(365, h-183, "Rut")
    c.drawString(365, h-195, "Cuenta")
    c.drawString(365, h-207, "Cuota")
    c.drawString(435, h-183, ":")
    c.drawString(435, h-195, ":")
    c.drawString(435, h-207, ":")
    c.setFont("Times-Bold", 7)
    c.setFillColor("grey")
    c.drawString(480, h-183, Rut)
    c.drawString(507, h-195, Cuenta)
    c.drawString(510, h-207, Cuota)
    c.line(350,h-175,350,h-212)
    c.line(350,h-212,550,h-212)
    c.drawImage(ruta_imagen,w - 150, h-140, width=120, height=100)
    c.setFillColorRGB(0.02, 0.02, 0.36)
    c.rect(25,600,w-50,20, fill=1)
    rt = Color(0.5, 0.5, 0.5, alpha=0.25 )
    c.setFillColor(rt)
    c.rect(25,577,w-50,20, fill=1, stroke=0)
    c.setFillColor("midnightblue")
    c.setFont('Times-Roman', 8.5)
    c.drawString(30, 590, "Fecha")
    c.drawString(90, 590, "Operación")
    c.drawString(140, 590, "Tipo Mvto.")
    c.drawString(210, 590, "Tipo")
    c.drawString(205, 580, "Inversión")
    if Moneda=="USD":
        c.drawString(260, 590, "Monto  (US$)")
    else:
        c.drawString(260, 590, "Monto  ($)")
    c.drawString(340, 590, "Valor")
    c.drawString(340, 580, "Cuota")
    c.drawString(400, 590, "Mvto.")
    c.drawString(395, 580, "en Cuotas")
    c.drawString(460, 590, "Saldo")
    c.drawString(455, 580, "en Cuotas")
    c.drawString(510, 590, "Rut")
    c.drawString(510, 580, "Autorizado")
    c.setFillColor("grey")
    c.setFont("Helvetica-Bold", 6.5)
    c.drawString(55, 568, "Saldo incial")
    c.drawString(360, 568, formatnum(Saldo_inicial))
    #DF se define con la función run_query antes de entregarla como input a esta función o a la función formato
    #Aquí se filtra DF para obtener los movimientos correspondientes al cliente y a las fechas indicadas
    DF['Fecha'] = pd.to_datetime(DF['Fecha'], format='%Y-%m-%d')
    df1 = DF[DF["Rut_Par"]==Rut]
    df1= df1.sort_values(by="Fecha")
    df2 = df1[df1["Fecha"]>=datetime.strptime(Fecha_inicio, '%Y-%m-%d')]
    df3 = df2[df2["Movto_en_Cuotas"]>0]
    Serie=registro["Codigo_Ser"] ##Serie
    df3 = df3[df3["Codigo_Fdo"]==Fondo]
    df3=df3[df3["Codigo_Ser"]== Serie]
    df3=df3.drop_duplicates(["Fol_Solicitud"])
    Fecha = df3["Fecha"]
    Mcuotas = df3["Movto_en_Cuotas"]
    TMovto=df3["Tipo_Movto"]
    Monto=df3["Monto"]
    VCuota2=df3["Val_Cuota2"]
    Rut1=df3["Rut_Par"]
    SCuotas=df3["Saldo_en_Cuotas"]
    Glos=df3["Glosa"]
    Indtrf=df3["Ind_trf"]
    FolSol=df3["Fol_Solicitud"]
    Tinversion=df3["Tipo_inversion"]
    d=12 #Se define la variable d que indica una posición en el eje vertical de la cartola
    largo = cantidad_de_paginas(DF,Rut,Fecha_inicio,Fecha_final, Cuenta)
    """
    A continuación, se escriben los movimientos que van en la cartola. Cuando la posición d es mayor a 432, lo que
    se produce cuando hay más de 36 movimientos en una hoja, se termina la hoja actual y se crea una nueva hoja
    """
    if len(df3)>0:
        Saldo_encuotas=Saldo_inicial
        for i in range(len(df3)):
            if TMovto.iloc[i] == "R":
                Saldo_encuotas=Saldo_encuotas- Mcuotas.iloc[i]
            else:
                Saldo_encuotas=Saldo_encuotas+ Mcuotas.iloc[i]
            while d>432:
                c.line(25,602,25,80)
                c.line(w-25,602,w-25,80)
                c.line(25,80,w-25,80)
                c.setStrokeColor(fidblue)
                c.line(25,100,w-25,100)
                c.setFillColor("midnightblue")
                c.drawString(30, 87, "Valor Cuota al")
                if Moneda=="USD":
                    c.drawString(440, 87, "(US$)")
                else:
                    c.drawString(440, 87, "($)")
                c.setFillColor("grey")
                c.setFont("Helvetica-Bold", 6.5)
                c.drawString(100, 87, Fecha_final)
                if Valor_Cuota==0:
                    c.drawString(260, 87, "--")
                    c.drawString(335, 87, "Saldo")
                    c.drawString(510, 87, "--")  
                else:
                    c.drawString(260, 87, str(formatnum(Valor_Cuota)))
                    c.drawString(335, 87, "Saldo")
                    if Moneda=="USD":
                        c.drawString(510, 87, str(formatnum(Saldo_final)))
                    else:
                        c.drawString(510, 87, str(formatnum2(Saldo_final)))
                c.setFillColor("grey")
                c.setFont("Helvetica-Bold", 5.3)
                c.drawString(30, 74, "Nota : Rogamos a usted(es) hacernos llegar su conformidad u observaciones sobre las operaciones y saldos referidos en este documento, dentro del plazo de 10 días contados desde esta fecha. Trancurrido este")
                c.drawString(30, 67, "plazo sin recibir su respuesta, entenderemos que usted(es) otorgan su conformidad. ")
                
            
                largo = cantidad_de_paginas(DF,Rut,Fecha_inicio, Fecha_final, Cuenta)
                page_num = c.getPageNumber()
                c.setFont("Helvetica-Bold", 7)
                c.drawString(40, 45, "Página: " + str(page_num) + " de " + str(largo))
                
                c.showPage() #Este comando termina la hoja actual
                d=d-432
                
                formato(registro,Valor_Cuota,c, DF, ruta_imagen ,Rut, Fecha_inicio, Fecha_final, Saldo_inicial, Saldo_final, Nombre, Direccion, Comuna, Ciudad, Moneda)
                c.setFillColor("grey")
                c.setFont("Helvetica-Bold", 6.5)
                
              
            """
            Mientras haya 36 movimientos o menos en una hoja, estos se escriben en la hoja actual y se 
            termina la cartola actual
            """
            c.setFillColor("grey")
            c.setFont("Helvetica-Bold", 6.5)
            c.drawString(30, 568-d,"{}".format(Fecha.iloc[i].strftime('%Y-%m-%d')))
            c.drawString(395, 568-d, str(formatnum(Mcuotas.iloc[i])))
            if Glos.iloc[i]=="CNJ" and Indtrf.iloc[i]=="S":
               c.drawString(150, 568-d,"TRS-CJ")
            elif Indtrf.iloc[i]=="S":
               c.drawString(150, 568-d, "TRS")
            elif Indtrf.iloc[i]=="N" and TMovto.iloc[i] == "R" :
                c.drawString(150, 568-d, "RLI")
            elif TMovto.iloc[i] == "R":
               c.drawString(150, 568-d,"RCE")
            else:
                  c.drawString(150, 568-d,"ASU")
            if TMovto.iloc[i] == "R":
               c.drawString(70, 568-d,"RE")
            if TMovto.iloc[i] == "I":
               c.drawString(70, 568-d,"IN")
            c.drawString(210, 568-d,str(Tinversion.iloc[i]))
            c.drawString(90, 568-d,str(FolSol.iloc[i]))
            c.drawString(260, 568-d, str(formatnum(Monto.iloc[i])))
            c.drawString(340, 568-d, str(formatnum(VCuota2.iloc[i])))
            c.drawString(510, 568-d, str(Rut1.iloc[i]))
            c.drawString(455, 568-d, str(formatnum(Saldo_encuotas)))
            
            
            d=d+12 #Se suma una distancia en el eje vertical por cada movimiento
            """
            Si no hay movimientos en una hoja, se escribe un mensaje al inicio y se termina de crear la hoja actual,
            esto ocurre a continuación
            """
    else:
        c.setFillColor("grey")
        c.setFont("Helvetica-Bold", 6.5)
        c.drawString(120, 556, "0")
        c.setFillColor("black")
        c.setFont("Helvetica-Bold", 7.5)
        c.drawString(35, h-220, "No registra movimiento para la fecha indicada")
      
    """
    A continuación el código que termina la cartola actual
    """    
    c.setStrokeColor(midnightblue)     
    c.line(25,602,25,80)
    c.line(w-25,602,w-25,80)
    c.line(25,80,w-25,80)    
    c.setStrokeColor(fidblue)
    c.line(25,100,w-25,100)
    c.setFillColor("midnightblue")
    c.drawString(30, 87, "Valor Cuota al")
    if Moneda=="USD":
        c.drawString(440, 87, "(US$)")
    else:
        c.drawString(440, 87, "($)")
    c.setFillColor("grey")
    c.setFont("Helvetica-Bold", 6.5)
    c.drawString(100, 87, Fecha_final)
    if Valor_Cuota==0:
        c.drawString(260, 87, "--")
        c.drawString(335, 87, "Saldo")
        c.drawString(510, 87, "--")  
    else:
        c.drawString(260, 87, str(formatnum(Valor_Cuota)))
        c.drawString(335, 87, "Saldo")
        if Moneda=="USD":
            c.drawString(510, 87, str(formatnum(Saldo_final)))
        else:
            c.drawString(510, 87, str(formatnum2(Saldo_final))) 
        
    c.setFillColor("grey")
    c.setFont("Helvetica-Bold", 5.3)
    c.drawString(30, 74, "Nota : Rogamos a usted(es) hacernos llegar su conformidad u observaciones sobre las operaciones y saldos referidos en este documento, dentro del plazo de 10 días contados desde esta fecha. Trancurrido este")
    c.drawString(30, 67, "plazo sin recibir su respuesta, entenderemos que usted(es) otorgan su conformidad. ")

  
"""
A continuación se definen algunas funciones auxiliares que se utilizan en el desarrollo
"""  
def last_weekday(day):
    """
    Esta funcion recibe un dia en formato datetime y entrega el dia de semana anterior
    """
    
    if type(day) == str:
        day = datetime.strptime(day,"%Y-%m-%d")
    last=day - timedelta(days=1)
    if last.weekday() == 6:
        last=day - timedelta(days=3)
    if last.weekday() == 5:
        last=day -timedelta(days=2)
    return last


def Saldo_fin(Saldo_inicial, Valor_Cuota,DF, registro, Fecha_inicio):
    """
    Esta función toma el saldo inicial de cada fondo en cada serie y calcula el saldo final en base
    a los movimientos realizados para el período en que se solicita la cartola, sumando los montos del
    movimiento si se realiza un ingreso/aporte y restando los montos si se realiza un retiro/rescate
    """
    Fondo=registro["Codigo_Fdo"]
    Serie=registro["Codigo_Ser"]
    Movimientos=DF[DF["Codigo_Ser"]==Serie]
    Movimientos=Movimientos[pd.to_datetime(Movimientos["Fecha"], format='%Y-%m-%d')>=datetime.strptime(Fecha_inicio,"%Y-%m-%d")]
    Movimientos=Movimientos[Movimientos["Codigo_Fdo"]==Fondo]
    Movimientos=Movimientos[Movimientos["Movto_en_Cuotas"]>0]
    Movimientos=Movimientos.drop_duplicates(["Fol_Solicitud"])
    Saldo_final=Saldo_inicial
    for i in range(len(Movimientos)):
        if Movimientos.iloc[i]["Tipo_Movto"] == "R":
            Saldo_final=Saldo_final- Movimientos.iloc[i]["Movto_en_Cuotas"]
        else:
            Saldo_final=Saldo_final+ Movimientos.iloc[i]["Movto_en_Cuotas"]
    Saldo_final=Saldo_final*Valor_Cuota
    return round(Saldo_final,4)


def Saldo_ini(df_saldo,df_saldo2,registro):
    """
    Esta función toma como saldo incial del cliente en cada fondo para cada serie el saldo del día
    anterior a la fecha de inicio del período en que se genera la cartola
    """
    
    Fondo=registro["Codigo_Fdo"]
    Serie=registro["Codigo_Ser"]
    df_saldo=df_saldo[df_saldo["Codigo_Fdo"]==Fondo]
    df_saldo=df_saldo[df_saldo["Codigo_Ser"]==Serie]
    if len(df_saldo)==0:
        return 0
    else:
        saldo = df_saldo.iloc[0]["Saldo_en_Cuotas"]
    df_saldo2=df_saldo2[df_saldo2["Codigo_Fdo"]==Fondo]
    df_saldo2=df_saldo2[df_saldo2["Codigo_Ser"]==Serie]
    df_saldo2=df_saldo2[df_saldo2["Tipo_Movto"]=="R"]
    if len(df_saldo2)>0:
        dif=df_saldo2["Movto_en_cuotas"].sum()
        return saldo - dif
    else:
        return saldo
    
    
def formatnum(numero):
    '''
    Esta función permite dar formato a los montos de saldo y valor cuota en las cartolas.
    '''
    return '{:,.4f}'.format(numero).replace(",", "@").replace(".", ",").replace("@", ".")


def formatnum2(numero):
    '''
    Esta función permite dar formato a los montos de saldo y valor cuota en las cartolas.
    '''
    return '{:,.0f}'.format(numero).replace(",", "@").replace(".", ",").replace("@", ".")

def limpieza():
    """
    Esta función permite dejar en valor 0 toda la columna de Gen_pendiente, que se utiliza para identificar
    cuando es necesario generar una cartola a pedido, en dicho caso el valor de la columna para esa/s fila/s
    es 1
    """
    cnxn = pyodbc.connect(str_conexion_dabus)
    cursor = cnxn.cursor()
    cursor.execute("UPDATE dbo.DM_ClientesDirectos_Cuentas SET ind_gen_pendiente = 0")
    cnxn.commit()
     

"""
A continuación se definen las funciones que se utilizan para 
el envío automático de cartolas y para el envío de las cartolas a través de sftp
"""       
      
def envio_automatico(db_auto, Fecha_inicio, Fecha_final, diario,query_fondos_rd,cnxn5,cnxn2,cnxn3):
    """
    Esta función genera una cartola para cada rut perteneciente a la lista de ruts que recibe como
    input y que puede contener ruts de envío diario, semanal o mensual según corresponda, cada lista
    con períodos definidos según la necesidad del cliente
    """
    if type(Fecha_inicio) == str:
        Fecha_inicio = datetime.strptime(Fecha_inicio,"%Y-%m-%d")
    a=0
    
    for j in range(len(db_auto)):
        a=a+1
        request=db_auto.iloc[j]
        #print(str((a/len(db_auto))*100)+"% de de las cartolas generadas")
        Rut = request["Rut"]
        if type(Fecha_inicio) == str:
            Fecha_inicio = datetime.strptime(Fecha_inicio,"%Y-%m-%d")
        fecin=last_weekday(Fecha_inicio)
        Fecha_inicio = datetime.strftime(Fecha_inicio, "%Y-%m-%d")
        if type(Fecha_final) == str:
            Fecha_final = datetime.strptime(Fecha_final,"%Y-%m-%d")
        fecfin=last_weekday(Fecha_final)
        Fecha_final = datetime.strftime(Fecha_final, "%Y-%m-%d")
        Fecha_VC=datetime.strptime(Fecha_final, "%Y-%m-%d") 
        Fecha_VC=datetime.strftime(Fecha_VC, "%Y-%m-%d")
        hora = datetime.strftime(datetime.today()," %HH - %MM")
        hoy = datetime.strftime(datetime.today(),"%Y-%m-%d")
        Cuenta_Par=request["Cuenta_par"]
        pdf_name= "Cartola de "  + Rut + " Cuenta " + str(Cuenta_Par)  + " entre " + Fecha_inicio + " y " +Fecha_final + hora + ".pdf"
        if diario==1:
            ruta_guardado_pdf2= ruta_guardado_pdf + datetime.strftime(tiempo_inicio - timedelta(days=1), "\\%Y-%m-%d")
        else:
            ruta_guardado_pdf2= ruta_guardado_pdf + datetime.strftime(tiempo_inicio - timedelta(weeks=1), "\\%Y-%m")
        save_name = os.path.join(ruta_guardado_pdf2, pdf_name)
        w, h = A4   
        c = canvas.Canvas(save_name, pagesize=A4)
        fecin= datetime.strftime(fecin,"%Y-%m-%d")
        fecfin= datetime.strftime(fecfin,"%Y-%m-%d")
        try:
            DF=run_query(Rut.strip(),fecin,Fecha_final, Cuenta_Par,query_fondos_rd,cnxn5)
            Rut= (11 - len(Rut))*" " + Rut
            DF['Fecha_zis'] = pd.to_datetime(DF['Fecha_zis'], format='%Y-%m-%d')
            df_saldo = DF.copy()
            df_saldo=df_saldo.sort_values(by="Fecha_zis",ascending=False)
        except:
            msje_error = Desc_error()
            error_run_query="Error querymovimientos"
            l_a.append([error_run_query, "0",msje_error,hoy])
            proceso_dm.registra_alerta_proc(error_run_query, "0",msje_error,hoy)
            pass


        if len(DF)==0:
            l_a.append([Rut+"Clientes sin custodia", Fecha_inicio, Fecha_final, "--"])
            error_apedido=Rut+"Clientes sin custodia"
            hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
            msje_error = "--"
            proceso_dm.registra_alerta_proc(error_apedido,0,msje_error,hoy1) 
            pass
        
        else:
            df5 = DF.copy()
            df5 = df5.sort_values(by="Fecha_zis", ascending=False)
            d=df5.copy()
            df5 = df5[df5["Fecha_zis"]<=datetime.strptime(fecfin, "%Y-%m-%d")]
            df5 = df5.drop_duplicates(["Codigo_Fdo","Codigo_Ser", "Cuenta_Par"])
            d=d.drop_duplicates(["Codigo_Fdo","Codigo_Ser", "Cuenta_Par"])
            df5=df5.sort_values(by=["Codigo_Fdo","Codigo_Ser", "Cuenta_Par"])
            d=d.sort_values(by=["Codigo_Fdo","Codigo_Ser"])
            fecin=datetime.strptime(fecin,"%Y-%m-%d")
            df_saldo=df_saldo[df_saldo["Fecha_zis"]<datetime.strptime(Fecha_inicio, '%Y-%m-%d')]
            df_saldo=df_saldo.drop_duplicates(["Codigo_Fdo","Codigo_Ser", "Cuenta_Par"])
            df_saldo2=run_query_saldo(Rut, Fecha_inicio,query_fondos_rd,cnxn5)
            Cuenta = request["Cuenta_par"] 
            if len(df5)!=0:
                for i in range(len(df5)):
                    """
                    En este "for" se ejecutan las funciones necesarias para generar la cartola en base al arreglo
                    realizado antes en los input recibidos
                   """
                    registro=df5.iloc[i]
                    Cuota=registro["Codigo_Ser"]
                    Fondo=registro["Codigo_Fdo"]
                    Saldo_inicial=Saldo_ini(df_saldo, df_saldo2,registro)
                    try:
                        Valor_Cuota2 = query_valor_cuota(Fecha_VC, Fondo, Cuota,query_fondos_rd,cnxn3)
                        Moneda = Valor_Cuota2["Moneda_base"][0]
                        Valor_Cuota=Valor_Cuota2["Valor_Cuota"][0]
                        Saldo_final=Saldo_fin(Saldo_inicial, Valor_Cuota, DF, registro, Fecha_inicio)
                        
                    except:
                        Valor_Cuota=0
                        Saldo_final=Saldo_fin(Saldo_inicial, Valor_Cuota, DF, registro, Fecha_inicio)
                        msje_error=Desc_error()
                        error_d=Fondo +" "+ Cuota + "ValorCuota no encontrado"
                        hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
                        l_a.append([Fondo +" "+ Cuota +" ValorCuota no encontrado", Fecha_inicio, Fecha_final, "--"])
                        proceso_dm.registra_alerta_proc(error_d,0,msje_error[2],hoy1)
                        pass
                    gencartola(registro,DF, Valor_Cuota, c, Fecha_inicio, Fecha_final, ruta_imagen, Rut, Saldo_inicial, Saldo_final, Moneda)
                    largo = cantidad_de_paginas(DF,Rut,Fecha_inicio, Fecha_final, Cuenta)
                    page_num = c.getPageNumber()
                    c.setFont("Helvetica-Bold", 7)
                    c.drawString(40, 45, "Página: " + str(page_num) + " de " + str(largo))
                    c.showPage()
                c.save()
                if os.stat(save_name).st_size < 1000:
                    os.remove(save_name)
            else:
                l_a.append([Rut+"Clientes sin custodia", Fecha_inicio, Fecha_final, "--"])
                error_apedido=Rut+"Clientes sin custodia"
                hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
                msje_error = "--"
                proceso_dm.registra_alerta_proc(error_apedido,0,msje_error,hoy1) 
                pass

            
            usuario=request["ruta_usuario"]
            clave=request["ruta_clave"]
            servidor_destino=request["ruta_servidor_destino"]
            ruta_destino=request["ruta_direccion_destino"]
            try:
                envio_sftp(usuario, clave, save_name, ruta_destino, servidor_destino, pdf_name,diario)
            except:
                msje_error = Desc_error()
                hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
                error_sftp="Error en envío de cartolaa sftp"
                proceso_dm.registra_alerta_proc(error_sftp,0,msje_error[2],hoy1)
                pass
         
            
def GenerarExcel(ruta_guardado, Pestañas, Data):
    """
    Esta función toma una lista de bases de datos y una lista con el nombre de las pestañas para
    crear un libro de excel con 1 pestaña para cada bbdd ingresada. Actualmente se utiliza 1 bbdd y
    1 pestaña, pero se utiliza esta lógica en caso que los clientes soliciten añadir más información 
    en el libro de excel
    """
    wb = openpyxl.Workbook()
    writer=ExcelWriter(ruta_guardado) 
    for pestaña in Pestañas:
        wb.create_sheet(pestaña)
    std=wb.get_sheet_by_name('Sheet')
    wb.remove_sheet(std)
    wb.save(ruta_guardado)
    for i in range(len(Pestañas)):
        Data[i].to_excel(writer, sheet_name=Pestañas[i], index=False)
    writer.save()
    
def ExcelCartola(db_auto,Fecha_inicio,Fecha_final, diario,cnxn3):
    """
    Esta función crea un excel con los movimientos de las cartolas solicitadas para aquellos 
    clientes que lo requieran, es decir, aquellos que tengan un 1 en la columna "gen_excel". Recibe 
    una bbdd, la fecha de inicio y fecha final de la/s cartola/s solicitada/s y se encarga de 
    realizar las modificaciones necesarias a la bbdd que recibe para entregarla como una lista a la 
    función GenerarExcel.
    """    
    db_auto = db_auto[db_auto["gen_excel"] == 1]
    if type(Fecha_inicio) == str:
        Fecha_inicio = datetime.strptime(Fecha_inicio,"%Y-%m-%d")
    a=0
    for j in range(len(db_auto)):
        a=a+1
        request=db_auto.iloc[j]
        Rut = request["Rut"]
        if type(Fecha_inicio) == str:
            Fecha_inicio = datetime.strptime(Fecha_inicio,"%Y-%m-%d")
        fecin=last_weekday(Fecha_inicio)
        Fecha_inicio = datetime.strftime(Fecha_inicio, "%Y-%m-%d")
        if type(Fecha_final) == str:
            Fecha_final = datetime.strptime(Fecha_final,"%Y-%m-%d")
        fecfin=last_weekday(Fecha_final)
        Fecha_final = datetime.strftime(Fecha_final, "%Y-%m-%d")
        Fecha_VC=datetime.strptime(Fecha_final, "%Y-%m-%d") 
        Fecha_VC=datetime.strftime(Fecha_VC, "%Y-%m-%d")
        hora = datetime.strftime(datetime.today()," %HH - %MM")
        hoy = datetime.strftime(datetime.today(),"%Y-%m-%d")
        fecin= datetime.strftime(fecin,"%Y-%m-%d")
        fecfin= datetime.strftime(fecfin,"%Y-%m-%d")
        Cuenta_Par=request["Cuenta_par"]
        excel_name= "Excel de "  + Rut + " Cuenta " + str(Cuenta_Par)  + " entre " + Fecha_inicio + " y " +Fecha_final + hora + ".xlsx" 
        try:
            DF=run_query(Rut.strip(),fecin,Fecha_final, Cuenta_Par,query_fondos_rd,cnxn5)
            Rut= (11 - len(Rut))*" " + Rut
            DF['Fecha_zis'] = pd.to_datetime(DF['Fecha_zis'], format='%Y-%m-%d')
            df_saldo = DF.copy()
            df_saldo=df_saldo.sort_values(by="Fecha_zis",ascending=False)
        except:
            msje_error = Desc_error()
            error_run_query="Error querymovimientos"
            l_a.append([error_run_query, "0",msje_error,hoy])
            proceso_dm.registra_alerta_proc(error_run_query, "0",msje_error,hoy)
            pass
        DF['Fecha'] = pd.to_datetime(DF['Fecha'], format='%Y-%m-%d')
        DF = DF[DF["Rut_Par"]==Rut]
        DF= DF.sort_values(by="Fecha")
        DF = DF[DF["Fecha"]>=datetime.strptime(Fecha_inicio, '%Y-%m-%d')]
        DF = DF[DF["Movto_en_Cuotas"]>0]
        DF=DF.drop_duplicates(["Fol_Solicitud"])
        lfondos=[]
        for i in range(len(DF)):
            nemo=DF.iloc[i]["Codigo_Fdo"]
            fondo=nombre_fondo(nemo, cnxn3).iloc[0]["Nombre"]
            lfondos.append(fondo)
        DF["Fondo"] = lfondos
        DF.drop(['Rut_Par', 'Cuenta_Par', 'Glosa', 'Ind_trf', 'Fol_Solicitud', 'Saldo_en_Cuotas', 'Tipo_inversion','Est_Movto','Fecha_zis'], axis=1, inplace=True)
        DF = DF[['Fecha','Codigo_Fdo','Codigo_Ser','Fondo','Tipo_Movto','Movto_en_Cuotas','Val_Cuota2','Monto']]
        DF['Tipo_Movto']=DF['Tipo_Movto'].replace("I", "COMPRA").replace("R", "VENTA")
        DF.set_axis(['Fecha Transacción', 'Nemotecnico SVS', 'Serie', 'Fondo', 'Transacción', 'Cantidad de Cuotas', 'Valor Cuota', 'Monto'], axis='columns',inplace=True)
        if diario==1:
            ruta_guardado_pdf2= ruta_guardado_pdf + datetime.strftime(tiempo_inicio - timedelta(days=1), "\\%Y-%m-%d")
        else:
            ruta_guardado_pdf2= ruta_guardado_pdf + datetime.strftime(tiempo_inicio - timedelta(weeks=1), "\\%Y-%m")
        save_name = os.path.join(ruta_guardado_pdf2, excel_name)
        db=[DF]
        pestañas=["movimientos"] 
        if len(DF)==0:
            pass
        else:
            try:
                GenerarExcel(save_name, pestañas, db)
            except:
                hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
                msje_error = Desc_error()
                msje_error
                error_xlsx="error función GenerarExcel"
                l.append([error_xlsx, Rut, Cuenta_Par, msje_error])
                proceso_dm.registra_alerta_proc(error_xlsx,0,msje_error[2],hoy1)
        
        usuario=request["ruta_usuario"]
        clave=request["ruta_clave"]
        servidor_destino=request["ruta_servidor_destino"]
        ruta_destino=request["ruta_direccion_destino"]
        try:
            envio_sftp(usuario, clave, save_name, ruta_destino, servidor_destino, excel_name,diario)
        except:
            msje_error = Desc_error()
            hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
            error_sftp="Error en envío de cartolaa sftp"
            proceso_dm.registra_alerta_proc(error_sftp,0,msje_error[2],hoy1)
            l.append([error_sftp, "", "", msje_error])
            pass


def envio_semanal_mensual():
    """
    Esta funcion ve qué fecha corresponde al día de hoy y genera cartolas con la función de envío automático,
    las cartolas generadas puedens ser de envío semanal si es lunes, cartolas de envío mensual si hoy el
    primer día hábil del mes y si coincide el primer día hábil del mes con un día lunes, genera cartolas 
    semanales para los clientes que habitualmente solicitan cartolas en ese período y genera cartolas mensuales
    para todos los clientes vigentes en LVAM
    """ 
    if not os.path.isdir(ruta_guardado_pdf + datetime.strftime(tiempo_inicio - timedelta(weeks=1), "\\%Y-%m")):
        os.mkdir(ruta_guardado_pdf + datetime.strftime(tiempo_inicio - timedelta(weeks=1), "\\%Y-%m"))
    diario=0
    if hoy.weekday()==0 and (hoy.strftime("%d")=="01" or hoy.strftime("%d")=="02" or hoy.strftime("%d")=="03"):
        Fecha_inicio=primer_dia_mespasado
        Fecha_inicio3=hoy-delta_semanal
        Fecha_final=ultimo_diamp
        Fec_final=hoy-timedelta(days=1)
        if len(db_coincidencia)==0:
             l_a.append(["Sin clientes para el envío mensual y semanal", "--", "--", "--"])
        else:
            try:
                envio_automatico(db_semanal, Fecha_inicio3, Fec_final , diario,query_fondos_rd,cnxn5,cnxn2,cnxn3)
                envio_automatico(db_coincidencia, Fecha_inicio, Fecha_final, diario,query_fondos_rd,cnxn5,cnxn2,cnxn3)
                try:
                    ExcelCartola(db_semanal,Fecha_inicio,Fec_final, diario)
                    ExcelCartola(db_coincidencia,Fecha_inicio,Fecha_final, diario)
                except:
                    hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
                    msje_error = Desc_error()
                    error_xlsx="error generación de excel"
                    l_sm.append([error_xlsx,"0",msje_error,hoy1])
                    proceso_dm.registra_alerta_proc(error_xlsx,0,msje_error[2],hoy1)
                    pass
            except:
                hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
                msje_error = Desc_error()
                error_sm="error envio semanal mensual"
                l_sm.append([error_sm,"0",msje_error,hoy1])
                proceso_dm.registra_alerta_proc(error_sm,0,msje_error[2],hoy1)
                pass
    elif hoy.weekday()==0:
        Fecha_inicio=hoy-delta_semanal
        Fecha_final=hoy-timedelta(days=1)
        if len(db_semanal)==0:
             l_a.append(["Sin clientes para el envío semanal", "--", "--", "--"])
        else:
            try:
                envio_automatico(db_semanal, Fecha_inicio, Fecha_final, diario,query_fondos_rd,cnxn5,cnxn2,cnxn3)
                try:
                    ExcelCartola(db_semanal,Fecha_inicio,Fecha_final, diario)
                except:
                    hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
                    msje_error = Desc_error()
                    error_xlsx="error generación de excel"
                    l_sm.append([error_xlsx,"0",msje_error,hoy1])
                    proceso_dm.registra_alerta_proc(error_xlsx,0,msje_error[2],hoy1)
                    pass
            except:
                hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
                msje_error = Desc_error()
                error_sm="error enviosemanal"
                l_sm.append([error_sm, "0", msje_error,hoy1])
                proceso_dm.registra_alerta_proc(error_sm,0,msje_error[2],hoy1)
                pass
    elif hoy.strftime("%d")=="01" and hoy.weekday()!= (5,6):
        Fecha_inicio=primer_dia_mespasado
        Fecha_final=ultimo_diamp
        try:
            envio_automatico(db_coincidencia, Fecha_inicio, Fecha_final, diario,query_fondos_rd,cnxn5,cnxn2,cnxn3)
            try:
                ExcelCartola(db_coincidencia,Fecha_inicio,Fecha_final, diario)
            except:
                hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
                msje_error = Desc_error()
                error_xlsx="error generación de excel"
                l_sm.append([error_xlsx,"0",msje_error,hoy1])
                proceso_dm.registra_alerta_proc(error_xlsx,0,msje_error[2],hoy1)
                pass
        except:
            hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
            msje_error = Desc_error()
            error_sm="error envíomensual"
            l_sm.append([error_sm, "0", msje_error, hoy1])
            proceso_dm.registra_alerta_proc(error_sm,0,msje_error[2],hoy1)
            pass
            
    elif hoy.strftime("%d")=="02" and hoy.weekday()!= (5,6):
        Fecha_inicio=primer_dia_mespasado
        Fecha_final=ultimo_diamp
        try:
            envio_automatico(db_coincidencia, Fecha_inicio, Fecha_final, diario,query_fondos_rd,cnxn5,cnxn2,cnxn3)
            try:
                ExcelCartola(db_coincidencia,Fecha_inicio,Fecha_final, diario)
            except:
                hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
                msje_error = Desc_error()
                error_xlsx="error generación de excel"
                l_sm.append([error_xlsx,"0",msje_error,hoy1])
                proceso_dm.registra_alerta_proc(error_xlsx,0,msje_error[2],hoy1)
                pass
        except:
            hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
            msje_error = Desc_error()
            error_sm="error envíomensual"
            l_sm.append([error_sm, "0", msje_error, hoy1])
            proceso_dm.registra_alerta_proc(error_sm,0,msje_error[2],hoy1)
            pass
        
    elif hoy.strftime("%d")=="03" and hoy.weekday()!= (5,6):
        Fecha_inicio=primer_dia_mespasado
        Fecha_final=ultimo_diamp
        try:
            envio_automatico(db_coincidencia, Fecha_inicio, Fecha_final,diario,query_fondos_rd,cnxn5,cnxn2,cnxn3)
            try:
                ExcelCartola(db_coincidencia,Fecha_inicio,Fecha_final, diario)
            except:
                hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
                msje_error = Desc_error()
                error_xlsx="error generación de excel"
                l_sm.append([error_xlsx,"0",msje_error,hoy1])
                proceso_dm.registra_alerta_proc(error_xlsx,0,msje_error[2],hoy1)
                pass
        except:
            hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
            msje_error = Desc_error()
            error_sm="error envíomensual"
            l_sm.append([error_sm,"0", msje_error,hoy1])
            proceso_dm.registra_alerta_proc(error_sm,0,msje_error[2],hoy1)
            pass     

def envio_diario():
    """
    Esta funcion genera cartolas diariamente con períodos de 1 día hábil, utilizando la función de
    envío automático
    """ 
    if not os.path.isdir(ruta_guardado_pdf + datetime.strftime(tiempo_inicio - timedelta(days=1), "\\%Y-%m-%d")):
        os.mkdir(ruta_guardado_pdf + datetime.strftime(tiempo_inicio - timedelta(days=1), "\\%Y-%m-%d"))
    diario=1
    if hoy.weekday()!=(5,6):
        Fecha_inicio=last_weekday(hoy)
        Fecha_final=hoy-timedelta(days=1)
        if len(db_diario)==0:
           l_d.append(["Sin clientes para el envío diario", "--", "--", "--"])
        else:
            try:
                envio_automatico(db_diario, Fecha_inicio, Fecha_final,diario,query_fondos_rd,cnxn5,cnxn2,cnxn3)
                try:
                    ExcelCartola(db_diario,Fecha_inicio,Fecha_final, diario,cnxn3)
                except:
                    hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
                    msje_error = Desc_error()
                    error_xlsx="error generación de excel"
                    l_d.append([error_xlsx,"0",msje_error,hoy1])
                    proceso_dm.registra_alerta_proc(error_xlsx,0,msje_error[2],hoy1)
                    pass
            except:
                hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
                Fecha_inicio = datetime.strftime(Fecha_inicio,"%Y-%m-%d")
                Fecha_final = datetime.strftime(Fecha_final,"%Y-%m-%d")
                msje_error = Desc_error()
                error_d="error envíoautodiario"
                l_d.append([error_d,"0",msje_error,hoy1])
 
                proceso_dm.registra_alerta_proc(error_d,0,msje_error[2],hoy1)

                pass


def envio_sftp(usuario, clave, save_name, ruta_destino, servidor_destino, pdf_name, diario ):
    """
    Se define esta función para enviar las cartolas generadas mediante el protocolo sftp a una dirección indicada
    """
    dir_ser = servidor_destino
    dir_usser = usuario
    dir_pass = clave
    dir_ruta_dest =  ruta_destino
    dir_bkp_local = save_name
    # CLIENTE SSH #
    SFTP_cliente = paramiko.SSHClient()
    SFTP_cliente.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # CONEXION A SFTP #
    SFTP_cliente.connect(dir_ser, 22, dir_usser, dir_pass)
    SFTP = SFTP_cliente.open_sftp()
    SFTP.chdir(dir_ruta_dest)
    # RUTA DE DESTINO DE ARCHIVOS #
    if diario==0:
        try:
            SFTP.mkdir(datetime.strftime(tiempo_inicio-timedelta(weeks=1), "%Y-%m"))
            dir_ruta_dest2 =  datetime.strftime(tiempo_inicio-timedelta(weeks=1), "%Y-%m")
        except:
            dir_ruta_dest2 = datetime.strftime(tiempo_inicio-timedelta(weeks=1), "%Y-%m")
            pass
       
    if diario==1:
        try:
            SFTP.mkdir(datetime.strftime(last_weekday(tiempo_inicio), "%Y-%m-%d"))
            dir_ruta_dest2 = datetime.strftime(last_weekday(tiempo_inicio), "%Y-%m-%d")
        except:
            dir_ruta_dest2 = datetime.strftime(last_weekday(tiempo_inicio), "%Y-%m-%d")
            pass 
       
    SFTP.chdir(dir_ruta_dest2)
    SFTP.put(dir_bkp_local, pdf_name) # CARGA ARCHIVO #
    SFTP.close()
    SFTP_cliente.close()



if (__name__ == '__main__'):
    
    proceso_dm = DM_proc.proceso_datamart(20, 'prclwsqlagf01') #prclwsqlagf01
    tiempo_inicio=datetime.today()#Variable auxiliar para medir el tiempo que se demora en ejecutarse el código
    server = "alaska" #alaska 
    server2 = "prclwsqlagf01" #prclwsqlagf01
    server3 = "prclwsqlagf01"
    #str_conexion_siga = 'DRIVER={SQL Server};SERVER=prclwsqlagf01;DATABASE=BDFM;Trusted_conexion=True'
    str_conexion_siga = 'DRIVER={SQL Server};SERVER='+server+';DATABASE=BDFM;Trusted_conexion=True'
    str_conexion_dabus = 'DRIVER={SQL Server};SERVER='+server3+';DATABASE=GOLF_ANALISIS;Trusted_conexion=True'
    str_conexion_prclwsqlagf01 = 'DRIVER={SQL Server};SERVER='+server2+';DATABASE=GOLF_ANALISIS;Trusted_conexion=True'
    str_conexion_prclwsqlagf01_BDFM = 'DRIVER={SQL Server};SERVER='+server+';DATABASE=BDFM;Trusted_conexion=True'
    cnxn = pyodbc.connect(str_conexion_siga)
    cnxn2 = pyodbc.connect(str_conexion_dabus)
    cnxn3 = pyodbc.connect(str_conexion_prclwsqlagf01)
    cnxn5 = pyodbc.connect(str_conexion_prclwsqlagf01_BDFM)
    query_fondos_rd = "SET NOCOUNT ON " 
    query_fondos_rd2 = "SET NOCOUNT ON "
    Query_cart = ""
    Query_clientes = ""
    Query_clientes2 = ""
    Query_saldo = ""
    Query_limp = ""
    query_vc= ""
    ruta_guardado_pdf= proceso_dm.get_carpeta_bkp_orden_0()  #"Z:\\Lucas Rencoret\\Cartolas Clientes AGF"
    ruta_imagen=r"C:\Program Files\LarrainVial\Python_venvs\Cartolas_ClientesDirectos_LVAM\Projects\Cartolas_clientes_directos\Foto.png"
    #ruta_imagen="Z:\\Lucas Rencoret\\Cartolas Clientes AGF\Foto.png"
    cd=clientes_directos(cnxn2,query_fondos_rd) #acá se carga la bbdd de clientes directos
    dbea = clientes_directos(cnxn2,query_fondos_rd)
    dbea = dbea[dbea["ind_vigente"] == 1] #Acá se carga la base de clientes directos vigentes en LVAM
    """
    A continuación se definen las bbdd correspondientes a los envíos automáticos, donde se separan según
    envío diario, semanal, mensual y se define la bbdd coincidencia, que se utiliza para realizar un envío
    mensual a todos los clientes, cuando coincide un día lunes con el primer día hábil del mes
    """
    db_diario=dbea[dbea["envio_automatico"] == 1]
    db_semanal=dbea[dbea["envio_automatico"] == 2]
    db_mensual=dbea[dbea["envio_automatico"] == 3]
    db_coincidencia=dbea[dbea["envio_automatico"] != 0]
    l_a=[] #Lista para guadrar errores
    l_sm=[] #Lista para guadrar errores
    l_d=[] #Lista para guadrar errores
    l=[]
    """
    A cotinuación se definen variables temporales que se utilizan para definir cuándo se realiza un envío semanal,
    mensual o diario
    """      
    hoy=datetime.today()
    año=hoy.year
    mes_pasado=hoy.month-1
    mes_actual=mes_pasado+1
    primer_dia_mespasado="%s-%s-01" % (año, mes_pasado)
    primer_dia_mesactual="%s-%s-01" % (año, mes_actual)
    primer_dia_mespasado=datetime.strptime(primer_dia_mespasado , "%Y-%m-%d")
    primer_dia_mesactual=datetime.strptime(primer_dia_mesactual , "%Y-%m-%d")
    ultimo_diamp=primer_dia_mesactual - timedelta(days=1)
    delta_semanal=timedelta(days=7)
    tiempo_inicio=datetime.today()#Variable auxiliar para medir el tiempo que se demora en ejecutarse el código
    try:
        envio_diario()
    except:
        hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
        msje_error = Desc_error()
        error_d="error_funcion_semanalmensual"
        l.append(["error_funcion","-","-",msje_error])
        proceso_dm.registra_alerta_proc(error_d,0,msje_error[2],hoy1)
        pass
    try:
        envio_semanal_mensual()
    except:
        hoy1 = datetime.strftime(datetime.today(),"%Y-%m-%d")
        msje_error = Desc_error()
        error_d="error_funcion_diaria"
        l.append(["error_funcion","-","-",msje_error])
        proceso_dm.registra_alerta_proc(error_d,0,msje_error[2],hoy1)
        pass

    lista_errores =l_sm+l_d+l_a+l+l_d
    dberrores=pd.DataFrame(lista_errores)
    fec=datetime.strftime(tiempo_inicio, "%Y%m%d%HH%SS")
    if not os.path.isdir(ruta_guardado_pdf + datetime.strftime(tiempo_inicio - timedelta(days=1), "\\%Y-%m-%d")):
        os.mkdir(ruta_guardado_pdf + datetime.strftime(tiempo_inicio - timedelta(days=1), "\\%Y-%m-%d"))
    diario=1
    #dberrores.to_csv(ruta_guardado_pdf + datetime.strftime(last_weekday(tiempo_inicio), "\\%Y-%m-%d") + "/dberrores" + fec + ".txt" , header=None, index=None, sep=' ', mode='a')  
    fin=datetime.today() #Variable auxiliar para medir el tiempo que se demora en ejecutarse el código
    inicio=tiempo_inicio
    ID=1
    instance_id=""
    id_proceso=1106
    estatus="Procesado"
    cant_error=len(dberrores)
    cant_procesados=len(dbea)
    fecha_ini=""
    fecha_fin=""
    l_dblog=[ID, instance_id, inicio, fin, fecha_ini, fecha_fin, estatus, cant_error ]
    #print(str(fin - tiempo_inicio) +" fue el tiempo de ejecución")
    proceso_dm.fin_proceso(cantidad_procesados = cant_procesados, cantidad_error = cant_error)

