#!/usr/bin/env python
# coding: utf-8
#from os import posix_fallocate
#import pandas as pd

import requests
import json
import pyodbc
from datetime import datetime, timedelta
import class_proceso_datamart
import sys
import traceback

# login: str, str -> str, str
# Logs into PuertoX's API
def login(username, password, environment='serverless_prodbpc', domain='serverless', customer_domain='prodbpc', api_id='login', url='https://apiprod.bpc.cl'):

    input_json = {

        'environment':environment,

        'domain':domain,

        'customer_domain':customer_domain,

        'api_id':api_id,

        'user_id':username,

        'password':password

    }

    try:

        response = requests.post(url, json = input_json)

        

        if response.status_code == 200:

            content = response.content.decode('utf-8')

            data = json.loads(content)

            res_code = data['resp_code']

            if res_code == 0:

                value = data['cookie']['session_id']

                return res_code, value

            elif res_code == 1:

                value =  "Sesión Expirada"

                return res_code, value

            elif res_code == 2:

                value = "Login Incorrecto"

                return res_code, value

            elif res_code == 3:

                value = "Faltan Parámetros de entrada"

                return res_code, value

            elif res_code == 4:

                value = "Token de sesión inválido"

                return res_code, value

            else:

                value = "Otro error de la API" 

                return res_code, value

        else:

            value = "Error de conexion. Respuesta status code: " + str(response.status_code) + '. Respuesta mensaje:' + response.text

            return -1, value



    except requests.exceptions.RequestException:

        value = "Error de conexión a API PuertoX"

        return -1, value



    except Exception as ex1:

        value = "Error: " + ex1

        return -1, value



    



# request_data: str, str, str(date), str(date) -> str(json)

# Make a POST request to PuertoX's API. It retrieves the 'movimientos' of 'facturas' between dates

def request_data(session_id, date_1, date_2, api_id='suitebpc_getMovimientosCustodia', environment='serverless_prodbpc', domain='serverless', customer_domain='prodbpc', url='https://apiprod.bpc.cl'):

    input_json= {

        'environment': environment, 

        'domain':domain,

        'customer_domain':customer_domain,

        'api_id':api_id,

        'session_id':session_id,

        'fecha_desde':date_1,

        'fecha_hasta':date_2

    }

    response = requests.post(url, json = input_json)

    if response.status_code == 200:

        content = response.content.decode('utf-8')

        return content  

    else:

        return ""





# get_partial_payment: list -> number

# Transform abono data from API according to data modeling

def get_partial_payment(ppayment_list):

    if ppayment_list != None:

        # Another way to get ppayment

        #partial_payment = 0

        #for ppayment in ppayment_list:

        #    partial_payment += ppayment['monto']

        last_data = ppayment_list[-1]

        partial_payment = last_data['monto']

        return partial_payment

    else:

        return 0





# set_date: str -> str

# Set date by default in case of missing data

def set_date(date):

    if date == '':

        return None

    else:

        return date

        



# format_data: dict -> list

# Format API data to insert in SQL database

def format_data(dict_data):

    data = []

    

    l_cedible = lambda x: 1 if(x == 'No') else 0

    l_tipo_documento = lambda x: 'Factura' if(x=='33') else 'No factura'

    l_set_date = lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S') if(x != "") else None



    data.append(datetime.strptime(dict_data['fecha_ingreso'], '%Y-%m-%d %H:%M:%S'))

    data.append(datetime.strptime(dict_data['fecha_movimiento'], '%Y-%m-%d %H:%M:%S'))

    data.append(11843)

    data.append(int(dict_data['folio']))

    data.append(datetime.strptime(dict_data['fecha_emision'], '%Y-%m-%d'))

    data.append(datetime.strptime(dict_data['fecha_cesion'], '%Y-%m-%d'))

    data.append(datetime.strptime(dict_data['fecha_vencimiento_nominal'], '%Y-%m-%d'))

    data.append(int(dict_data['emisor']))

    data.append(l_tipo_documento(dict_data['tipo_documento'])) 

    data.append(dict_data['rut_pagador'])

    data.append(dict_data['razon_social_pagador'])

    data.append(dict_data['nombre_sector_pagador']) #

    data.append(dict_data['rut_vendedor'])

    data.append(dict_data['razon_social_vendedor'])

    data.append(dict_data['estado'])

    data.append(l_cedible(dict_data['no_cedible']))

    data.append(dict_data['rut_cesionario'])

    data.append(dict_data['razon_social_cesionario'])

    data.append(datetime.strptime(dict_data['fecha_vencimiento_puertox'], '%Y-%m-%d'))

    data.append(datetime.strptime(dict_data['fecha_pago_tenedor'], '%Y-%m-%d'))

    data.append(l_set_date(dict_data['fecha_salida_custodia']))

    data.append(float(dict_data['monto_factura']))

    data.append(int(dict_data['mora'])) 

    data.append(float(dict_data['tasa_referencial']))

    data.append(dict_data['moneda'])

    data.append(float(get_partial_payment(dict_data['abonos'])))

    data.append(int(dict_data['colateral']))

    data.append('API PuertoX')

    return data



def insert_data(data, cnxn):

    cursor = cnxn.cursor()

    query = "INSERT INTO dbo.[Ext_MovimientosFacturas] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

    f_params = format_data(data)

    cursor.execute(query, f_params)

    cursor.commit()



def delete_movs_dia(fecha_ini, fecha_fin, cnxn):

    cursor = cnxn.cursor()

    query_del = f"delete from dbo.Ext_MovimientosFacturas where cast(fecha_movimiento as date) between '{fecha_ini}' and '{fecha_fin}' and fuente = 'API PuertoX'; "

    cursor.execute(query_del)

    cursor.commit()



def Desc_error():

    ex_type, ex_value, ex_traceback = sys.exc_info()

    trace_back = traceback.extract_tb(ex_traceback)

    stack_trace = []

    for trace in trace_back:

        stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))

    Msje_error = 'Exception Type: {}'.format(str(ex_type.__name__)) + \

         ' Exception message: {} '.format(str(ex_value)) + \

         ' Trace back: {}'.format(str(stack_trace))

    return Msje_error







if(__name__ == '__main__'):

    # inicio del proceso

    id_proceso = 43 

    server = 'PRCLWSQLAGF01'

    info_error_proceso = ""

    segmentado = datetime.strftime(datetime.now(),"%Y-%m-%d")

    cant_data_insertar = 0

    cant_data_nook = 0



    # proceso datamart

    proceso = class_proceso_datamart.proceso_datamart(id_proceso, server)

    # variables del proceso datamart:

    p_gestora, p_fecha_ini, p_fecha_fin, p_ind_inactivo, p_ind_feriados, p_pais_feriados = proceso.get_variables_proceso_basico()





    # parametros de entrada a API quedaran en duro??

    username='larrainvial_fip_fac_nacionales_prod'

    password='3oFwC4Tc8k6S3U8a'



    if(p_ind_inactivo == 0):



        try:

            cod_res_api, session_api = login(username, password)



            if(cod_res_api == 0):

                

                if(p_fecha_ini < datetime.today() or p_fecha_fin < datetime.today()):

                    p_fecha_ini = datetime.today()

                    p_fecha_fin = datetime.today()



                # fecha inicio y fin para consultar en el API

                p_fecha_ini_str = datetime.strftime(p_fecha_ini, "%Y-%m-%d 00:00")

                p_fecha_fin_str = datetime.strftime(p_fecha_fin, "%Y-%m-%d 23:59")



                # solicitar movimientos a API de PuertoX

                raw_data = request_data(session_api, p_fecha_ini_str, p_fecha_fin_str)



                if raw_data != "":

                    json_data = json.loads(raw_data)

                    try:

                        # revisar movimientos de facturas

                        data = json_data['facturas']

                        cant_data_insertar = len(data)



                        connection = pyodbc.connect("DRIVER={SQL Server};SERVER="+ server +";DATABASE=Golf_analisis;trusted_connection=yes")

                        #borrar movimientos del dia

                        delete_movs_dia(fecha_ini = datetime.strftime(p_fecha_ini, "%Y-%m-%d"), fecha_fin = datetime.strftime(p_fecha_fin, "%Y-%m-%d"), cnxn = connection)



                        for f in data:                 

                            try:

                                insert_data(f, connection)

                            except Exception as ex_insert_data:

                                cant_data_nook += 1

                                mensaje_alerta = ';'.join(format_data(f)) + ';' + str(ex_insert_data)



                                proceso.registra_alerta_proc(titulo_alerta = 'Error insert movimiento factura'

                                                            , ind_alerta = 1

                                                            , mensaje = mensaje_alerta

                                                            , segmentado = segmentado)



                        if(cant_data_insertar != 0):

                            crsr = connection.cursor()

                            if(p_fecha_ini !=p_fecha_fin):

                                l_range_fechas = [p_fecha_ini + timedelta(days = x) for x in range((p_fecha_fin -p_fecha_ini).days)]

                                for sp_input_fecha in l_range_fechas:

                                    params_sp_cartera = (datetime.strftime(sp_input_fecha, "%Y-%m-%d"), id_proceso, proceso.get_instance_id())

                                    crsr.execute('Exec dbo.spCarga_DM_CarteraFacturas ?, ?, ?',(params_sp_cartera))

                                    crsr.commit()

                            else:

                                params_sp_cartera = (datetime.strftime(p_fecha_ini, "%Y-%m-%d"), id_proceso, proceso.get_instance_id())

                                crsr.execute('Exec dbo.spCarga_DM_CarteraFacturas ?, ?, ?',(params_sp_cartera))

                                crsr.commit()

                            crsr.close()

                    # se deberia manejar un error mas general

                    except KeyError as except_key:

                        print("No data to populate")

                        proceso.registra_alerta_proc(titulo_alerta = 'No existe llave en data descargada desde API'

                                                    , ind_alerta = 0

                                                    , mensaje = 'No existe llave "facturas" en data descargada desde API: ' + Desc_error()

                                                    , segmentado = segmentado)



                    except Exception as ex_carga_data:

                        # mensaje_alerta = 'Error leer data descargada de API de PuertoX: ' + ex_carga_data

                        proceso.registra_alerta_proc(titulo_alerta = 'Error leer data de API'

                                                    , ind_alerta = 1

                                                    , mensaje = 'Error al leer datos desde API: ' + p_fecha_ini_str + " - " + p_fecha_fin_str + ". Error: " + Desc_error()

                                                    , segmentado = segmentado)            



                else:

                    print("No data returned from API")

                    proceso.registra_alerta_proc(titulo_alerta = 'No data obtenida desde API'

                                                , ind_alerta = 1

                                                , mensaje = 'La API de Puerto X no retornó datos. Fechas consultadas (fecha inicio, fecha fin): ' + p_fecha_ini_str + " - " + p_fecha_fin_str

                                                , segmentado = segmentado)



            else:

                print("No se pudo hacer login correctamente")

                proceso.registra_alerta_proc(titulo_alerta = 'Error login API PuertoX'

                                , ind_alerta = 1

                                , mensaje = 'Error al hacer Login al API de PuertoX. Retorno de sesión: ' + session_api

                                , segmentado = segmentado)





        except Exception as ex_error_proceso:

            info_error_proceso = "Error proceso: Revisar log"

            proceso.registra_alerta_proc(titulo_alerta = 'Error proceso'

                                        , ind_alerta = 1

                                        , mensaje = 'Error proceso descarga de movimientos desde API PuertoX: ' + Desc_error()

                                        , segmentado = segmentado)

        

        proceso.proxima_ejecucion()

        # ejecutar fin de proceso

        proceso.fin_proceso(info_error = info_error_proceso, cantidad_procesados = cant_data_insertar, cantidad_error = cant_data_nook)



    else:

        proceso.fin_proceso(info_error = 'Proceso inactivo, se debe desactivar ejecución automática', cantidad_procesados = cant_data_insertar, cantidad_error = cant_data_nook)