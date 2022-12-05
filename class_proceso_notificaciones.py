import os
import csv
import pandas as pd
import smtplib
from email.message import EmailMessage

class proceso_notificaciones():
    # iniciacion de objeto proceso notificaciones
    def __init__(self, id_etl, instance_id, cnxn):
        self.cnxn = cnxn
        self.cursor_noti = cnxn.cursor()
        self.instance_id = instance_id

        # query notificaciones desde etl notificaciones
        query_var_notificaciones = f""" select isnull(noti.asunto,'')asunto
                                            ,isnull(destinatario,'')destinatario
                                            ,isnull(notproc.emisor,'')emisor
                                            ,isnull(notproc.cc,'')cc
                                            ,isnull(notproc.cco,'')cco
                                            ,isnull(notproc.query_contenido,'')query_contenido
                                            ,isnull(noti.pie_pagina,'')pie_pagina
                                            ,isnull(notproc.mensaje_contenido,'')mensaje_contenido
                                            ,notproc.orden
                                            ,notproc.ID
                                    ,isnull(noti.smtp,'') smtp
                                    from DM_Notificacion noti 
                                        left join DM_Notificacion_Proceso notproc on noti.ID=notproc.id_notificacion
                                    where notproc.ind_inactivo=0
                                            and notproc.id_proceso={id_etl}
                                    order by orden asc """
        
        # dataframe con notificaciones
        self.df_var_notificaciones = pd.read_sql(query_var_notificaciones, self.cnxn)
        self.df_var_notificaciones.sort_values('orden', ascending = True, inplace = True)
        
        # recorrer notificaciones
        self.recorrer_notificaciones()

    def obtener_adjuntos(self, id_notificacion_proceso):
        # funcion que crea adjuntos de un correo en caso de que la ejecucion los tenga
        # retorna un listado con las ubicaciones de esos archivos adjuntos
        query_adjuntos_notificacion = f"""SELECT isnull(ruta_archivo,'') AS ruta_archivo
                                                        ,convert(int,isnull(ind_generacion,0)) AS ind_generacion
                                                        ,isnull(encabezado,'') AS encabezado
                                                        ,isnull(query_adjunto,'') AS query_adjunto
                                                        ,isnull(nombre_archivo,'') AS nombre_arch_adjunto
                                                        ,isnull(adjunto_ya_creado,'') AS adjunto_ya_creado
                                                FROM dbo.DM_Adjunto
                                                WHERE id_notificacion_proceso={id_notificacion_proceso} """
        
        # obtener adjuntos desde bbdd
        self.cursor_noti.execute(query_adjuntos_notificacion)
        var_adjuntos_notificacion = self.cursor_noti.fetchall()
        self.cursor_noti.commit()

        list_adjuntos_notificacion = []

        # recorrer listado de adjuntos
        for fila in var_adjuntos_notificacion:
            n_ruta_archivo = fila[0]
            n_ind_generacion= fila[1]
            n_encabezado = fila[2]
            n_query_adjunto = fila[3]
            n_nombre_arch_adjunto = fila[4]
            n_adjunto_ya_creado = fila[5]
            
            if(n_adjunto_ya_creado != ''): list_adjuntos_notificacion.append(n_adjunto_ya_creado)

            if(n_ruta_archivo != '' and n_ind_generacion != 0 and n_query_adjunto != ''):
                n_query_adjunto = n_query_adjunto.replace('?', f"'{self.instance_id}'")
                #self.cursor_noti.execute(n_query_adjunto, self.instance_id)
                #var_data_tocsv = self.cursor_noti.fetch_all()
                df_adjuntos = pd.read_sql(n_query_adjunto, self.cnxn)

                # listado de distintos valores segmentado desde 
                list_segmentados = df_adjuntos['segmentado'].sort_values().unique().tolist()

                for segmentado in list_segmentados:
                    # nombre de archivo con segmentado
                    archivo_nombre_completo = n_ruta_archivo +  n_nombre_arch_adjunto.split('.')[0] + str(segmentado) + '.' + n_nombre_arch_adjunto.split('.')[1]

                    # reemplazar nombre de columna del dataframe con el encabezado del csv
                    df_adjuntos.rename({'mensaje':n_encabezado}, axis = 1, inplace = True)

                    # creacion de adjuntos csv
                    df_adjuntos[df_adjuntos['segmentado']==segmentado][n_encabezado].to_csv(archivo_nombre_completo, index = False, quoting = csv.QUOTE_NONE, quotechar = '', sep = "\t" , header = True)

                    # agregar ruta de adjunto a lista a retornar
                    list_adjuntos_notificacion.append(archivo_nombre_completo)

        
        return list_adjuntos_notificacion
            
    def obtener_mensaje_contenido(self, q_contenido, m_mensaje, m_pie , len_adjuntos):
        # funcion que determina el contenido del correo
        # retorna string con html del correo
        self.cursor_noti.execute(q_contenido, self.instance_id)
        contenido = self.cursor_noti.fetchone()
        self.cursor_noti.commit()

        m_base_estructura = ""

        if(contenido[0] != '' or len_adjuntos > 0):
            m_contenido = contenido[0]

            m_base_estructura = "<html><head><meta http-equiv='Content-Type' content='text/html; charset=utf-8'></head>"
            m_base_estructura = m_base_estructura + "<body style='margin: 0; padding: 0;'>"
            m_base_estructura = m_base_estructura + "<table align='center' border='1' cellpadding='0' cellspacing='0' width='90%'>"
            m_base_estructura = m_base_estructura + "<tr>"
            m_base_estructura = m_base_estructura + "<td bgcolor='#ffffff' style='padding: 40px 30px 40px 30px;font-size:12'>"
            m_base_estructura = m_base_estructura + "<table border='0' cellpadding='0' cellspacing='0' width='100%' style=''font-size:12''>"
            m_base_estructura = m_base_estructura + "<tr>"
            m_base_estructura = m_base_estructura + "<td style='padding: auto;font-size:15'>" + m_mensaje + " </td> "
            m_base_estructura = m_base_estructura + "</tr>"
            m_base_estructura = m_base_estructura + "<tr>"
            m_base_estructura = m_base_estructura + "<td style='padding: auto;font-size:12'><br></td>"
            m_base_estructura = m_base_estructura + "</tr>"
            m_base_estructura = m_base_estructura + "<tr>"
            m_base_estructura = m_base_estructura + "<td style='padding: auto;font-size:12;text-align:center'>" + m_contenido + "</td> "
            m_base_estructura = m_base_estructura + "</tr>"
            m_base_estructura = m_base_estructura + "</table>"
            m_base_estructura = m_base_estructura + "</td>"
            m_base_estructura = m_base_estructura + "</tr>"
            m_base_estructura = m_base_estructura + "<tr style='font-size:10'>"
            m_base_estructura = m_base_estructura + "<td bgcolor='#ee4c50' align='center' style='font-size:10'>" + m_pie + "</td>"
            m_base_estructura = m_base_estructura + "</tr>"
            m_base_estructura = m_base_estructura + "</table>"
            m_base_estructura = m_base_estructura + "</body>"
            m_base_estructura = m_base_estructura + "</html>"

        return m_base_estructura

    def recorrer_notificaciones(self):
        # funcion que recorre las notificaciones
        # por cada notificacion, si se cumplen las condiciones, se envia un correo
        for fila in self.df_var_notificaciones.itertuples():   
            # obtener listado de adjuntos de la notificacion     
            lista_adjuntos_notificacion = self.obtener_adjuntos(fila.ID)
            
            # cantidad de adjuntos que tendra el correo
            cant_adjuntos = len(lista_adjuntos_notificacion)

            # estructura y mensaje del correo en html
            estructura_mensaje = self.obtener_mensaje_contenido(fila.query_contenido, fila.mensaje_contenido, fila.pie_pagina, cant_adjuntos)

            if(estructura_mensaje != "" or cant_adjuntos > 0):
                # objeto mensaje
                notif = EmailMessage()
                notif['From'] = fila.emisor
                notif['To'] = fila.destinatario
                notif['Subject'] = fila.asunto
                notif['Cc'] = fila.cc
                notif['Bcc'] = fila.cco

                notif.add_alternative(estructura_mensaje, subtype = 'html')

                # agregar adjuntos
                if(cant_adjuntos > 0):
                    for archivo_adjuntar in lista_adjuntos_notificacion:
                        with open(archivo_adjuntar, 'rb') as f:
                            arch_ = f.read()
                            arch_nombre_ = os.path.basename(f.name)
                            notif.add_attachment(arch_, maintype = 'Application', subtype = 'octet-stream', filename = arch_nombre_)
                
                # enviar correo
                try:
                    with(smtplib.SMTP(fila.smtp)) as smtp_obj:
                        smtp_obj.send_message(notif)
                
                except Exception as ex:
                    print('ex: ', ex)

                # eliminar adjuntos creados
                if(cant_adjuntos > 0):
                    for archivo_adjuntar in lista_adjuntos_notificacion:
                        if(os.path.exists(archivo_adjuntar)):
                            os.remove(archivo_adjuntar)
                