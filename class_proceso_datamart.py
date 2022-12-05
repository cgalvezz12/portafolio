import pyodbc
import class_proceso_notificaciones as notifications
from datetime import date

class proceso_datamart():
    
    
    def __init__(self, id_proceso, servidor = None):
        self.id_proceso = id_proceso
         
        if(servidor is None): servidor = "Dabus"
        
        self.str_conexion_configuracion = 'DRIVER={SQL Server};SERVER=' + servidor + ';DATABASE=DM_Configuracion;Trusted_conexion=True'
        try:
            self.cnxn_configuracion = pyodbc.connect(self.str_conexion_configuracion)            
            self.cursor_configuracion = self.cnxn_configuracion.cursor()
        except:
            pass
        
        #iniciar proceso
        parametros_inicio_proceso = (self.id_proceso)
        query_inicio_proceso = "" \
            " DECLARE @myid uniqueidentifier = CONVERT(nvarchar(50), NEWID())  \n" \
            " exec [dbo].[sp_regista_log_proceso]                              \n" \
            " @myid, /*instanceid*/                                            \n" \
            " ?, /*id_etl*/                                                    \n" \
            " '', /*info_error*/                                               \n" \
            " 0, /*cant_procesado*/                                            \n" \
            " 0 /*cant_error*/                                                 \n" \
            " select @myid instanceid  "            
        
        self.cursor_configuracion.execute(query_inicio_proceso, parametros_inicio_proceso) # ejecucion de inicio de proceso
        filas = self.cursor_configuracion.fetchone()
        self.instance_id = filas[0] # instance id de la ejecucion
        self.cursor_configuracion.commit()
                
        self.n_veces_fin_proc = 0 #numero de veces que se ha llamado el fin_proceso

    
    def __del__(self):
        if(self.n_veces_fin_proc == 0):
            self.fin_proceso(info_error = "cierre proceso por destruccion de objeto proceso_datamart")
            print("fin proceso cerrado en __del__")
        else:
            print("fin proceso ya cerrado")
            pass
    
    def get_instance_id(self):
        return self.instance_id
    
    def ejecutar_notificaciones(self):
        notifications.proceso_notificaciones(self.id_proceso, self.instance_id, self.cnxn_configuracion)

    def fin_proceso(self, info_error = "", cantidad_procesados = 0, cantidad_error = 0):
        self.n_veces_fin_proc += 1
        try:
            #self.ejecutar_notificaciones()
            print("notificacion proceso")
        except Exception as ex_noti:
            fecha_ejecucion = date.today().strftime("%Y-%m-%d")
            self.registra_alerta_proc(titulo_alerta= 'Error enviar notificaciones', ind_alerta=0, mensaje= 'Error ejecucion de notificaciones por python:' + ex_noti, segmentado = fecha_ejecucion)
            
        finally:

            parametros_fin_proceso = (self.instance_id
                                    , self.id_proceso
                                    , info_error
                                    , cantidad_procesados
                                    , cantidad_error)
            
            query_fin_proceso = " exec [dbo].[sp_regista_log_proceso]               \n" \
                " ?, /*instanceid*/                                                 \n" \
                " ?, /*id_etl*/                                                     \n" \
                " ?, /*info_error*/                                                 \n" \
                " ?, /*cant_procesado*/                                             \n" \
                " ? /*cant_error*/                                                  \n"
            
            self.cursor_configuracion.execute(query_fin_proceso, parametros_fin_proceso)
            self.cursor_configuracion.commit()
            
            parametros_prox_ejecucion = (self.instance_id
                                     , self.id_proceso)

            query_prox_ejecucion = "exec [dbo].[sp_actualiza_proxima_ejecucion_Proceso] ? /*instanceid*/,? /*id_etl*/ "

            self.cursor_configuracion.execute(query_prox_ejecucion, parametros_prox_ejecucion)
            self.cursor_configuracion.commit()
        
    def proxima_ejecucion(self):
        parametros_prox_ejecucion = (self.instance_id
                                     , self.id_proceso)
    
        query_prox_ejecucion = "exec [dbo].[sp_actualiza_proxima_ejecucion_Proceso] ? /*instanceid*/,? /*id_etl*/ "
        
        self.cursor_configuracion.execute(query_prox_ejecucion, parametros_prox_ejecucion)        
        self.cursor_configuracion.commit()
        
    def registra_alerta_proc(self, titulo_alerta, ind_alerta, mensaje, segmentado):
        parametros_alerta_proc = (self.instance_id
                                  , titulo_alerta[:50]
                                  , ind_alerta
                                  , mensaje
                                  , segmentado)
        
        query_alerta = "exec dbo.sp_regista_alertas ? /*instanceid*/            \n" \
                        ", ? /*titulo*/                                         \n" \
                        ", ? /*ind_alerta*/                                     \n" \
                        ", ? /*mensaje*/                                        \n" \
                        ", ? /*segmentado*/     "
                        
        self.cursor_configuracion.execute(query_alerta, parametros_alerta_proc)
        self.cursor_configuracion.commit()
        
    def registra_alerta_proc_iter(self, list_alertas):
        for alerta in list_alertas:
            self.registra_alerta_proc(alerta[0], alerta[1], alerta[2], alerta[3])
    
    def get_variables_proceso_basico(self):
        query_var_proceso = " declare @par_id_etl int = ?																																		\n" \
                    "                                                                                                                                                                   \n" \
                    " SELECT gestora                                                                                                                                                    \n" \
                    " ,CASE WHEN fecha_ini>(CASE WHEN fecha_fin>DATEADD(DAY,dia_tope,GETDATE()) THEN DATEADD(DAY,dia_tope,GETDATE()) ELSE fecha_fin END)                                \n" \
                    " 		THEN DATEADD(DAY,dia_reproceso,(CASE WHEN fecha_fin>DATEADD(DAY,dia_tope,GETDATE()) THEN DATEADD(DAY,dia_tope,GETDATE()) ELSE fecha_fin END))               \n" \
                    " 		ELSE fecha_ini END fecha_ini                                                                                                                                \n" \
                    " ,CASE WHEN fecha_fin>DATEADD(DAY,dia_tope,GETDATE()) THEN DATEADD(DAY,dia_tope,GETDATE()) ELSE fecha_fin END fecha_fin                                            \n" \
                    " ,CASE WHEN GETDATE()>ISNULL(fecha_inactivo,GETDATE()) THEN 0 ELSE 1 END ind_inactivo                                                                              \n" \
                    " ,ind_feriados                                                                                                                                                     \n" \
                    " ,isnull(pais_feriado, 'CHILE')                                                                                                                                    \n" \
                    " FROM dbo.DM_Catalogo_Proceso                                                                                                                                      \n" \
                    " WHERE id=@par_id_etl 	"
    
        self.cursor_configuracion.execute(query_var_proceso, (self.id_proceso))
        fila = self.cursor_configuracion.fetchone()
        self.cursor_configuracion.commit()
        
        # asignacion variables de ejecucion
        self.gestora_proc = fila[0]
        self.fecha_ini_proc = fila[1]
        self.fecha_fin_proc = fila[2]
        self.ind_inactivo_proc = fila[3]
        self.ind_feriados_proc = fila[4]
        self.pais_feriados_proc = fila[5]
        
        return self.gestora_proc, self.fecha_ini_proc, self.fecha_fin_proc \
            , self.ind_inactivo_proc, self.ind_feriados_proc, self.pais_feriados_proc
        
        
    def get_carpeta_bkp_orden_0(self):
        query_var_ruta_bkp = "SELECT ruta_backup + dir_backup                               \n" \
                        " FROM [DM_CONFIGURACION].[dbo].[DM_Configuracion_Proceso]          \n" \
                        " where id_proceso = ?                                              \n" \
                        " and orden = 0     "
        
        self.cursor_configuracion.execute(query_var_ruta_bkp, (self.id_proceso))
        fila = self.cursor_configuracion.fetchone()
        
        ruta_bkp_orden_0 = fila[0]
        
        return ruta_bkp_orden_0
        
        
        
        