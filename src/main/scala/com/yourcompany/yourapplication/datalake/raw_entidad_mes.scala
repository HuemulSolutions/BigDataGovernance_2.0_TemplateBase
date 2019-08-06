package com.yourcompany.yourapplication.datalake
        
import com.yourcompany.yourapplication.globalSettings._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.datalake._
import com.huemulsolutions.bigdata.tables._
import org.apache.spark.sql.types._
import com.huemulsolutions.bigdata.control.huemulType_Frequency._

//ESTE CODIGO FUE GENERADO A PARTIR DEL TEMPLATE DEL SITIO WEB


/**
 * Clase que permite abrir un archivo de texto, devuelve un objeto huemul_dataLake con un DataFrame de los datos
 * ejemplo de nombre: raw_institucion_mes
 */
class raw_entidad_mes(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_DataLake(huemulBigDataGov, Control) with Serializable  {
   this.Description = "Decripción de la interfaz"
   this.GroupName = "[[yourapplication]]"
   this.setFrequency(huemulType_Frequency.MONTHLY)
   
   //Crea variable para configuración de lectura del archivo
   val CurrentSetting = new huemul_DataLakeSetting(huemulBigDataGov)
   //setea la fecha de vigencia de esta configuración
   CurrentSetting.StartDate = huemulBigDataGov.setDateTime(2010,1,1,0,0,0)
   CurrentSetting.EndDate = huemulBigDataGov.setDateTime(2050,12,12,0,0,0)

   //Configuración de rutas globales
   CurrentSetting.GlobalPath = huemulBigDataGov.GlobalSettings.RAW_SmallFiles_Path
   //Configura ruta local, se pueden usar comodines
   CurrentSetting.LocalPath = "[[yourapplication]]/"
   //configura el nombre del archivo (se pueden usar comodines)
   CurrentSetting.FileName = "[[name.TXT]]"
   //especifica el tipo de archivo a leer
   CurrentSetting.FileType = huemulType_FileType.TEXT_FILE
   //expecifica el nombre del contacto del archivo en TI
   CurrentSetting.ContactName = "[[nombre de contacto del origen]]"

   //Indica como se lee el archivo
   CurrentSetting.DataSchemaConf.ColSeparatorType = huemulType_Separator.CHARACTER  //POSITION;CHARACTER
   //separador de columnas
   CurrentSetting.DataSchemaConf.ColSeparator = "\t"    //SET FOR CARACTER
   //forma rápida de configuración de columnas del archivo
   //CurrentSetting.DataSchemaConf.setHeaderColumnsString("institucion_id;institucion_nombre")
   //Forma detallada
   CurrentSetting.DataSchemaConf.AddColumns("ejemplo_producto_id", "pk", IntegerType, "codigo del producto")
   CurrentSetting.DataSchemaConf.AddColumns("fecha_venta", "fecven", StringType, "fecha de la venta")
   CurrentSetting.DataSchemaConf.AddColumns("cantidad", "cant", DecimalType(10,2), "Cantidad del producto")
   CurrentSetting.DataSchemaConf.AddColumns("precio", "prec", DecimalType(10,2), "Precio de la transaccion")
    
   //Seteo de lectura de información de Log (en caso de tener)
   CurrentSetting.LogSchemaConf.ColSeparatorType = huemulType_Separator.CHARACTER  //POSITION;CHARACTER;NONE
   CurrentSetting.LogNumRows_FieldName = null
   CurrentSetting.LogSchemaConf.ColSeparator = ";"    //SET FOR CARACTER
   CurrentSetting.LogSchemaConf.setHeaderColumnsString("VACIO") 
   this.SettingByDate.append(CurrentSetting)
  
    /***
   * open(ano: Int, mes: Int) <br>
   * método que retorna una estructura con un DF de detalle, y registros de control <br>
   * ano: año de los archivos recibidos <br>
   * mes: mes de los archivos recibidos <br>
   * dia: dia de los archivos recibidos <br>
   * Retorna: true si todo está OK, false si tuvo algún problema <br>
  */
  def open(Alias: String, ControlParent: huemul_Control, ano: Integer, mes: Integer, dia: Integer, hora: Integer, min: Integer, seg: Integer): Boolean = {
    //Crea registro de control de procesos
     val control = new huemul_Control(huemulBigDataGov, ControlParent, huemulType_Frequency.ANY_MOMENT)
    //Guarda los parámetros importantes en el control de procesos
    control.AddParamYear("Ano", ano)
    control.AddParamMonth("Mes", mes)
       
    try { 
      //NewStep va registrando los pasos de este proceso, también sirve como documentación del mismo.
      control.NewStep("Abre archivo RDD y devuelve esquemas para transformar a DF")
      if (!this.OpenFile(ano, mes, dia, hora, min, seg, null)){
        //Control también entrega mecanismos de envío de excepciones
        control.RaiseError(s"Error al abrir archivo: ${this.Error.ControlError_Message}")
      }
   
      control.NewStep("Aplicando Filtro")
      //Si el archivo no tiene cabecera, comentar la línea de .filter
      val rowRDD = this.DataRDD
            //filtro para considerar solo las filas que los tres primeros caracteres son numéricos
            .filter { x => x.length()>=4 && huemulBigDataGov.isAllDigits(x.substring(0, 3) )  }
            //filtro para dejar fuera la primera fila
            //.filter { x => x != this.Log.DataFirstRow  }
            .map { x => this.ConvertSchema(x) }
            
      control.NewStep("Transformando datos a dataframe")      
      //Crea DataFrame en Data.DataDF
      this.DF_from_RAW(rowRDD, Alias)
        
      //****VALIDACION DQ*****
      //**********************
      control.NewStep("Valida que cantidad de registros esté entre 10 y 100")    
      //validacion cantidad de filas
      val validanumfilas = this.DataFramehuemul.DQ_NumRowsInterval(this, 10, 100)      
      if (validanumfilas.isError) control.RaiseError(s"user: Numero de Filas fuera del rango. ${validanumfilas.Description}")
                        
      control.FinishProcessOK                      
    } catch {
      case e: Exception => {
        control.Control_Error.GetError(e, this.getClass.getName, null)
        control.FinishProcessError()   
      }
    }         
    return control.Control_Error.IsOK()
  }
}


/**
 * Este objeto se utiliza solo para probar la lectura del archivo RAW
 * La clase que está definida más abajo se utiliza para la lectura.
 */
object raw_entidad_mes_test {
   /**
   * El proceso main es invocado cuando se ejecuta este código
   * Permite probar la configuración del archivo RAW
   */
  
  def main(args : Array[String]) {
    
    //Creación API
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"Testing DataLake - ${this.getClass.getSimpleName}", args, Global)
    //Creación del objeto control, por default no permite ejecuciones en paralelo del mismo objeto (corre en modo SINGLETON)
    val Control = new huemul_Control(huemulBigDataGov, null, huemulType_Frequency.MONTHLY )
    
    /*************** PARAMETROS **********************/
    var param_ano = huemulBigDataGov.arguments.GetValue("ano", null, "Debe especificar el parámetro año, ej: ano=2017").toInt
    var param_mes = huemulBigDataGov.arguments.GetValue("mes", null, "Debe especificar el parámetro mes, ej: mes=12").toInt
    
    //Inicializa clase RAW  
    val DF_RAW =  new raw_entidad_mes(huemulBigDataGov, Control)
    if (!DF_RAW.open("DF_RAW", null, param_ano, param_mes, 0, 0, 0, 0)) {
      println("************************************************************")
      println("**********  E  R R O R   E N   P R O C E S O   *************")
      println("************************************************************")
    } else
      DF_RAW.DataFramehuemul.DataFrame.show()
      
    Control.FinishProcessOK
    huemulBigDataGov.close()
   
  }  
}
