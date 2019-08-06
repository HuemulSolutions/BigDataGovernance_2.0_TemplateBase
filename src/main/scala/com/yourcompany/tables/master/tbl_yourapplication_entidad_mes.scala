package com.yourcompany.tables.master


import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.dataquality._
import org.apache.spark.sql.types._


class tbl_yourapplication_entidad_mes(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(huemulBigDataGov, Control) with Serializable {
  /**********   C O N F I G U R A C I O N   D E   L A   T A B L A   ****************************************/
  //Tipo de tabla, Master y Reference son catalogos sin particiones de periodo
  this.setTableType(huemulType_Tables.Transaction)
  //Base de Datos en HIVE donde sera creada la tabla
  this.setDataBase(huemulBigDataGov.GlobalSettings.MASTER_DataBase)
  //Tipo de archivo que sera almacenado en HDFS
  this.setStorageType(huemulType_StorageType.PARQUET)
  //Ruta en HDFS donde se guardara el archivo PARQUET
  this.setGlobalPaths(huemulBigDataGov.GlobalSettings.MASTER_SmallFiles_Path)
  //Ruta en HDFS especifica para esta tabla (Globalpaths / localPath)
  this.setLocalPath("yourapplication/")
    //columna de particion
  this.setPartitionField("periodo_mes")
  //Frecuencia de actualización de los datos
  this.setFrequency(huemulType_Frequency.MONTHLY)
  
  /**********   O P T I M I Z A C I O N  ****************************************/
  //Indica la cantidad de particiones al guardar un archivo, para archivos pequeños (menor al bloque de HDFS) se 
  //recomienda el valor 1, mientras mayor la tabla la cantidad de particiones debe ser mayor para aprovechar el paralelismo
  //this.setNumPartitions(1)
  
  /**********   C O N T R O L   D E   C A M B I O S   Y   B A C K U P   ****************************************/
  //Permite guardar los errores y warnings en la aplicación de reglas de DQ, valor por default es true
  //this.setSaveDQResult(true)
  //Permite guardar backup de tablas maestras
  //this.setSaveBackup(true)  //default value = false
   
  /**********   S E T E O   I N F O R M A T I V O   ****************************************/
  //Nombre del contacto de TI
  this.setDescription("[[LLENAR ESTE CAMPO]]")
  //Nombre del contacto de negocio
  this.setBusiness_ResponsibleName("[[LLENAR ESTE CAMPO]]")
  //Nombre del contacto de TI
  this.setIT_ResponsibleName("[[LLENAR ESTE CAMPO]]")
   
  /**********   D A T A   Q U A L I T Y   ****************************************/
  //DataQuality: maximo numero de filas o porcentaje permitido, dejar comentado o null en caso de no aplicar
  //this.setDQ_MaxNewRecords_Num(null)  //ej: 1000 para permitir maximo 1.000 registros nuevos cada vez que se intenta insertar
  //this.setDQ_MaxNewRecords_Perc(null) //ej: 0.2 para limitar al 20% de filas nuevas
    
  /**********   S E G U R I D A D   ****************************************/
  //Solo estos package y clases pueden ejecutar en modo full, si no se especifica todos pueden invocar
  //this.WhoCanRun_executeFull_addAccess("process_entidad_mes", "com.yourcompany.yourapplication")
  //Solo estos package y clases pueden ejecutar en modo solo Insert, si no se especifica todos pueden invocar
  //this.WhoCanRun_executeOnlyInsert_addAccess("[[MyclassName]]", "[[my.package.path]]")
  //Solo estos package y clases pueden ejecutar en modo solo Update, si no se especifica todos pueden invocar
  //this.WhoCanRun_executeOnlyUpdate_addAccess("[[MyclassName]]", "[[my.package.path]]")
  

  /**********   C O L U M N A S   ****************************************/

    //Columna de periodo
  val periodo_mes = new huemul_Columns (StringType, true,"periodo de los datos")
  periodo_mes.setIsPK(true)
  periodo_mes.setBusinessGlossary_Id("BG001")  
    
  val ejemplo_producto_id = new huemul_Columns (IntegerType, true, "codigo del producto") 
  ejemplo_producto_id.setARCO_Data(false)  
  ejemplo_producto_id.setSecurityLevel(huemulType_SecurityLevel.Public)  
  //ejemplo_producto_id.setDQ_MinDecimalValue(Decimal.apply(0))  
  //ejemplo_producto_id.setDQ_MaxDecimalValue(Decimal.apply(200.34))  

  val fecha_venta = new huemul_Columns (StringType, true, "fecha de la venta") 
  fecha_venta.setARCO_Data(false)  
  fecha_venta.setSecurityLevel(huemulType_SecurityLevel.Public)  
  //fecha_venta.setDQ_MinLen(5) 
  //fecha_venta.setDQ_MaxLen(100)  

  val cantidad = new huemul_Columns (DecimalType(10,2), true, "Cantidad del producto") 
  cantidad.setARCO_Data(false)  
  cantidad.setSecurityLevel(huemulType_SecurityLevel.Public)  

  val precio = new huemul_Columns (DecimalType(10,2), true, "Precio de la transaccion") 
  precio.setARCO_Data(false)  
  precio.setSecurityLevel(huemulType_SecurityLevel.Public)  



  //**********Atributos adicionales de DataQuality
  //yourColumn.setIsPK(true)     //valor por default en cada campo es false
  //yourColumn.setIsUnique(true) //valor por default en cada campo es false
  //yourColumn.setNullable(true) //valor por default en cada campo es false
  //yourColumn.setIsUnique(true) //valor por default en cada campo es false
  //yourColumn.setDQ_MinDecimalValue(Decimal.apply(0))
  //yourColumn.setDQ_MaxDecimalValue(Decimal.apply(200.0))
  //yourColumn.setDQ_MinDateTimeValue("2018-01-01")
  //yourColumn.setDQ_MaxDateTimeValue("2018-12-31")
  //yourColumn.setDQ_MinLen(5)
  //yourColumn.setDQ_MaxLen(100)
  //yourColumn.setDQ_RegExp("")                          //desde versión 2.0
  //yourColumn.setDefaultValue("'string'") // "10" // "'2018-01-01'"
  //**********Atributos adicionales para control de cambios en los datos maestros
  //yourColumn.setMDM_EnableDTLog(true)
  //yourColumn.setMDM_EnableOldValue(true)
  //yourColumn.setMDM_EnableProcessLog(true)
  //yourColumn.setMDM_EnableOldValue_FullTrace(true)     //desde 2.0: guarda cada cambio de la tabla maestra en tabla de trace
  //**********Otros atributos de clasificación
  //yourColumn.setEncryptedType("tipo")
  //yourColumn.setARCO_Data(true)
  //yourColumn.setSecurityLevel(huemulType_SecurityLevel.Public)
  //yourColumn.setBusinessGlossary_Id("BG_ID")           //desde 2.0: enlaza id de glosario de términos con campos de la tabla
   
  
  //**********Ejemplo para aplicar DataQuality de Integridad Referencial
  //val i[[tbl_PK]] = new [[tbl_PK]](huemulBigDataGov,Control)
  //val fk_[[tbl_PK]] = new huemul_Table_Relationship(i[[tbl_PK]], false)
  //fk_[[tbl_PK]].AddRelationship(i[[tbl_PK]].[[PK_Id]], [[LocalField]_Id)
    
  //**********Ejemplo para agregar reglas de DataQuality Avanzadas  -->ColumnXX puede ser null si la validacion es a nivel de tabla
  //**************Parametros
  //********************  ColumnXXColumna a la cual se aplica la validacion, si es a nivel de tabla poner null
  //********************  Descripcion de la validacion, ejemplo: "Consistencia: Campo1 debe ser mayor que campo 2"
  //********************  Formula SQL En Positivo, ejemplo1: campo1 > campo2  ;ejemplo2: sum(campo1) > sum(campo2)  
  //********************  CodigoError: Puedes especificar un codigo para la captura posterior de errores, es un numero entre 1 y 999
  //********************  QueryLevel es opcional, por default es "row" y se aplica al ejemplo1 de la formula, para el ejmplo2 se debe indicar "Aggregate"
  //********************  Notification es opcional, por default es "error", y ante la aparicion del error el programa falla, si lo cambias a "warning" y la validacion falla, el programa sigue y solo sera notificado
  //********************  SaveErrorDetails es opcional, por default es "true", permite almacenar el detalle del error o warning en una tabla específica, debe estar habilitada la opción DQ_SaveErrorDetails en GlobalSettings
  //********************  DQ_ExternalCode es opcional, por default es "null", permite asociar un Id externo de DQ
  //val DQ_NombreRegla: huemul_DataQuality = new huemul_DataQuality(ColumnXX,"Descripcion de la validacion", "Campo_1 > Campo_2",1)
  //**************Adicionalmeente, puedes agregar "tolerancia" a la validacion, es decir, puedes especiicar 
  //************** numFilas = 10 para permitir 10 errores (al 11 se cae)
  //************** porcentaje = 0.2 para permitir una tolerancia del 20% de errores
  //************** ambos parametros son independientes (condicion o), cualquiera de las dos tolerancias que no se cumpla se gatilla el error o warning
  //DQ_NombreRegla.setTolerance(numfilas, porcentaje)
  //DQ_NombreRegla.setDQ_ExternalCode("Cod_001")
    
  this.ApplyTableDefinition()
}
