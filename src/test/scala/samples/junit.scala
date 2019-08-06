package samples

import org.junit._
import Assert._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.yourcompany.tables.master._


@Test
class AppTest {
    val args: Array[String] = new Array[String](1)
    args(0) = "Environment=production,RegisterInControl=false,TestPlanMode=true"
      
    val huemulBigDataGov = new huemul_BigDataGovernance("Pruebas Inicialización de Clases",args,generador.globalSettings.Global)
    val Control = new huemul_Control(huemulBigDataGov,null, huemulType_Frequency.ANY_MOMENT)

    /****************************************************************************************/
    /**************   tbl_yourapplication_entidad_mes  *********************/
    /****************************************************************************************/
    
    @Test
    def test_tbl_yourapplication_entidad_mes() = assertTrue(TEST_tbl_yourapplication_entidad_mes)
    def TEST_tbl_yourapplication_entidad_mes: Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_yourapplication_entidad_mes(huemulBigDataGov,Control)
        if (Master.Error_isError) {
          println(s"Codigo: ${Master.Error_Code}, Descripción: ${Master.Error_Text}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
      return SinError
    }


    
   

}


