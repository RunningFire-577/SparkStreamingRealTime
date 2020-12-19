package bean

import java.io.{FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Properties
object MyProperties {
  def load(filepath:String):Properties={
    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(filepath),StandardCharsets.UTF_8))
    prop

  }

}
