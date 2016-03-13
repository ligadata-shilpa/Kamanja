import scala.beans.BeanProperty

/**
  * Created by Saleh on 3/13/2016.
  */
class Bean2(@BeanProperty var a:String,@BeanProperty @transient var b:String) extends  Serializable {

}
