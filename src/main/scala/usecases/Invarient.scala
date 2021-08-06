package usecases

sealed abstract class Vehicle

case class van() extends Vehicle {

}


case class Motorcycle() extends Vehicle


case class Parking[+A](value: A) {
  self =>


}

object test {
  def main(args: Array[String]): Unit = {
    //    val p1: com.test.exercise.Parking[Car]  = com.test.exercise.Parking(new Car)


  }
}



