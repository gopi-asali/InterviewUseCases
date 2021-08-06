package usecases

import scala.collection.mutable

object angram {

  def main(args: Array[String]): Unit = {

    val in: Array[String] = "abeee".split("")

    val in2: Array[String] = "abe".split("")

    val mapper: mutable.Map[String, Int] = mutable.Map[String, Int]().empty
    val mapper1: mutable.Map[String, Int] = mutable.Map[String, Int]().empty

    var flag = true

    for (x <- 0 to in.length - 1) {
      if (mapper.contains(in(x))) {

        val number = mapper(in(x)) + 1
        mapper.put(in(x), number)

      } else {
        mapper.put(in(x), 1)
      }
    }

    for (x <- 0 to in2.length - 1) {
      if (mapper1.contains(in2(x))) {

        val number = mapper1(in2(x)) + 1
        mapper1.put(in2(x), number)

      } else {
        mapper1.put(in2(x), 1)
      }
    }

    mapper.foreach(value => {
      if (mapper1.contains(value._1)) {
        if (mapper1(value._1) == value._2) {

        } else {
          println("Not angram")
          System.exit(0)
        }
      }else{
        println("Not angram")
        System.exit(0)
      }
    })

    mapper1.foreach(value => {
      if (mapper.contains(value._1)) {
        if (mapper(value._1) == value._2) {
        } else {
          println("Not angram")
          System.exit(0)
        }
      }else{
        println("Not angram")
        System.exit(0)
      }
    })


    println("angram")

  }

}
