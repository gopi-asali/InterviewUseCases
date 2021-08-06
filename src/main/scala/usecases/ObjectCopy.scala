package usecases

;

sealed trait noName

sealed trait Name

sealed trait NoAge

sealed trait Age

sealed trait NotPrinted

sealed trait Printed


object TestClass {


  def main(args: Array[String]): Unit = {
    val record = Record[noName, NoAge, NotPrinted](None, None)
    val namedRecord = addName(record)
    printRecord(printRecord(addAge(namedRecord)))
  }


  def addName(a: Record[noName, NoAge, NotPrinted]): Record[Name, NoAge, NotPrinted] = {
    a.copy(name = Some("gopi"))
  }

  def addAge(a: Record[Name, NoAge, NotPrinted]): Record[Name, Age, NotPrinted] = {
    a.copy(age = Some("27"))
  }

  def printRecord(a: Record[Name, Age, NotPrinted]): Record[Name, Age, NotPrinted] = {


    println(a.name.get + a.age.get)

    a

  }


}


case class Record[A, B, C](name: Option[String], age: Option[String])
