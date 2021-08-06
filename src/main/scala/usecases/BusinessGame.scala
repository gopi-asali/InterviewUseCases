package usecases

case class Hotel(roomNumber: Int, User: String) extends Assets
case class Jail() extends Assets
case class Treasure() extends Assets
case class Empty() extends Assets

trait Assets

case class UserInfo(username: String,money: Int)

case class BusinesBoard(userInfo: UserInfo ,asset: Assets,position:Int)





object BusinessGame {

  def main(args: Array[String]): Unit = {

    val positions = Array("E", "E", "H","J")



    "" match{
      case "E" => Empty
      case "" => 
    }


    var personsWithCash = Seq(
      UserInfo("gopi", 1000),
      UserInfo("ram", 1000)
    ).map(obj => BusinesBoard(obj,null,0))

    val start = 1
    val end = 4
    val rnd = new scala.util.Random

    def diceValue: Int = start + rnd.nextInt((end - start) + 1)



    for (i <- 1 to 3) {


      personsWithCash.foreach(board => {
        var movablePosition: Int = board.position + diceValue

        while (movablePosition >= positions.length) {
          movablePosition = movablePosition - positions.length
        }


      })
    }



    /*

    val positions = Array("E", "E", "H","J")
    var personsWithCash = Seq(
      com.test.exercise.UserInfo("gopi", 0, 1000),
      com.test.exercise.UserInfo("ram", 0, 1000)
    )

    if(personsWithCash.length <2 ) throw new Exception("Min 2 users")


    val usersCurrentPosition: mutable.Set[com.test.exercise.Hotel] =
      scala.collection.mutable.Set.empty
    val start = 1
    val end = 4
    val rnd = new scala.util.Random

    def dice: Int = start + rnd.nextInt((end - start) + 1)

    def calculateValue(username: String,
                       position: Int,
                       positionType: String,
                       money: Int): immutable.Seq[com.test.exercise.UserInfo] = {

      positionType match {
        case "E" =>
          List(com.test.exercise.UserInfo(username, position, money))
        case "J" =>

          List(com.test.exercise.UserInfo(username, position,  money - 150))
        case "H" =>
          val anotherUserOwnedStatus: mutable.Set[com.test.exercise.Hotel] =
            usersCurrentPosition.filter(_.roomNumber == position)

          if (anotherUserOwnedStatus.nonEmpty) {
//            println(anotherUserOwnedStatus.head.User)
            List(
              com.test.exercise.UserInfo(username, position,  money - 50),
              com.test.exercise.UserInfo(anotherUserOwnedStatus.head.User, position, 50)
            )
          } else {
            usersCurrentPosition.add(com.test.exercise.Hotel(position, username))
            List(com.test.exercise.UserInfo(username, position, money - 200))
          }
        case "T" =>

          List(com.test.exercise.UserInfo(username, position, money + 200))
      }

    }

    for (i <- 1 to 3) {
      personsWithCash = personsWithCash
        .flatMap(valueTup => {
          val diceValue: Int = dice

          val (position, money) = (valueTup.position, valueTup.money)
          var movablePosition: Int = position + diceValue

          while (movablePosition >= positions.length ){
            movablePosition = movablePosition - positions.length
          }

          val positionValue: String = positions(movablePosition)
          println(movablePosition , positionValue)
          //E,J,T
          calculateValue(
            valueTup.username,
            movablePosition,
            positionValue,
            money
          )
        })
        .groupBy(_.username)
        .map((userDetails: (String, Seq[com.test.exercise.UserInfo])) => {
          val duplicateUserInfo = userDetails._2
          val money =
            duplicateUserInfo.foldLeft(0)((money, user) => money + user.money)
          com.test.exercise.UserInfo(
            duplicateUserInfo.head.username,
            duplicateUserInfo.head.position,
            money
          )
        })
        .toSeq
    }

    val finalValues: String =
      personsWithCash.sortBy(_.money).mkString("\n")
    println(finalValues)*/
  }

}
