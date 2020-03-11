package id3

class Node(val id: Int, val parentId: Int, val unusedAttributes: Set[Int]) {
  var attribute: Int = -1
  var children: Array[Int] = Array.empty[Int]
  var isLeaf = true
  var leafLabel: String = ""
  //val branchFactor = 3  //corrisponde al numero massimo di valori per attributo TENNIS
  val branchFactor = 4    //CAR

  def setAttribute(attr: Int): Unit ={
    this.attribute = attr
  }

  def setIsleaf(value: Boolean): Unit ={
    this.isLeaf = value
  }

  def setLeafLabel(label: String): Unit ={
    this.leafLabel = label
  }

  def setChildren(size: Int): Unit ={
    this.children = new Array[Int](size)
  }

  def setChild(index: Int, value: Int): Unit ={
    this.children(index) = value
  }

  //il numero di figli dipende dal numero di valori per attributo
  def genChildren(size: Int): List[Node] ={

    //lista per return
    var childrenNodeList = List.empty[Node]
    this.children = new Array[Int](size)
    var childId = this.id * branchFactor

    for(index <- 0 until size){
      childId += 1
      //aggiorno lista interna del nodo padre
      this.children(index) = childId
      //creo nodo
      childrenNodeList = new Node(childId, this.id, this.unusedAttributes.diff(Set(this.attribute))) :: childrenNodeList
    }

    childrenNodeList
  }
}
