package id3

import java.io.Serializable


class Node(val branchFactor: Int,val id: Int, val parentId: Int, val unusedAttributes: Set[Int]) extends Serializable{
  var attribute: Int = -1
  var children: Array[Int] = Array.empty[Int]
  var isLeaf = true
  var leafLabel: String = ""

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

  def genChildren(size: Int): List[Node] ={

    var childrenNodeList = List.empty[Node]

    //prima di procedere con la generazione dei figli
    //devo controllare se nella lista degli attributi non usati dei figli
    //è rimasto qualche attributo

      //ok, ci sono ancora attributi disponibili, posso procedere
      this.children = new Array[Int](size)
      var childId = this.id * this.branchFactor

      for(index <- 0 until size){
        childId += 1
        //aggiorno lista interna del nodo padre
        this.children(index) = childId
        //creo nodo
        childrenNodeList = new Node(this.branchFactor, childId, this.id, this.unusedAttributes.diff(Set(this.attribute))) :: childrenNodeList
      }
    childrenNodeList
  }

}
