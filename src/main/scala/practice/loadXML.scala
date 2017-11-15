





package practice






object XMLStuff extends App {
  val data = xml.XML.loadFile("../data/medsamp2016a.xml")
  val citations = data \ "MedlineCitation"
  println(citations.head \ "DateCreated" \ "Year")
  println(citations.last \ "DateCreated" \ "Year")
}