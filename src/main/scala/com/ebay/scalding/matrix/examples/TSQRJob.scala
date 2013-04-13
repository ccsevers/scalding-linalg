package com.ebay.scalding.matrix.examples

import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix
import com.ebay.scalding.linalg._


class TSQRJob(args: Args) extends Job(args) {
  import Matrix._
  import com.ebay.scalding.linalg.tsqr

  val inputMatrix = Tsv( args("input"), ('doc, 'word, 'count) )
    .read
    .toMatrix[Long,Int,Double](('doc, 'word, 'count))

  // compute the overall document frequency of each row
  val rMatrix = tsqr(inputMatrix,args("cols").toInt, 3)
  rMatrix.write(Tsv("output"))




}
