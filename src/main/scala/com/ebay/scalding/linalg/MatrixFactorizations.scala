package com.ebay.scalding

import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix
import breeze.linalg.{SparseVector, DenseMatrix}
import breeze.linalg.parallel
import com.twitter.scalding.mathematics.Matrix._
import com.twitter.algebird.{Aggregator,Monoid}

object linalg {

  class QRDecompositionAggregator(buf: Int) extends Aggregator[SparseVector[Double],DenseMatrix[Double],Iterable[(Int,Int,Double)]]  {
    def prepare(input: SparseVector[Double]): DenseMatrix[Double] = DenseMatrix(input.toDenseVector.toArray)

    def reduce(l: DenseMatrix[Double], r: DenseMatrix[Double]): DenseMatrix[Double] = {
      val newMat = DenseMatrix.vertcat(l,r)
      if (newMat.rows > buf) parallel.tsqr(newMat, buf) else newMat
    }

    def present(dMat: DenseMatrix[Double]): Iterable[(Int, Int, Double)] = {
      val finalMat = parallel.tsqr(dMat,buf)
      finalMat.iterator.map {
        tup => (tup._1._1, tup._1._2, tup._2)
      }.toIterable
    }
  }


  def tsqr[RowT,ValT](inputMat: Matrix[RowT, Int, ValT],
                 cols: Int,
                 bufferMultiple: Int = 5)
                (implicit mon: Monoid[Double], ord: Ordering[RowT], ev : =:=[ValT,Double]): Matrix[Int, Int, Double] = {
    // import TDsl._
    import Dsl._
    val mat = inputMat.asInstanceOf[Matrix[RowT,Int,Double]]
    def rowToSparseVec(scaldingRow: Iterator[(RowT,Int,Double)]) : Iterator[SparseVector[Double]] = {
      val indexedList = scaldingRow.map(tup => (tup._2,tup._3)).toSeq
      Iterator(SparseVector[Double](cols)(indexedList: _*))
    }
    val agg = new QRDecompositionAggregator(cols*bufferMultiple)
    val newPipe = filterOutZeros(mat.valSym, mon) {
        val sparseSym = Symbol(mat.valSym + "_sparse")
        val denseSym = Symbol(mat.valSym + "_dense")
        mat.pipe.groupBy(mat.rowSym){_.mapStream((mat.rowSym,mat.colSym,mat.valSym)->sparseSym)(rowToSparseVec)}
                .groupAll(_.mapReduceMap(sparseSym->denseSym)(agg.prepare)(agg.reduce)(agg.present))
                .flatMapTo(denseSym->(mat.rowSym,mat.colSym,mat.valSym)){it: Iterable[(Int,Int,Double)] => it}

//      val outpipe = mat.pipe.toTypedPipe[(RowT,Int,Double)]((mat.rowSym,mat.colSym,mat.valSym))
//                       .groupBy(_._1)
//                       .mapValueStream(rowToSparseVec)
//                       .values
//                       .aggregate(new QRDecompositionAggregator(cols*bufferMultiple))
//                       .flatMap(identity)
//      outpipe.toPipe((mat.rowSym,mat.colSym,mat.valSym))
    }
    val newHint = mat.sizeHint.setCols(cols).setRows(cols)
    new Matrix[Int, Int, Double](mat.rowSym, mat.colSym, mat.valSym, newPipe, newHint)
  }


}