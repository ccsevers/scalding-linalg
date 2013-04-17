/*
Copyright 2012 eBay, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/



package com.ebay.scalding

import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix
import breeze.linalg.{SparseVector, DenseMatrix}
import breeze.linalg.parallel
import com.twitter.scalding.mathematics.Matrix._
import com.twitter.algebird.{Aggregator,Monoid}

object linalg {

  class QRDecompositionAggregator(buf: Int) extends Aggregator[SparseVector[Double],DenseMatrix[Double],Iterable[(Int,Int,Double)]]  {

    // it would be nice if there was a more efficient way to do this.
    def prepare(input: SparseVector[Double]): DenseMatrix[Double] = DenseMatrix(input.toDenseVector.toArray)

    def reduce(l: DenseMatrix[Double], r: DenseMatrix[Double]): DenseMatrix[Double] = {
      val newMat = DenseMatrix.vertcat(l,r)
      if (newMat.rows > buf) parallel.tsqr(newMat, buf, true)._2 else newMat
    }

    def present(dMat: DenseMatrix[Double]): Iterable[(Int, Int, Double)] = {
      val finalMat = parallel.tsqr(dMat,buf,true)._2
      finalMat.iterator.map {
        tup => (tup._1._1, tup._1._2, tup._2)
      }.toIterable
    }
  }

  /**
   * Do a distributed QR factorization of a tall-skinny matrix.
   * @param inputMat The scalding matrix to factorize. It should be mxn with m >= n. It also needs ValT to be viewable
   *                 as Double.
   * @param cols For now you have to explicitly give the number of columns.
   * @param bufferMultiple Number of nxn blocks to consider at a time.
   * @param mon Already set to be a Double monoid. Maybe changeable in the future.
   * @param ord Need an ordering for RowT.
   * @param ev Need to be sure ValT is viewable as Double.
   * @tparam RowT The row index type
   * @tparam ValT The value type
   * @return The R part of the QR factorization, which is a scalding Matrix[Int,Int,Double], nxn, upper triangular. Zeros
   *         removed.
   */
  def tsqr[RowT,ValT](inputMat: Matrix[RowT, Int, ValT],
                 cols: Int,
                 bufferMultiple: Double = 1.01)
                (implicit mon: Monoid[Double], ord: Ordering[RowT], ev : =:=[ValT,Double]): Matrix[Int, Int, Double] = {
    // import TDsl._
    import Dsl._
    val mat = inputMat.asInstanceOf[Matrix[RowT,Int,Double]]
    def rowToSparseVec(scaldingRow: Iterator[(RowT,Int,Double)]) : Iterator[SparseVector[Double]] = {
      val indexedList = scaldingRow.map(tup => (tup._2,tup._3)).toSeq
      Iterator(SparseVector[Double](cols)(indexedList: _*))
    }
    val agg = new QRDecompositionAggregator((cols*bufferMultiple).toInt)
    val newPipe = filterOutZeros(mat.valSym, mon) {
        val sparseSym = Symbol(mat.valSym + "_sparse")
        val denseSym = Symbol(mat.valSym + "_dense")
        mat.pipe.groupBy(mat.rowSym){_.mapStream((mat.rowSym,mat.colSym,mat.valSym)->sparseSym)(rowToSparseVec)}
                .groupAll(_.mapReduceMap(sparseSym->denseSym)(agg.prepare)(agg.reduce)(agg.present))
                .flatMapTo(denseSym->(mat.rowSym,mat.colSym,mat.valSym)){it: Iterable[(Int,Int,Double)] => it}

      /* Waiting to change to typed api
      val outpipe = mat.pipe.toTypedPipe[(RowT,Int,Double)]((mat.rowSym,mat.colSym,mat.valSym))
                       .groupBy(_._1)
                       .mapValueStream(rowToSparseVec)
                       .values
                       .aggregate(new QRDecompositionAggregator(cols*bufferMultiple))
                       .flatMap(identity)
      outpipe.toPipe((mat.rowSym,mat.colSym,mat.valSym))
      */
    }
    val newHint = mat.sizeHint.setCols(cols).setRows(cols)
    new Matrix[Int, Int, Double](mat.rowSym, mat.colSym, mat.valSym, newPipe, newHint)
  }


}