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
import com.twitter.scalding.mathematics.{FiniteHint, SparseHint, Matrix}
import breeze.linalg.{SparseVector, DenseMatrix, parallel}
import com.twitter.scalding.mathematics.Matrix._
import com.twitter.algebird.{Aggregator,Monoid}
import cascading.tuple.{TupleEntry, Fields}
import cascading.operation._
import cascading.flow.FlowProcess
import cascading.pipe.{Every, Each, Pipe}
import java.util.UUID
import scala.Some
import collection.JavaConverters._


object linalg {

  type QRPair = (Option[DenseMatrix[Double]],DenseMatrix[Double])


  private def stackAndFactor(l: QRPair, r: QRPair, cols: Int, skipQ: Boolean = false) : QRPair = {
    val (lq,lr) = l
    val (rq,rr) = r
    if(lr.rows + rr.rows < cols) (None,DenseMatrix.vertcat(lr,rr))
    else {
      // no Q calculations, much faster
      if(skipQ) (None,breeze.linalg.qr(DenseMatrix.vertcat(lr,rr),true)._2(0 until cols,::))
      // do the Q calculations, but do them piecewise as we go for numerical stability
      else {
        val (newQ, newR) = breeze.linalg.qr(DenseMatrix.vertcat(lr,rr),false)
        val outR = newR(0 until cols,::)
        val newQTop = lq.map{mat => mat * newQ(0 until lr.rows,0 until cols)}.getOrElse(newQ(0 until lr.rows,0 until cols))
        val newQBottom = rq.map{mat => mat * newQ(lr.rows until (lr.rows + rr.rows),0 until cols)}.getOrElse(newQ(lr.rows until (lr.rows + rr.rows),0 until cols))
        println("Top Q is of dimension " + newQTop.rows + "x" + newQTop.cols + " and bottom Q of dimension " + newQBottom.rows + "x" + newQBottom.cols)
        (Some(DenseMatrix.vertcat(newQTop,newQBottom)), outR)
      }
    }
  }

  class MapSideFactorizeFn ( fields: Fields,
                             cols: Int,
                             mult: Double)
    extends BaseOperation[List[DenseMatrix[Double]]](fields) with Function[List[DenseMatrix[Double]]]{

    import Dsl._
    val conv = implicitly[TupleConverter[SparseVector[Double]]]
    val set = implicitly[TupleSetter[(Boolean,UUID,DenseMatrix[Double])]]

    override def prepare(flowProcess: FlowProcess[_], operationCall: OperationCall[List[DenseMatrix[Double]]]) {

      operationCall.setContext(List.empty[DenseMatrix[Double]])
    }

    override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[List[DenseMatrix[Double]]]) {
      // this seems like a good place for pattern matching
      val matList = functionCall.getContext
      val vec = conv(functionCall.getArguments)
      val rowMat = DenseMatrix(vec.toDenseVector.toArray)
      val currentMat = if(!matList.isEmpty && (matList.head.rows < mult*matList.head.cols)) Some(matList.head)
                       else None
      val newMatList = if(matList.isEmpty) List(rowMat) else List(rowMat,matList.head)
      val newHead = currentMat.map(mat => List(DenseMatrix.vertcat(rowMat,mat))).getOrElse(newMatList)
      functionCall.setContext(if(matList.isEmpty) newHead else newHead ++ matList.tail)
    }



    override def flush(flowProcess: FlowProcess[_], functionCall: OperationCall[List[DenseMatrix[Double]]]) {
      val matList = functionCall.getContext
      val matPairList: List[QRPair] = matList.map(mat => (None,mat))
      val (q,r) = matPairList.par.reduce((l,r) => stackAndFactor(l,r,cols))

      // this is a safe cast per the cascading docs
      val collector = functionCall.asInstanceOf[FunctionCall[DenseMatrix[Double]]].getOutputCollector
      val uuid = UUID.randomUUID

      q.foreach{mat => collector.add(set(false,uuid,mat))}
      collector.add(set(true,uuid,r))
    }
  }




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

  private def rowToSparseVec[RowT](scaldingRow: Iterator[(RowT,Int,Double)], cols: Int) : Iterator[SparseVector[Double]] = {
    val indexedList = scaldingRow.map(tup => (tup._2,tup._3)).toSeq
    Iterator(SparseVector[Double](cols)(indexedList: _*))
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

    val agg = new QRDecompositionAggregator((cols*bufferMultiple).toInt)
    val newPipe = filterOutZeros(mat.valSym, mon) {
        val sparseSym = Symbol(mat.valSym + "_sparse")
        val denseSym = Symbol(mat.valSym + "_dense")
        mat.pipe.groupBy(mat.rowSym){_.mapStream((mat.rowSym,mat.colSym,mat.valSym)->sparseSym){it: Iterator[(RowT,Int,Double)] => rowToSparseVec(it,cols)}}
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
    val newHint = SparseHint(0.5,cols,cols)
    new Matrix[Int, Int, Double](mat.rowSym, mat.colSym, mat.valSym, newPipe, newHint)
  }

  def dtsqr[RowT,ValT](inputMat: Matrix[RowT, Int, ValT],
                      cols: Int,
                      bufferMultiple: Double = 1.01)
                     (implicit mon: Monoid[Double], ord: Ordering[RowT], ev : =:=[ValT,Double]):
  (Matrix[String, Int, Double],Matrix[Int, Int, Double]) = {
    import Dsl._
    import TDsl._

    val mat = inputMat.asInstanceOf[Matrix[RowT,Int,Double]]

    val sparseSym = Symbol(mat.valSym + "_sparse")
    val denseSym = Symbol(mat.valSym + "_dense")
    val uuidSym = Symbol(mat.valSym + "_uuid")
    val uuidSym2 = Symbol(mat.valSym + "_uuid2")
    val isRSym = Symbol(mat.valSym + "_isR")
    val reduceSym = Symbol(mat.valSym + "_lastQR")
    val qSym = Symbol(mat.valSym + "_lastQ")
    val sparseRowPipe = mat.pipe.groupBy(mat.rowSym){_.mapStream((mat.rowSym,mat.colSym,mat.valSym)->sparseSym){it: Iterator[(RowT,Int,Double)] => rowToSparseVec(it,cols)}}
    val qrTogether = new Each(sparseRowPipe, sparseSym, new MapSideFactorizeFn((isRSym,uuidSym,denseSym),cols,bufferMultiple), Fields.RESULTS)
    val typedQRTogether = qrTogether.toTypedPipe[(Boolean,UUID,DenseMatrix[Double])]((isRSym,uuidSym,denseSym)) 


    val firstQs = typedQRTogether.filter(! _._1).groupBy(_._2) 
    val intermedRs = typedQRTogether.filter(_._1) 

    val finalQR = intermedRs.groupAll
                            .mapValueStream{ it: Iterator[(Boolean,UUID, DenseMatrix[Double])] =>
                              val parts = it.map{tup => (List(tup._2),(None: Option[DenseMatrix[Double]],tup._3))}.toSeq
                              val (uuidList,(q,r)) = if (parts.length < 2) {
                                val (ul,(q,r)) = parts(0)
                                (ul,(Some(DenseMatrix.eye[Double](cols)),r))
                              }
                              else {
                                  parts.par.reduce{(l: (List[UUID],QRPair),r: (List[UUID],QRPair)) =>
                                    val uuidL = l._1 ++ r._1
                                    val newMats = stackAndFactor(l._2,r._2,cols)
                                    (uuidL,newMats)
                                }
                              }
                              val qs = q.map{mat =>
                                uuidList.zipWithIndex.map{ case(uuid,ind) =>
                                  (false,uuid,mat(ind*cols until (ind+1)*cols,::))
                                }
                              }.getOrElse(List.empty)
                              val rs = (true,uuidList.head,r)
                              Iterator(qs: _*) ++ Iterator(rs)
                            }
                            .values


    val secondQs = finalQR.filter(! _._1).groupBy(_._2) 
    val finalR = finalQR.filter(_._1) 
    val r = {
       val newPipe = filterOutZeros(mat.valSym, mon) {
        finalR.toPipe(('isR,'uuid,'matrix)).flatMapTo('matrix->(mat.rowSym,mat.colSym,mat.valSym))
                                    {mat: DenseMatrix[Double] => mat.iterator.map {
                                      case((row,col),value) => (row, col, value)
                                      }.toIterable
                                    }
      }
      val newHint = SparseHint(0.5,cols,cols)
      new Matrix[Int, Int, Double](mat.rowSym, mat.colSym, mat.valSym, newPipe, newHint)
    }

    val q = {
        val newPipe = firstQs.join(secondQs).toTypedPipe
                             .flatMap{case(uuid,(l,r)) =>
                                val mat = l._3 * r._3
                                val output = for(((row,col),value) <- mat.iterator; if(value != 0))
                                  yield (uuid.toString + "_" + row,col,value)
                                output.toIterable
                             }
                             .toPipe((mat.rowSym,mat.colSym,mat.valSym))
        val newHint = FiniteHint(-1L,cols)
      new Matrix[String, Int, Double](mat.rowSym, mat.colSym, mat.valSym, newPipe, newHint)
    }
    (q,r)

  }



}