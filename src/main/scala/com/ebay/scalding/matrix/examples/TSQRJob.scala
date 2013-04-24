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
  val (q,r) = dtsqr(inputMatrix,args("cols").toInt)
  q.write(Tsv("output/Q"))
  r.write(Tsv("output/R"))
}
