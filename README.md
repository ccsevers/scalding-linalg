scalding-linalg
===============

Linear algebra routines for Scalding. 

Right now contains Tall-Skinny QR Factorization, based on the paper by Constantine and Gleich:

http://www.cs.purdue.edu/homes/dgleich/publications/Constantine%202011%20-%20TSQR.pdf

The direct Q calculation is from a preprint by Benson, Demmel and  Gleich:

http://arxiv.org/abs/1301.1071

TODO:

SVD via random projection based on the approach of Halko, Martinsson and Tropp:

http://arxiv.org/abs/0909.4061

Also investigating to see if there is a good parallel NMF algorithm suitable for Hadoop. 

