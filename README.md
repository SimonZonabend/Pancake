# Pancake
Exhaustive java graph search program for the Pancake Sorting Problem
(https://en.wikipedia.org/wiki/Pancake_sorting)
Using a TBBFS - Two bit breadth first search it is possible to 
walk through all possible pancakes stacks and then finding the maximum branch.
Ideas taken from: https://www.aaai.org/Papers/AAAI/2008/AAAI08-050.pdf

 * Typical run.
 * Pancake 11: vertexes=39916800 chunks=4 threads=4 memory usage 9 MB
 * LEVEL 0 NODES=1
 * LEVEL 1 NODES=10
 * LEVEL 2 NODES=90
 * LEVEL 3 NODES=809
 * LEVEL 4 NODES=6429
 * LEVEL 5 NODES=43891
 * LEVEL 6 NODES=252737
 * LEVEL 7 NODES=1174766
 * LEVEL 8 NODES=4126515
 * LEVEL 9 NODES=9981073
 * LEVEL 10 NODES=14250471
 * LEVEL 11 NODES=9123648
 * LEVEL 12 NODES=956354
 * LEVEL 13 NODES=6
 * LAST NODES:
 * [0, 10, 2, 7, 4, 9, 6, 3, 8, 5, 1]
 * [0, 10, 2, 5, 8, 3, 6, 9, 4, 7, 1]
 * [0, 6, 1, 10, 3, 5, 8, 4, 7, 9, 2]
 * [0, 2, 10, 4, 7, 5, 1, 8, 6, 9, 3]
 * [6, 1, 8, 5, 10, 3, 0, 9, 2, 7, 4]
 * [0, 4, 2, 9, 1, 5, 8, 10, 6, 3, 7]
 * Time: 11237 millis. Used memory: 11,26 MB
