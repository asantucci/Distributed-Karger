# Distributed-Karger
Class project for [CME 323](https://stanford.edu/~rezab/classes/cme323/S15/). Working with Eric Lax, we authored several distributed algorithms to solve the global min-cut problem in graph theory. Along the way to designing our own algorithm, we come up with a new implementation for Karger's min-cut algorithm, which we implement in `karger.scala`. 

### Core Idea
The core behind the novel implementation of Karger's min-cut algorithm is that by randomly assigning edge weights, finding a minimum spanning tree, and then removing the heaviest weight edge, we realize a cut; this is equivalent to a series of contractions in Karger's min-cut algorithm.
