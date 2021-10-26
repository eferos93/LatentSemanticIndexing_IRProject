# Information Retrieval System with Latent Semantic Indexing and Spark
Final project of the Information Retreival Course of the master degree in Data Science and Scientific computing.

## Project Requirements
Write an IR system that uses latent semantic indexing to answer queries.
| Requirement | Satisfied |
| ----------- | --------- |
| The system must accept free-form text queries. | :heavy_check_mark: |
| Evaluate the system on a set of queries… | :heavy_check_mark: |
| …and try to use different dimensions for the dimensionality reduction. | :heavy_check_mark: |
| The system must be able to save the data structures used for querying. | :heavy_check_mark: |

## Data
For this project we used two datasets: [CMU Movie Summary Corpus](http://www.cs.cmu.edu/~ark/personas/) and [NPL collection](http://ir.dcs.gla.ac.uk/resources/test_collections/npl/).

The second dataset is already in this repository, while the other must be downloaded from the link above (extract everythning inside the dicrectory data/)


## Running the program
The are two applications: `RankOptimisation` and `MovieSummariesTest`. The first uses the *NPL Corpus* (that has also a set of queries and their correspondet relevance set) for evaluation of the optimal number of singular values, while the latter applies the results obtained in the other corpus.

There are two ways to run the project: on *IntelliJ IDEA* and on terminal via *SBT*. Note that, for an ununkown reason, **performaces are better when running the program inside IntelliJ**

### Running on IntelliJ IDEA

To run one of those two applications, just right click and click run, IntelliJ will create a run configuration, but it needs to be modified. Thus, stop the program and modify the configuration by adding the **VM Options** flags `-Xms10g -Xmx10g`. It should look like the following:
![alt text](https://github.com/eferos93/LatentSemanticIndexing_IRProject/blob/master/config_screenshots/executable_config.png?raw=true)

Similarly you can run also a [**Scala REPL**](https://docs.scala-lang.org/overviews/repl/overview.html). Just right click on any class and select Scala REPL. As before, stop the program and modify the Scala REPL configuration. Add the same flags mentioned above and it should look like the this:
![alt text](https://github.com/eferos93/LatentSemanticIndexing_IRProject/blob/master/config_screenshots/scalaREPL_config.png?raw=true)

### Running on SBT (not reccomended)
To run on SBT, firstly you need to [install it](https://www.scala-sbt.org/1.x/docs/Setup.html). Afterwards, open a terminal inside the project directory and run `sbt`. This will open the sbt console. Then run `compile` and then either `console` to run a scala REPL console, or `run` and the terminal will prompt you to select which of the applications to run.

The memory configurations are already in the file `.sbtopts` inside the repository.


## System Requirements
- At least 16 Gb of RAM
- JDK 1.8 or newer
- Scala plugin for IntelliJ IDEA
