## Get Started
In order to be able to submit RDFStats at `spark-submit`, first clone the repo and compile the project.

##Load the RDF datasets
Before computing statistics, download datasets and upload them on the HDFS. The following steps should be taken :
* Download DBPedia and extract it on bin .nt file
    * Dbpedia en
    ```sh
    wget -r -np -nd -nc -A'*.nt.bz2' http://downloads.dbpedia.org/3.9/en/
    cat *.nt.bz2 >Dbpedia_en.nt.bz2
    bzip2 -d Dbpedia_en.nt.bz2
    hadoop fs -put Dbpedia_en.nt /<pathToHDFS>/
    ```
    * Dbpedia de
     ```sh
    wget -r -np -nd -nc -A'*.nt.bz2' http://downloads.dbpedia.org/3.9/de/
    cat *.nt.bz2 >Dbpedia_de.nt.bz2
    bzip2 -d Dbpedia_de.nt.bz2
    hadoop fs -put Dbpedia_de.nt /<pathToHDFS>/
    ```
     * Dbpedia fr
     ```sh
    wget -r -np -nd -nc -A'*.nt.bz2' http://downloads.dbpedia.org/3.9/fr/
    cat *.nt.bz2 >Dbpedia_fr.nt.bz2
    bzip2 -d Dbpedia_fr.nt.bz2
    hadoop fs -put Dbpedia_fr.nt /<pathToHDFS>/
    ```
* Download LinkedGeoData and extract it on bin .nt file
    ```sh
    wget -r -np -nd -nc -A'*.nt.bz2' http://downloads.linkedgeodata.org/releases/2015-11-02/
    cat *.nt.bz2 >LinkedGeoData.nt.bz2
    bzip2 -d LinkedGeoData.nt.bz2
    hadoop fs -put LinkedGeoData.nt /<pathToHDFS>/
    ```
* Generate BSBM datasets
    ```sh
    wget http://downloads.sourceforge.net/project/bsbmtools/bsbmtools/bsbmtools-0.2/bsbmtools-v0.2.zip
    unzip bsbmtools-v0.2.zip
    cd bsbmtools-0.2/
    ```
    We have generated the dataset based on the size :
    ```sh
     "./generate -fc -s nt -fn BSBM_2GB -pc 23336"
     "./generate -fc -s nt -fn BSBM_20GB -pc 233368"
     "./generate -fc -s nt -fn BSBM_200GB -pc 2333682"
     "./generate -fc -s nt -fn BSBM_50GB -pc 583420" 
     "./generate -fc -s nt -fn BSBM_100GB -pc 1166840"
     ```
     ```sh
     hadoop fs -put BSBM_XGB.nt /<pathToHDFS>/
     ```
## Experiments
* Distributed Processing on Large-Scale Datasets
Run DistLODStats agains the datasets and get the stats generated. Run the following command to get the results:
    1. For `cluster` mode
    ```sh
    ./run_stats.sh Dbpedia_en Iter1
    ```
    1. For `local` mode
    ```sh
    ./run_stats-local.sh Dbpedia_en Iter1
    ```
* Scalability
  * Sizeup  scalability
  To measure the performance of size-up scalability of our approach, we run experiments on three different sizes.
  * Node scalability
  In order to measure node scalability, we use variations of the number of the workers on our cluster. 