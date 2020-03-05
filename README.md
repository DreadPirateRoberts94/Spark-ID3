# Spark-ID3

Spark-ID3 è un progetto universitario realizzato per prendere dimestichezza con il framework Spark e la programmazione con Scala in ambiente cloud.



## Run

Per eseguire in locale il progetto è necessario compilarlo attraverso il comando:

```bash
sbt assembly
```

e successivamente 

```
spark-submit --class ID3.Main /path-jar/id3.jar PARAM_LIST 
```

### PARAM_LIST

1. Header del dataset
2. Dataset in input 
3. Path dove si vuole l'albero in output


© Giovanni Fabbretti, Luigi Grossi. Alma Mater Studiorum Università di Bologna.

