# -*- coding: utf-8 -*-

import sys
from operator import add 
from pyspark import SparkContext, SparkConf

# Initialisation de la configuration Spark
sparkConf = SparkConf().setAppName("wordCount").setMaster("local")
# Creation du contexte Spark
sc = SparkContext(conf = sparkConf)
# Lecture du fichier "sample"
texte = sc.textFile("sample.txt")

# 1) Decoupage du texte par mots
# 2) Creation des tuples (mot, comptage)
# 3) Regroupement des mots en additionnant les valeurs de chaque mot
rdd_counts = texte \
    .flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word,1)) \
    .reduceByKey(add)

counts = rdd_counts.collect() # Execution des fonctions au dessus

# Affichage du resultat
print(counts)
for (word, count) in counts:
    print(word, count)

# Sauvegarde du RDD dans le dossier "result"
rdd_counts.coalesce(1).saveAsTextFile("result") # 1 = repartition sur 1 partition
