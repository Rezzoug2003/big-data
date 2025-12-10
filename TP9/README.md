# TP9 – Traitement par Lot et Streaming avec Apache Spark

## 📌 Objectifs
- Manipuler Apache Spark en **Batch** et **Streaming**
- Déployer Spark sur un cluster Hadoop via **Docker**
- Exécuter des applications Spark en modes **local** et **YARN**

---

## 🛠️ Technologies utilisées
- Apache Hadoop 3.2.1
- Apache Spark 3.5.0
- Docker
- Java 8
- Maven
- YARN
- Netcat

---

## ⚙️ Architecture du cluster
- 1 NameNode / ResourceManager : `hadoop-master`
- 2 DataNodes : `hadoop-worker1`, `hadoop-worker2`

Vérification des conteneurs :
```bash
docker ps
```

---

## 🔹 Phase 1 : Démarrage des services Hadoop

```bash
docker exec -it hadoop-master bash
hdfs --daemon start namenode
hdfs --daemon start datanode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager
jps
```

---

## 🔹 Phase 2 : Spark Batch avec Spark Shell (Scala)

### Création du fichier texte
```bash
echo "Hello Spark Wordcount! Hello Hadoop Also :)" > file1.txt
```

### Lancement de Spark Shell
```bash
spark-shell
```

### WordCount en Scala
```scala
val lines = sc.textFile("file1.txt")
val words = lines.flatMap(_.split("\\s+"))
val wc = words.map(w => (w, 1)).reduceByKey(_ + _)
wc.saveAsTextFile("file1.count")
```

### Résultats
```bash
hdfs dfs -cat file1.count/part-00000
hdfs dfs -cat file1.count/part-00001
```

---

## 🔹 Phase 3 : Spark Batch en Java

### Compilation Maven
```bash
mvn package
```

### Copie du JAR vers le cluster
```bash
docker cp target/wordcount-spark-1.0-SNAPSHOT.jar hadoop-master:/root/wordcount-spark.jar
```

### Préparation des données
```bash
hdfs dfs -mkdir -p input
echo "apple banana apple orange banana apple" > purchases.txt
hdfs dfs -put purchases.txt input/
```

### Exécution en mode local
```bash
spark-submit --class spark.batch.tp21.WordCountTask --master local /root/wordcount-spark.jar input/purchases.txt out-spark
```

### Exécution en mode YARN (cluster)
```bash
spark-submit --class spark.batch.tp21.WordCountTask --master yarn --deploy-mode cluster /root/wordcount-spark.jar input/purchases.txt out-spark2
```

### Résultats Batch
```bash
hdfs dfs -cat out-spark/part-00000
hdfs dfs -cat out-spark2/part-00000
```

---

## 🔹 Phase 4 : Spark Streaming

### Installation de Netcat
```bash
apt-get update && apt-get install -y netcat-openbsd
```

### Génération et copie du JAR Streaming
```bash
mvn package
docker cp target/wordcount-spark-1.0-SNAPSHOT.jar hadoop-master:/root/stream-1.jar
```

### Terminal 1 : Générateur de flux
```bash
nc -lk 9999
```

Exemple :
```
hello spark hello world
```

### Terminal 2 : Récepteur Spark Streaming
```bash
spark-submit --class spark.streaming.tp22.Stream --master local /root/stream-1.jar
```

### Résultat Streaming
```
(hello,2)
(spark,1)
(world,1)
```

---

## ✅ Conclusion
- Spark Batch fonctionne en Scala et Java
- Spark Streaming traite les données en temps réel
- Exécution réussie en mode local et YARN

---

## 👤 Auteur
**Bilal Rezzoug**  
Université d’El Oued  
Année universitaire : 2025 / 2026
