# Desafio Semantix Engenheiro de Dados

## Questões Teóricas

### Questão 1
Resposta: Força o RDD a ser processado na memória RAM.

### Questão 2
Resposta: O Spark utiliza o máximo de memória que pode para processamento dos dados, só utilizando o disco quando não há mais espaço em memória. Já ferramentas que utilizam o paradigma padrão de MapReduce utilizam o disco para armazernar os resultados dos processamentos, independentemente de haver espaço disponível na memória.

### Questão 3
Resposta: O SparkContext é o meio de instanciar e configurar o acesso ao Spark.

### Questão 4
Resposta: O RDD é o modelo principal de estrutura de dados que o Spark utiliza para realizar seus processamentos.

### Questão 5
Resposta: O reduceByKey economiza em tráfego de rede e uso de memória ao fazer a redução dos dados antes de distribuí-los aos clusters do Spark.

### Questão 6
Resposta: O código reproduz em linguagem Scala um algoritmo de contagem de palavras de um arquivo do sistema de arquivos do Hadoop.

## Questão prática
Foi utilizada linguagem Java com as bibliotecas do spark-core e spark-sql, demonstradas no arquivo pom.xml.

O output do console da execução do código foi gravado no arquivo output.txt.
