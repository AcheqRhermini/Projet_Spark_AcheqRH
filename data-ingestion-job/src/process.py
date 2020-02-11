# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse


def main():

    parser = argparse.ArgumentParser(
        description='Discover driving sessions into log files.')
    parser.add_argument('-v', "--dvf_file", help='Videos input file', required=True)
    parser.add_argument('-c', "--age_file", help='Categories input file', required=True)
    parser.add_argument('-o', '--output', help='Output file', required=True)

    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    process(spark,args.dvf_file,args.age_file,args.output)


def process(spark, dvf_file, age_file, output):
    dvf = spark.read.option('header','true').option('inferSchema','true').csv(dvf_file)
    age = spark.read.option('header','true').option('delimiter',';').option('inferSchema','true').csv(age_file)

    age = age.withColumnRenamed('Code postal','code_postal_etablissement')
    age = age.withColumnRenamed('Code établissement','Code_etablissement')
    age = age.withColumnRenamed('Code état établissement','Code_etat_etablissement')
    age = age.withColumnRenamed('Secteur Public/Privé','Secteur_Public_Prive')

    age1 = age.select('Code_etablissement','Code_etat_etablissement','code_postal_etablissement','Secteur_Public_Prive')
    
    age1 = age1.na.drop()

    dvf1 = dvf.select('id_mutation','valeur_fonciere','code_type_local','code_postal').where((dvf.code_type_local == '1') | (dvf.code_type_local =='2'))
    
    dvf1 = dvf1.na.drop()

    joinedD = dvf1.join(age1, dvf1.code_postal == age1.code_postal_etablissement)
    df_final = joinedD.select('id_mutation','valeur_fonciere','code_type_local','code_postal','Code_etablissement','Code_etat_etablissement','Secteur_Public_Prive')
    df_final.write.csv(output)
    #df_final.write.parquet(output)

if __name__ == '__main__':
    main()
