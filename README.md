# Projet_Spark_Ms_SIO
Pour réaliser cette exercice j'ai procéder de la manière suivante :
  1. J'ai mis en place l'environnement spark on se basant sur l'image docker (j'ai monté le docker sur un dossier contenant déja les deux datasets)
  
  2. J'ai chargé les deux datasets dans des dataframes pour ingerer et manipuler les données.
  
  3. J'ai commencé par faire un printSchema , un count, et un show pour découvrir les datasset et reperer la corélation entre les deux.
  
  4. J'ai ensuite selectionné les colonnes 'Code établissement','Code état établissement','Code postal','Secteur Public/Privé' du dataset des ecoles, que j'ai ensuite renommé pour enlever les accents, espaces et caractères spéciaux.
  j'ai essayé de selectionner que des features codés sous format integer pour faciliter la tache aux datascientists. 
  
  5. A partir du dataset des logements j'ai selectionné les 3 colonnes 'id_mutation','valeur_fonciere','code_type_local'(
      la colonne du code_type_local nous permet de savoir le type du local tout étant codé sous forme integer, ceci facilitera letravail pour  les datascientists)
      j'ai ajouté à cette selection une clause where pour avoir que les logements des particuliers.
  
   6. J'ai enfin fait un inner join pour produire un dataset qui va servir aux datascientists pour faire des prédictions en pranant en compte                 la corélation entre les prix des logements et la présence des ecoles.  
   
    
  
