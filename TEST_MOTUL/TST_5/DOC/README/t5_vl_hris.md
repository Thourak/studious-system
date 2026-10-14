**Nom de l'objet**

T5_VL_HRIS

**Type d'objet**

VariableLibrary.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/QUOTIDIEN/VL_HRIS.VariableLibrary, avec les fichiers variables.json, settings.json et le dossier valueSets.

**Description fonctionnelle**

Cette variable library porte tous les paramètres d'environnement du hub, sans aucune valeur secrète : nom du Lakehouse, adresses des coffres de secrets, adresse du tenant TalentSoft, adresses d'ADP et de l'interface des communes, niveaux de parallélisme, durée du bail du verrou, interrupteurs de la publication ADP et paramètres de l'import historique. Elle contient trois jeux de valeurs : DEV, qui est le jeu par défaut, UAT pour la recette et PRD pour la production. Les notebooks la lisent eux-mêmes à chaque exécution ; aucune valeur d'environnement n'est écrite dans le code.

**Dépendances**

Elle est lue par tous les notebooks du projet par la fonction initialize_notebook_context de la bibliothèque T5_NB_HRIS_LIB, et par le pipeline T5_PL_HRIS_BOOTSTRAP pour la variable bootstrap_run_tests.

**Fonctionnement et logique de traitement**

Le jeu de valeurs actif est un réglage du workspace, qui n'est pas enregistré dans le dépôt : après la première synchronisation d'un workspace, c'est le jeu DEV qui est actif ; dans le workspace de recette il faut activer UAT, et dans celui de production il faut activer PRD, puis enregistrer la variable library avant de lancer quoi que ce soit. Le jeu PRD ne remplace que l'environnement, le coffre ADP, la campagne de tests du premier chargement et le compte de stockage de l'import historique ; les autres valeurs viennent du jeu par défaut.

**Paramètres**

Les variables sont toutes de type texte. La variable adp_read_enabled active la lecture d'ADP et la variable adp_write_enabled l'écriture ; l'écriture exige la lecture, et aucune des deux ne remplace la garde sur le nom du workspace. Les deux valent false dans tous les jeux : leur passage à true en production se fait par un commit tracé du jeu PRD, d'abord pour la lecture seule pendant la non-régression, puis pour l'écriture après la bascule. Les notes de chaque variable, dans le fichier variables.json, en précisent le rôle.

**Sorties produites**

Aucune : la variable library ne produit que des valeurs lues par les autres objets.

**Limitations connues et points d'attention**

Le nom du workspace de recette ne doit pas contenir la suite de lettres PRD, sans quoi la garde de publication le considérerait comme la production. Changer le jeu actif ne se voit pas dans le dépôt : la mise en service doit le vérifier et le consigner.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
