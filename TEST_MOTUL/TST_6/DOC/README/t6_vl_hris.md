**Nom de l'objet**

T6_VL_HRIS

**Type d'objet**

VariableLibrary.

**Chemin dans le dépôt**

TEST_MOTUL/TST_6/QUOTIDIEN/VL_HRIS.VariableLibrary, avec les fichiers variables.json, settings.json et le dossier valueSets.

**Description fonctionnelle**

Cette variable library porte les paramètres d'environnement du hub qui ne sont pas des secrets : nom du Lakehouse, adresses des coffres de secrets, adresse du tenant TalentSoft, adresses d'ADP et de l'interface des communes, niveaux de parallélisme, durée du bail du verrou, interrupteurs de la publication ADP, paramètres du premier chargement et, depuis TST_6, trois paramètres de la notification de fin de run. Elle contient trois jeux de valeurs : DEV, qui est le jeu par défaut, UAT pour la recette et PRD pour la production. Les notebooks la lisent eux-mêmes à chaque exécution ; aucune valeur d'environnement n'est écrite dans le code. Son usage est autorisé dans ce projet à titre d'exception justifiée : les tables d'un Lakehouse ne sont ni versionnées dans Git ni déployées, alors que les fichiers de la variable library le sont, et elle est lisible à la fois par les notebooks et par les pipelines.

**Dépendances**

Elle est lue par tous les notebooks du projet par la fonction initialize_notebook_context de la bibliothèque T6_NB_HRIS_LIB, dont la liste de variables doit rester identique à celle de ce fichier, et par le pipeline T6_PL_HRIS_BOOTSTRAP pour la variable bootstrap_run_tests.

**Fonctionnement et logique de traitement**

Le jeu de valeurs actif est un réglage du workspace, qui n'est enregistré ni dans le dépôt ni dans un déploiement : après la première synchronisation d'un workspace, c'est le jeu par défaut, DEV, qui est actif ; dans le workspace de recette il faut activer UAT, et dans celui de production il faut activer PRD, puis enregistrer la variable library avant de lancer quoi que ce soit. Le jeu PRD ne remplace que l'environnement, le coffre ADP et la campagne de tests du premier chargement ; les autres valeurs viennent du jeu par défaut.

**Paramètres**

Les variables sont toutes de type texte, car le type nombre n'est pas utilisable dans les pipelines. La variable adp_read_enabled active la lecture d'ADP et la variable adp_write_enabled l'écriture ; l'écriture exige la lecture, et aucune des deux ne remplace la garde sur le nom du workspace. Les deux valent false dans tous les jeux : leur passage à true en production se fait par un commit tracé du jeu PRD, d'abord pour la lecture seule pendant la non-régression, puis pour l'écriture après la bascule. Les variables notify_enabled (false par défaut), notify_recipients (vide par défaut, de préférence une liste de diffusion) et notify_subject_prefix commandent la notification de fin de run ; l'étiquette d'environnement ([DEV], [UAT] ou [PRD]) est ajoutée automatiquement en tête de l'objet par T6_NB_HRIS_NOTIFY. Les notes de chaque variable, dans le fichier variables.json, en précisent le rôle. Aucune variable ne doit contenir de secret.

**Sorties produites**

Aucune : la variable library ne produit que des valeurs lues par les autres objets.

**Limitations connues et points d'attention**

Le nom du workspace de recette ne doit pas contenir la suite de lettres PRD, sans quoi la garde de publication le considérerait comme la production. Changer le jeu actif ne se voit pas dans le dépôt : la mise en service doit le vérifier et le consigner. Les fonctions de lecture de variable library des notebooks ne prennent pas en charge un principal de service, et l'identité d'un notebook lancé par un pipeline n'est pas documentée : à vérifier au premier run planifié. Les variables d'un ancien import de l'historique des journaux ADP (compte de stockage externe) ont été retirées en TST_6.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
