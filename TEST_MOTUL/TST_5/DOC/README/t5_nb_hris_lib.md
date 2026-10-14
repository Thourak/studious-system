**Nom de l'objet**

T5_NB_HRIS_LIB

**Type d'objet**

Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark. Il joue pour ce projet le rôle du notebook partagé que les règles de développement appellent nb_shareddef : chaque notebook du projet le charge par la commande %run T5_NB_HRIS_LIB en troisième cellule.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/QUOTIDIEN/NB_HRIS_LIB.Notebook/notebook-content.py

**Description fonctionnelle**

Ce notebook contient toute la logique métier du hub qui transmet les salariés de Motul France de l'outil de gestion des ressources humaines TalentSoft vers l'outil de paie ADP. Il reprend à l'identique les règles de l'ancien traitement Azure : lecture des salariés, mise en forme pour la paie, contrôles, rejets, calcul des changements et publication vers ADP. Il fournit aussi les fonctions génériques communes à tous les notebooks : création du journal de fonctionnement, enregistrement de ce journal dans le Lakehouse, lecture des secrets dans le coffre, lecture des paramètres d'environnement et lecture des paramètres transmis par un événement.

**Dépendances**

Le notebook ne dépend d'aucun autre notebook. Les modules qu'il charge utilisent les bibliothèques fournies par l'environnement d'exécution Fabric version 1.3 : pyspark et delta pour les tables, requests pour les appels aux interfaces de programmation, pandas et openpyxl pour les classeurs, python-dateutil pour les dates. Les fonctions génériques utilisent les objets notebookutils et spark du notebook appelant, ainsi que la variable library T5_VL_HRIS, dont le nom est un objet d'infrastructure identique dans tous les environnements.

**Fonctionnement et logique de traitement**

La troisième cellule définit la fonction d'enregistrement d'un module : elle calcule l'empreinte numérique du texte du module, la compare à l'empreinte attendue écrite dans la cellule, puis exécute le module et le range dans le paquet hris. Une empreinte différente arrête immédiatement le chargement, ce qui garantit que le code exécuté est celui qui a été relu et testé. Chaque module occupe ensuite sa propre cellule, dans l'ordre de leurs dépendances. Les cellules suivantes définissent, une par cellule, les fonctions génériques appelées par les notebooks. La dernière cellule rend le paquet hris disponible sous ce nom et affiche le nombre de modules chargés.

**Paramètres**

Ce notebook n'a pas de paramètre : il ne fait que définir du code dans l'espace du notebook qui le charge.

**Sorties produites**

Le notebook ne lit ni n'écrit aucune table. Il laisse dans l'espace du notebook appelant le paquet hris, la fonction register_hris_module et les fonctions setup_fabric_logger, flush_logger, get_parameter, initialize_notebook_context, parse_eventpath, tables_for_run, create_lakehouse_schemas et attach_logger_to_lakehouse.

**Limitations connues et points d'attention**

Le chargement par %run d'un notebook rangé dans un autre dossier du workspace n'est pas décrit par la documentation de Microsoft : il doit être vérifié au premier lancement, ce que fait le pipeline de premier chargement dès sa première activité. Conformément à la règle propre au notebook partagé, ce notebook n'utilise pas de logger : ses diagnostics passent par la sortie standard. Les modules suivent les conventions de nommage du langage Python, et non la règle de nommage des variables de notebook, pour ne pas modifier du code testé et protégé par empreinte ; cette dérogation est documentée dans le fichier README de TST_5.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
