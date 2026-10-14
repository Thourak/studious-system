**Nom de l'objet**

T5_NB_HRIS_LEGACY_LOGS_IMPORT

**Type d'objet**

Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/ONESHOT/NB_HRIS_LEGACY_LOGS_IMPORT.Notebook/notebook-content.py

**Description fonctionnelle**

Ce notebook importe une seule fois, dans le Lakehouse, l'historique des journaux de publication de l'ancienne fonction Azure qui alimentait l'outil de paie ADP. Il assure la continuité de la traçabilité des envois vers ADP entre l'ancien traitement et le nouveau. Les journaux sont des fichiers produits par l'existant, un par jour, rangés dans le stockage Azure de l'existant ; ils sont lus en mémoire et aucun fichier n'est créé, modifié ni supprimé. C'est la seule lecture de fichiers du projet, décidée comme exception ponctuelle et hors du traitement quotidien.

**Dépendances**

Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules d'import des journaux historiques, d'écriture dans les tables et de journal. Il lit dans la variable library T5_VL_HRIS l'adresse du compte de stockage de l'existant, le nom du conteneur, le dossier des journaux et l'adresse du coffre. Il lit dans le coffre les trois secrets de l'application Azure autorisée à lire ce stockage : app-tenant-id, app-client-id et app-client-secret. Il utilise les bibliothèques Azure d'identité et de stockage. Il lit et écrit log.log_file_import, et écrit log.adp_publication_log et log.log_file_import_reject. Il est lancé à la main ; aucun pipeline ne l'appelle.

**Fonctionnement et logique de traitement**

Après la configuration et la préparation du Lakehouse, le notebook contrôle ses paramètres et la présence des valeurs de stockage. Il lit ensuite les trois secrets et construit l'identité de l'application Azure. Il liste les fichiers du dossier des journaux dans le conteneur, les lit en mémoire et les analyse : en-têtes attendus, séparateur, encodage, date tirée du dossier daté. Un fichier déjà importé est ignoré, ce qui rend l'import rejouable. Les lignes sans horodatage valide ou au nombre de colonnes incorrect sont rejetées et tracées sans valeur. En mode CHECK, le notebook affiche seulement les compteurs ; en mode APPLY, il écrit les lignes, les rejets et le registre des fichiers.

**Paramètres**

Le paramètre p_mode vaut CHECK par défaut, pour compter sans écrire, ou APPLY pour écrire. Le paramètre p_extensions liste les extensions de fichiers acceptées, séparées par des virgules, et vaut .csv par défaut. Le paramètre p_sheet_name désigne la feuille à lire dans un classeur, la première par défaut. Le paramètre p_change_ref porte la référence de la validation et il est obligatoire en mode APPLY. Le paramètre p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.

**Sorties produites**

En mode APPLY, le notebook écrit les lignes de journal dans log.adp_publication_log, qui contient des noms et des matricules et reste en accès restreint, les rejets sans valeur dans log.log_file_import_reject et le registre des fichiers dans log.log_file_import. Dans tous les modes, il écrit son journal de fonctionnement dans log.notebook_log et renvoie des compteurs.

**Limitations connues et points d'attention**

L'existant écrasait son journal à chaque run du jour : un seul journal par jour est disponible. Le compte de stockage de l'existant n'accepte que des accès privés : le workspace doit y avoir accès, par exemple par un point de terminaison privé géré, et l'application Azure doit avoir le droit de lecture des blobs. Les valeurs de stockage ne sont renseignées que dans le jeu de valeurs de production.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
