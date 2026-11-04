**Nom de l'objet**

T6_NB_HRIS_REFERENCE_IMPORT

**Type d'objet**

Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.

**Chemin dans le dépôt**

TEST_MOTUL/TST_6/ONESHOT/NB_HRIS_REFERENCE_IMPORT.Notebook/notebook-content.py

**Description fonctionnelle**

Ce notebook reprend une seule fois, dans les tables de correspondance du Lakehouse, le contenu des dix-sept fichiers de référentiels de l'existant (classes, classification, contrat, contrat_v2, convention_collective, cost_center, diplome, entite, location_country, nationalite, organisationalstructure, pays, raison_debut_contrat, sexe, situation_matrimoniale, titre, work_accident). Ces fichiers portent les codes ADP choisis par le métier, qui n'existent nulle part ailleurs. Le notebook lit les fichiers que l'utilisateur a déposés à la main dans la zone Files du Lakehouse, contrôle leur contenu, puis renseigne les codes ADP des tables cfg.lu_hr_ et ajoute les lignes qui n'existent pas encore. Il passe par les mêmes contrôles que la saisie manuelle de T6_NB_HRIS_REFERENCE_ADMIN. Il sert à la mise en service de chaque environnement ; les fichiers déposés sont supprimés ensuite.

**Dépendances**

Le notebook charge la bibliothèque commune par %run T6_NB_HRIS_LIB et utilise ses modules de correspondances, d'écriture dans les tables, de contrôle du run, de modèle de données et de journal. Il lit les fichiers du dossier indiqué par p_folder dans la zone Files du Lakehouse T6_LH_HRIS, lit et écrit les tables cfg.lu_hr_, lit le verrou ctl.process_lock et trace l'opération dans ctl.run_step. Il est lancé à la main par le développeur ; il n'est appelé par aucun pipeline.

**Fonctionnement et logique de traitement**

Après la configuration et la préparation du Lakehouse, le notebook contrôle les paramètres, puis lit tous les fichiers CSV du dossier. Chaque fichier est décodé en UTF-8, sinon en Windows-1252, et son séparateur (virgule, point-virgule ou tabulation) est reconnu sur la ligne d'en-têtes. Les colonnes sont lues par position, comme le font les tables externes de l'existant, car les en-têtes des fichiers diffèrent du modèle pour trois d'entre eux. Le nombre de colonnes doit être celui de la table. Un code TalentSoft en double avec des valeurs différentes est une erreur, sauf si p_conflict_policy demande de garder la première ou la dernière occurrence. Pour un code déjà présent dans la table, le notebook renseigne les colonnes métier du fichier, sans jamais remplacer une valeur déjà saisie, sauf si p_overwrite vaut true ; la partie TalentSoft de la table n'est pas modifiée. Un code absent de la table est ajouté avec les valeurs du fichier, s'il porte au moins une valeur métier ; l'acquisition mettra ensuite à jour ses libellés TalentSoft sans jamais toucher aux colonnes ADP. En mode CHECK, rien n'est écrit et chaque changement est affiché. En mode APPLY, le notebook refuse d'écrire si un run tient le verrou ; une seule erreur, sur n'importe quelle table, fait tout refuser.

**Paramètres**

Le paramètre p_mode vaut CHECK par défaut ou APPLY. Le paramètre p_folder désigne le dossier des fichiers dans la zone Files du Lakehouse et vaut Files/reference_import par défaut. Le paramètre p_table limite l'import à une table de correspondance, par exemple lu_hr_emp_titre, et reste vide pour traiter tous les fichiers présents. Le paramètre p_overwrite vaut false par défaut : une valeur déjà saisie est conservée. Le paramètre p_conflict_policy vaut ERROR par défaut, FIRST ou LAST pour trancher les codes en double. Le paramètre p_change_ref porte la référence du changement et il est obligatoire en mode APPLY. Le paramètre p_max_lines limite le nombre de changements affichés par table, p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.

**Sorties produites**

En mode APPLY, le notebook remplace les tables de correspondance modifiées, chaque écriture créant une version Delta qui permet un retour arrière, et ajoute une trace dans ctl.run_step. Dans tous les modes, il écrit son journal dans log.notebook_log et renvoie un résultat court de compteurs par table, avec les fichiers absents ou inattendus.

**Limitations connues et points d'attention**

Les fichiers sont des données métier déposées dans la zone Files du Lakehouse : ils doivent y rester le temps de l'import seulement, puis être supprimés. La lecture des fichiers par Spark en mode binaire n'a pas encore été essayée dans Fabric ; en cas d'échec, le notebook retombe sur une lecture en texte UTF-8. Le nom du workspace est utilisé dans le chemin OneLake, comme dans les autres notebooks du projet. Une clé de situation matrimoniale porte deux codes ADP différents dans l'existant : le métier doit choisir le bon code, dans le fichier ou par p_conflict_policy. Un import rejoué ne remplace jamais une valeur saisie depuis ; p_overwrite à true est réservé à une reprise volontaire.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
