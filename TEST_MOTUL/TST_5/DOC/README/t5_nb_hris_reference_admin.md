**Nom de l'objet**

T5_NB_HRIS_REFERENCE_ADMIN

**Type d'objet**

Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/ONESHOT/NB_HRIS_REFERENCE_ADMIN.Notebook/notebook-content.py

**Description fonctionnelle**

Ce notebook permet de saisir, de façon contrôlée et tracée, les codes ADP des tables de correspondance entre les codes de TalentSoft et ceux de l'outil de paie ADP. Ces tables remplacent les dix-sept fichiers de référentiels de l'existant : aucun fichier n'est lu. Leur partie TalentSoft est mise à jour automatiquement par l'acquisition ; leur partie ADP n'existe pas dans TalentSoft et se saisit ici. Le notebook affiche d'abord les codes TalentSoft sans code ADP, les plus employés en premier, sous la forme d'une grille prête à compléter, puis contrôle et applique la saisie.

**Dépendances**

Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules de correspondances, d'écriture dans les tables, de contrôle du run, de modèle de données et de journal. Il lit et écrit les tables cfg.lu_hr_, lit la dernière extraction stg.ts_employee_raw pour compter l'usage des codes et lit le verrou ctl.process_lock. Il est lancé à la main par le métier ou le développeur ; il n'est appelé par aucun pipeline.

**Fonctionnement et logique de traitement**

Après la configuration et la préparation du Lakehouse, le notebook contrôle les paramètres. En mode REPORT, il affiche la grille des codes sans code ADP. En mode CHECK, il lit la grille collée ou la saisie en format JSON, rapproche chaque code de la table, applique la saisie en mémoire et affiche chaque changement, sans rien écrire. En mode APPLY, il refuse d'écrire si un run tient le verrou, pour ne pas croiser la mise à jour des codes TalentSoft faite par l'acquisition ; sinon il écrit la saisie, table par table, avec la référence du changement, et trace l'opération dans ctl.run_step. Une seule erreur, sur n'importe quelle table, fait tout refuser.

**Paramètres**

Le paramètre p_mode vaut REPORT par défaut, CHECK ou APPLY. Le paramètre p_table désigne une table de correspondance, ou reste vide pour traiter les treize tables utilisées ou les tables nommées dans la grille. Le paramètre p_grid reçoit une grille collée, avec ses en-têtes, depuis un tableur ou le résultat d'une requête sur les tables de l'existant ; une cellule vide laisse la valeur inchangée. Le paramètre p_assignments reçoit, à la place de la grille, une saisie en format JSON pour une seule table. Le paramètre p_change_ref porte la référence du changement et il est obligatoire en mode APPLY. Le paramètre p_run_id choisit l'extraction utilisée pour compter les codes, p_max_lines limite le nombre de codes affichés, p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.

**Sorties produites**

En mode APPLY, le notebook remplace les tables de correspondance modifiées, chaque écriture créant une version qui permet un retour arrière, et ajoute une trace dans ctl.run_step. Dans tous les modes, il écrit son journal dans log.notebook_log et renvoie un résultat court de compteurs.

**Limitations connues et points d'attention**

Pour reprendre les codes ADP de l'existant, il faut coller le résultat des requêtes sur les tables externes de Synapse, dont les colonnes portent les noms du modèle : les fichiers de référentiels de l'existant ont des en-têtes différents pour les nationalités, les pays et les entités, et une grille collée telle quelle est refusée. Une clé de situation matrimoniale porte deux codes ADP différents dans l'existant : le métier doit choisir le bon code avant la saisie. Après APPLY, il faut vider p_grid pour ne pas enregistrer la grille dans le contrôle de source.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
