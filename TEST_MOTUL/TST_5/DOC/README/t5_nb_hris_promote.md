**Nom de l'objet**

T5_NB_HRIS_PROMOTE

**Type d'objet**

Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/QUOTIDIEN/NB_HRIS_PROMOTE.Notebook/notebook-content.py

**Description fonctionnelle**

Ce notebook fait d'un run validé la nouvelle référence et prépare la liste des salariés à transmettre à l'outil de paie ADP. La référence contient l'état validé de chaque salarié tel qu'il a été transmis ; elle sert de point de comparaison au run suivant. La liste à transmettre, appelée outbox, contient les salariés nouveaux ou modifiés du run, avec un statut qui permet de suivre leur publication. Au premier chargement, la référence est construite sans aucune liste à transmettre.

**Dépendances**

Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules d'étapes, de comparaison, de modèle de données et de journal. Il lit ctl.run, la décision de la comparaison, stg.ts_employee_adp et log.comparison_detail du run. Il est appelé par le pipeline T5_PL_HRIS_ORCHESTRATOR dans l'activité Promote, après une comparaison réussie.

**Fonctionnement et logique de traitement**

Après la configuration et la préparation du Lakehouse, la promotion vérifie le mode du run et ne fait rien pour une republication. Elle relit la décision du run : un run bloqué n'est jamais promu. Pour un run validé, elle met à jour la référence par fusion, en retirant les salariés absents et en conservant ceux qui sont rejetés, puis écrit la liste à transmettre avec le statut PENDING, sauf au premier chargement. Une promotion rejouée est sans effet.

**Paramètres**

Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire. Le paramètre p_run_mode vaut NORMAL, INIT_REFERENCE ou RETRY_PUBLICATION et doit être identique au mode enregistré. Le paramètre p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.

**Sorties produites**

Le notebook écrit la référence ref.ts_employee_adp et la liste pub.adp_outbox, qui contiennent des données personnelles et restent en accès restreint, ainsi que ctl.run, ctl.run_step et son journal log.notebook_log. Il renvoie au pipeline un résultat court de compteurs.

**Limitations connues et points d'attention**

La référence est remplacée à chaque promotion ; son historique reste disponible par le voyage dans le temps des tables Delta pendant trente jours. Une restauration ne doit être faite qu'avec une validation distincte.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
