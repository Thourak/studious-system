**Nom de l'objet**

T5_NB_HRIS_REMUNERATION

**Type d'objet**

Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/QUOTIDIEN/NB_HRIS_REMUNERATION.Notebook/notebook-content.py

**Description fonctionnelle**

Ce notebook reprend le flux de rémunération de l'ancien traitement Azure, qui comparait chaque jour les salaires de base et les primes cibles des salariés à ceux de la veille. Il lit l'export de rémunération de TalentSoft par son interface de programmation, sans aucun fichier, le met au format des exports de l'existant, calcule les changements avec les règles exactes des deux vues de l'existant et les conserve en table. Conformément à la décision du projet, aucun changement n'est renvoyé vers TalentSoft : l'ancien dépôt de fichiers sur le serveur de transfert est abandonné. L'instantané du jour devient la référence du run suivant, ce qui reproduit la comparaison quotidienne entre le jour et la veille.

**Dépendances**

Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules de rémunération, d'acquisition, d'écriture dans les tables et de journal. Il lit dans la variable library T5_VL_HRIS l'activation du flux, l'adresse du tenant TalentSoft et l'adresse du coffre. Il lit les secrets client-id-ts et client-secret-ts dans le coffre. Il lit ctl.run et cfg.ts_remuneration_mapping, lit et écrit ref.ts_remuneration_snapshot, et écrit stg.ts_remuneration_snapshot, pub.ts_remuneration_change, log.ts_remuneration_computation, log.anomaly, ctl.run_step et log.notebook_log. Il est appelé par le pipeline T5_PL_HRIS_ORCHESTRATOR dans l'activité Remuneration, en parallèle de l'acquisition des salariés.

**Fonctionnement et logique de traitement**

Après la configuration et la préparation du Lakehouse, le pré-contrôle vérifie le run, la source de production, les secrets et le réseau. En mode RUN, le notebook demande l'export de rémunération à TalentSoft, attend sa production, le lit en mémoire, construit l'instantané selon la table de correspondance, calcule les changements de salaire de base et de prime par rapport à la référence, enregistre l'instantané, les changements et leur suivi, puis fait avancer la référence. Au premier run, la référence est seulement initialisée. En mode VALIDATE, il ne modifie rien : il compare la structure du contenu qu'il produirait à celle du dernier import accepté par TalentSoft dans l'existant, pour valider les hypothèses de correspondance.

**Paramètres**

Le paramètre p_mode vaut RUN pour le flux quotidien ou VALIDATE pour la vérification de structure. Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire en mode RUN. Le paramètre p_only_active_employees vaut false par défaut, comme l'existant, et limiterait l'export aux salariés actifs. Le paramètre p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.

**Sorties produites**

Le notebook écrit l'instantané du run dans stg.ts_remuneration_snapshot, la référence dans ref.ts_remuneration_snapshot, les lignes de changement au format de l'existant dans pub.ts_remuneration_change, leur suivi sans valeur dans log.ts_remuneration_computation, un contrôle d'information dans log.anomaly et son journal dans log.notebook_log. Les trois premières tables contiennent des données personnelles et restent en accès restreint.

**Limitations connues et points d'attention**

La correspondance entre l'export de TalentSoft et le format de l'existant repose sur des hypothèses semées par l'installation : elles doivent être validées par le mode VALIDATE puis par la comparaison avec les archives de l'existant. L'export est demandé à six heures alors que l'existant lisait des exports produits avant deux heures : des changements saisis entre-temps apparaissent un jour plus tôt.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
