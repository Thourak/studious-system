**Nom de l'objet**

T5_NB_HRIS_TS_ACQUIRE

**Type d'objet**

Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/QUOTIDIEN/NB_HRIS_TS_ACQUIRE.Notebook/notebook-content.py

**Description fonctionnelle**

Ce notebook extrait tous les salariés de la société Motul France depuis le tenant de production de TalentSoft, l'outil de gestion des ressources humaines, et les dépose dans le Lakehouse pour le run en cours. Il reprend les règles de l'ancienne fonction Azure d'extraction : même population, même construction des soixante-neuf champs, mêmes valeurs par défaut. Contrairement à l'existant, il vérifie que l'extraction est complète et bloque le run si elle ne l'est pas, plutôt que de transmettre une liste partielle. Il met aussi à jour la partie TalentSoft des tables de correspondance à partir des référentiels de TalentSoft.

**Dépendances**

Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules d'acquisition, d'étapes, d'écriture dans les tables, de correspondances et de journal. Il lit dans la variable library T5_VL_HRIS l'adresse du tenant TalentSoft, l'adresse du coffre de secrets et les niveaux de parallélisme. Il lit les secrets client-id-ts et client-secret-ts dans le coffre avec l'identité d'exécution du notebook. Il appelle l'interface de programmation de TalentSoft en HTTPS. Il lit ctl.run, écrit stg.ts_employee_raw, log.anomaly, ctl.run_step, la partie TalentSoft des tables cfg.lu_hr_ et log.notebook_log. Il est appelé par le pipeline T5_PL_HRIS_ORCHESTRATOR dans l'activité Acquire.

**Fonctionnement et logique de traitement**

Après la configuration et la préparation du Lakehouse, le pré-contrôle vérifie que l'adresse est bien celle du tenant de production, lit les deux secrets dans le coffre et teste la connexion réseau, sans rien afficher de secret. L'acquisition lit ensuite le run ; si le run n'est pas à l'état ouvert ou s'il s'agit d'une republication, elle ne fait rien. Sinon, elle extrait les salariés avec un parallélisme borné, contrôle la complétude par rapport au nombre total annoncé par TalentSoft, écrit les données brutes et les contrôles, puis marque le run comme acquis. Enfin, la synchronisation du catalogue met à jour les codes et libellés TalentSoft des tables de correspondance ; elle n'écrit jamais de code ADP et son échec ne bloque pas le run.

**Paramètres**

Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire, sauf en pré-contrôle seul. Le paramètre p_preflight_only vaut false par défaut ; à true, le notebook vérifie seulement la source, les secrets, le réseau et la destination, sans appeler TalentSoft. Le paramètre p_sync_reference_catalogue vaut true par défaut et active la mise à jour de la partie TalentSoft des correspondances. Le paramètre p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.

**Sorties produites**

Le notebook écrit les salariés bruts du run dans stg.ts_employee_raw, qui contient des données personnelles et reste en accès restreint, les contrôles de complétude dans log.anomaly, la trace de l'étape dans ctl.run_step, les codes TalentSoft dans les tables cfg.lu_hr_ et son journal dans log.notebook_log. Il renvoie au pipeline un résultat court de compteurs.

**Limitations connues et points d'attention**

Le niveau de parallélisme est de dix requêtes et dix salariés simultanés, comme l'existant ; l'augmenter suppose l'accord de TalentSoft, faute de quoi des limitations de débit peuvent apparaître. Le coffre de secrets est dans le tenant Motul : il n'est lisible que depuis un workspace de ce tenant, avec une identité autorisée. Les messages d'erreur et les compteurs ne contiennent aucune valeur de salarié.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
