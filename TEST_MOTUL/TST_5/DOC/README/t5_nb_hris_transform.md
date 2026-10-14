**Nom de l'objet**

T5_NB_HRIS_TRANSFORM

**Type d'objet**

Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/QUOTIDIEN/NB_HRIS_TRANSFORM.Notebook/notebook-content.py

**Description fonctionnelle**

Ce notebook met en forme pour l'outil de paie ADP les salariés extraits de TalentSoft pendant le run. Il reprend exactement les règles des procédures SQL de l'ancien traitement Azure : lecture des soixante-neuf champs avec les mêmes conversions et troncatures, quinze correspondances entre les codes TalentSoft et les codes ADP, rejets pour valeur absente, format incorrect ou code sans correspondance, puis construction des soixante-douze colonnes attendues par la paie. Un salarié rejeté n'est pas transmis. Comme dans l'existant, une catégorie professionnelle inconnue seule ne provoque pas de rejet.

**Dépendances**

Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules d'étapes, de transformation, de modèle de données et de journal. Il lit le registre ctl.run, les données brutes stg.ts_employee_raw du run, produites par T5_NB_HRIS_TS_ACQUIRE, et les dix-sept tables de correspondance cfg.lu_hr_. Il est appelé par le pipeline T5_PL_HRIS_ORCHESTRATOR dans l'activité Transform, après une acquisition réussie.

**Fonctionnement et logique de traitement**

Après la configuration et la préparation du Lakehouse, la transformation vérifie que le run est bien acquis, puis écrit la source typée, contrôle les tables de correspondance et s'arrête proprement si l'une d'elles est absente, vide ou contient une clé en double. Elle écrit ensuite les données enrichies par les correspondances, calcule les rejets, écrit une copie des rejets dont les valeurs sensibles sont masquées, construit la sortie au format de la paie pour les salariés non rejetés, puis marque le run comme transformé. Une erreur de conversion reproduite de l'existant arrête le run, comme l'existant.

**Paramètres**

Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire. Le paramètre p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.

**Sorties produites**

Le notebook écrit, pour le run, les tables stg.ts_employee_source, stg.ts_employee_enriched, stg.ts_employee_reject et stg.ts_employee_adp, qui contiennent des données personnelles et restent en accès restreint, ainsi que log.reject, log.anomaly, ctl.run_step et son journal log.notebook_log. Il renvoie au pipeline un résultat court de compteurs.

**Limitations connues et points d'attention**

Les correspondances doivent contenir les codes ADP saisis par T5_NB_HRIS_REFERENCE_ADMIN ; une table entièrement vide bloque le run. Les écarts éventuels entre les fonctions de conversion de SQL Server et leur émulation ne peuvent être mesurés que sur données réelles, pendant la non-régression.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
