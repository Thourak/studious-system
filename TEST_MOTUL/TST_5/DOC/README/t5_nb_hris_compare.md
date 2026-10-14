**Nom de l'objet**

T5_NB_HRIS_COMPARE

**Type d'objet**

Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/QUOTIDIEN/NB_HRIS_COMPARE.Notebook/notebook-content.py

**Description fonctionnelle**

Ce notebook décide quels salariés doivent être transmis à l'outil de paie ADP. Il compare la sortie au format de la paie du run à la référence, qui est l'état validé du dernier run promu, et classe chaque salarié comme nouveau, modifié, inchangé, absent ou rejeté. Ce choix, approuvé par le projet, remplace la comparaison du fichier brut du jour à celui de la veille de l'existant : la population transmise est la même dans le cas courant et plus juste quand une correspondance change ou qu'un rejet est corrigé. Il contrôle aussi la cohérence du run et décide si le run est validé ou bloqué.

**Dépendances**

Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules d'étapes, de comparaison, de modèle de données et de journal. Il lit ctl.run, ctl.run_step, cfg.process pour les seuils facultatifs, stg.ts_employee_adp du run et la référence ref.ts_employee_adp. Il est appelé par le pipeline T5_PL_HRIS_ORCHESTRATOR dans l'activité Compare, après une transformation réussie.

**Fonctionnement et logique de traitement**

Après la configuration et la préparation du Lakehouse, la comparaison vérifie que le mode demandé est celui enregistré pour le run. En mode NORMAL, une référence vide bloque le run, car le premier chargement doit passer par le mode INIT_REFERENCE. Elle contrôle l'absence de clé vide ou en double, calcule les catégories de chaque salarié sur les colonnes comparées, écrit le détail des colonnes changées sans leurs valeurs et le résumé, puis décide VALIDATED ou BLOCKED. Les seuils de taux ne sont pas renseignés, comme dans l'existant : ils sont calculés et journalisés sans jamais bloquer.

**Paramètres**

Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire. Le paramètre p_run_mode vaut NORMAL, INIT_REFERENCE ou RETRY_PUBLICATION et doit être identique au mode enregistré par l'ouverture du run ; en RETRY_PUBLICATION, la comparaison n'est pas recalculée. Le paramètre p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.

**Sorties produites**

Le notebook écrit log.comparison_summary, log.comparison_detail, qui contient les clés des salariés et reste en accès restreint, log.anomaly, ctl.run_step, ctl.run et son journal log.notebook_log. Il renvoie au pipeline un résultat court de compteurs et la décision.

**Limitations connues et points d'attention**

Un run bloqué laisse la référence intacte ; il suffit de corriger la cause puis de relancer. Le pipeline échoue explicitement sur un run bloqué, à la clôture, pour que le blocage soit visible dans le suivi de Fabric.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
