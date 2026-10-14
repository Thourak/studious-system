**Nom de l'objet**

T5_NB_HRIS_RUN_CONTROL

**Type d'objet**

Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/QUOTIDIEN/NB_HRIS_RUN_CONTROL.Notebook/notebook-content.py

**Description fonctionnelle**

Ce notebook ouvre, clôture ou déclare en échec un run du hub qui transmet les salariés de Motul France de TalentSoft vers l'outil de paie ADP. À l'ouverture, il prend un verrou pour qu'un seul run s'exécute à la fois et enregistre le run dans le registre des runs avec sa date métier. À la clôture, il calcule le bilan chiffré du run, libère le verrou et renvoie le statut final au pipeline, qui échoue explicitement lorsqu'un contrôle a bloqué le run. En cas d'échec d'une étape, il marque le run en échec et libère le verrou.

**Dépendances**

Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules de contrôle du run, de modèle de données et de journal. Il lit la variable library T5_VL_HRIS pour le fuseau de la date métier et la durée du bail du verrou. Il lit et écrit les tables ctl.run, ctl.run_step et ctl.process_lock, écrit log.run_summary à la clôture et log.notebook_log pour son journal de fonctionnement. Il est appelé par le pipeline T5_PL_HRIS_ORCHESTRATOR dans les activités RunStart, RunEnd, RunFail et RunEndFail.

**Fonctionnement et logique de traitement**

La configuration lit le contexte réel du notebook et la variable library, puis la cellule du Lakehouse crée les schémas s'ils manquent et dirige le journal vers la table log.notebook_log. En mode START, le notebook calcule la date métier à partir de l'heure de déclenchement et du fuseau, prend le verrou, puis enregistre le run ; si le verrou est déjà tenu par un autre run dont le bail n'a pas expiré, il renvoie le statut SKIPPED_CONCURRENT et le pipeline s'arrête sans rien faire. En mode END, il détermine le statut final, calcule le bilan dans le format de l'existant, enregistre la fin du run et libère le verrou. En mode FAIL, il enregistre l'échec avec un message expurgé et libère le verrou.

**Paramètres**

Le paramètre p_mode vaut START, END ou FAIL et choisit l'action. Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire. Le paramètre p_process_code identifie le processus et vaut TS_ADP_EMPLOYEE. Le paramètre p_run_mode vaut NORMAL pour l'exploitation courante, INIT_REFERENCE pour le premier chargement ou RETRY_PUBLICATION pour republier un run déjà promu, dont l'identifiant est alors donné par p_target_run_id. Le paramètre p_trigger_time reçoit l'heure de déclenchement du pipeline en temps universel. Le paramètre p_business_date_override force la date métier pour les tests et il est refusé en mode NORMAL dans un workspace de production. Le paramètre p_pipeline_name est enregistré pour information, p_error_message porte le message d'échec en mode FAIL, p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.

**Sorties produites**

Le notebook écrit le registre ctl.run, le journal des étapes ctl.run_step, le verrou ctl.process_lock, le bilan log.run_summary et son journal de fonctionnement log.notebook_log. Il renvoie au pipeline un résultat court qui contient le statut du run et des compteurs, sans aucune donnée personnelle ; le pipeline lit ce statut pour décider de la suite.

**Limitations connues et points d'attention**

Le bail du verrou doit rester supérieur à la durée possible d'un run ; il est fixé à six cents minutes dans la variable library. Un verrou orphelin ne doit jamais être supprimé à la main : il faut attendre l'expiration de son bail. Le bilan reprend les colonnes de l'existant, mais il compte les salariés de la population comparée par Fabric et non ceux du delta brut de l'existant.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
