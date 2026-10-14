**Nom de l'objet**

T5_PL_HRIS_ORCHESTRATOR

**Type d'objet**

Pipeline.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/QUOTIDIEN/PL_HRIS_ORCHESTRATOR.DataPipeline/pipeline-content.json, avec sa planification dans le fichier .schedules du même dossier.

**Description fonctionnelle**

Ce pipeline exécute chaque jour le transfert des salariés de Motul France de TalentSoft vers l'outil de paie ADP, ainsi que le calcul des changements de rémunération. Il remplace à lui seul le déclencheur quotidien, la fonction d'extraction, le fichier drapeau et les pipelines Synapse de l'ancien traitement Azure. Il est planifié chaque jour à six heures, heure de Paris, une heure volontairement différente de celle de l'existant. Il garantit qu'un seul run s'exécute à la fois et rend visibles dans le suivi de Fabric les échecs comme les blocages.

**Dépendances**

Le pipeline appelle les notebooks T5_NB_HRIS_RUN_CONTROL, T5_NB_HRIS_TS_ACQUIRE, T5_NB_HRIS_REMUNERATION, T5_NB_HRIS_TRANSFORM, T5_NB_HRIS_COMPARE, T5_NB_HRIS_PROMOTE et T5_NB_HRIS_PUBLISH_ADP du même workspace, désignés par leur identifiant logique. Les notebooks lisent eux-mêmes la variable library T5_VL_HRIS et chargent la bibliothèque T5_NB_HRIS_LIB. Le pipeline est appelé par la planification quotidienne et par le pipeline de premier chargement T5_PL_HRIS_BOOTSTRAP.

**Fonctionnement et logique de traitement**

L'activité RunStart ouvre le run et prend le verrou ; si le verrou est déjà tenu, la condition IfStarted est fausse et le pipeline se termine sans rien faire. Sinon, l'acquisition et le flux de rémunération démarrent en parallèle, puis la transformation, la comparaison, la promotion et la publication s'enchaînent. L'activité RunEnd clôture le run ; si le run est bloqué par un contrôle, la condition IfRunBlocked fait échouer le pipeline avec le code HRIS_RUN_BLOCKED. Tout échec ou toute étape sautée déclenche RunFail, qui marque le run en échec et libère le verrou, puis le pipeline échoue avec le code HRIS_RUN_FAILED. Un échec du seul flux de rémunération fait échouer le pipeline avec le code HRIS_REMUNERATION_FAILED après la clôture du run.

**Paramètres**

Le paramètre p_process_code vaut TS_ADP_EMPLOYEE. Le paramètre p_run_mode vaut NORMAL pour l'exploitation courante, INIT_REFERENCE pour le premier chargement ou RETRY_PUBLICATION pour republier les éléments en échec d'un run déjà promu, désigné par p_target_run_id. Le paramètre p_business_date_override sert uniquement aux tests et il est refusé en mode NORMAL dans un workspace de production. Aucun paramètre ne permet de forcer une publication.

**Sorties produites**

Le pipeline produit, par ses notebooks, toutes les tables du run dans le Lakehouse T5_LH_HRIS, le journal de fonctionnement de chaque notebook dans log.notebook_log et, en production avec les interrupteurs actifs, les publications vers ADP. Son statut dans Fabric est en échec pour un run en échec ou bloqué, et réussi sinon.

**Limitations connues et points d'attention**

La planification est désactivée dans le dépôt : elle s'active dans le workspace de production après le premier chargement, et ne doit jamais être activée dans le dépôt, car elle le serait alors dans chaque workspace synchronisé. Les délais maximaux cumulés des activités atteignent neuf heures quarante, ce que couvre le bail du verrou de six cents minutes. Toutes les activités ont zéro reprise automatique : les reprises passent par les modes du run.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
