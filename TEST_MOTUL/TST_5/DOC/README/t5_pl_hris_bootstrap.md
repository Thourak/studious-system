**Nom de l'objet**

T5_PL_HRIS_BOOTSTRAP

**Type d'objet**

Pipeline.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/ONESHOT/PL_HRIS_BOOTSTRAP.DataPipeline/pipeline-content.json

**Description fonctionnelle**

Ce pipeline réalise en une seule action le premier chargement du hub dans un workspace : installation du socle du Lakehouse, campagne de tests sur données fictives lorsque l'environnement le demande, run complet du pipeline quotidien dans le bon mode, puis indication précise de l'action suivante. Il est lancé à la main et n'est jamais planifié. Il peut être relancé autant de fois que nécessaire : il ne réinitialise jamais la référence des salariés.

**Dépendances**

Le pipeline appelle les notebooks T5_NB_HRIS_SETUP, T5_NB_HRIS_TESTS et T5_NB_HRIS_BOOTSTRAP et le pipeline T5_PL_HRIS_ORCHESTRATOR du même workspace, désignés par leur identifiant logique. Il lit la variable bootstrap_run_tests de la variable library T5_VL_HRIS, qui vaut true en développement et en recette et false en production.

**Fonctionnement et logique de traitement**

L'activité Setup installe le socle et le pipeline s'arrête si une table existante est incompatible avec le modèle. La campagne de tests s'exécute si le paramètre p_run_tests vaut true, ou s'il est vide et que la variable bootstrap_run_tests vaut true ; un test en échec arrête le pipeline avant tout run réel. L'activité Plan choisit INIT_REFERENCE si la référence est vide et NORMAL sinon, et arrête le pipeline si un run tient le verrou. Le pipeline quotidien s'exécute ensuite et attend sa fin, réussie ou non. L'activité Status lit l'issue et le pipeline échoue avec le message de l'action suivante, sauf si cette action est l'activation de la planification.

**Paramètres**

Le paramètre p_run_tests est vide par défaut, ce qui laisse décider la variable bootstrap_run_tests ; true force la campagne de tests et false la saute.

**Sorties produites**

Le pipeline produit, par ses notebooks, le socle du Lakehouse, le rapport de tests dans le schéma isolé et le premier run complet. Son message d'échec indique l'action suivante, par exemple la saisie des codes ADP manquants.

**Limitations connues et points d'attention**

La planification quotidienne doit rester désactivée pendant tout le premier chargement. En production, le premier chargement a lieu au début de la marche à blanc, pendant que l'existant publie encore ; la bascule ne le relance pas : elle active l'écriture et lance un run quotidien juste après le dernier run de l'existant, comme le décrit le README de TST_5.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
