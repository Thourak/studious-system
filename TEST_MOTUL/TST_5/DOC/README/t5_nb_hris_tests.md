**Nom de l'objet**

T5_NB_HRIS_TESTS

**Type d'objet**

Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/TESTS/NB_HRIS_TESTS.Notebook/notebook-content.py

**Description fonctionnelle**

Ce notebook exécute la campagne de tests du hub sur des données entièrement fictives. Il vérifie les règles reprises de l'existant, l'acquisition contre un faux serveur, la publication ADP contre un simulateur, la garde sur le nom du workspace et les interrupteurs, le flux de rémunération, les correspondances, le premier chargement et le journal de fonctionnement. Les scénarios d'intégration écrivent dans un schéma isolé du Lakehouse, jamais dans les tables réelles, et n'appellent ni TalentSoft ni ADP. Il peut être lancé à la main ou par le pipeline de premier chargement, qui s'arrête si un test échoue.

**Dépendances**

Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB, puis enregistre seize modules propres aux tests : le simulateur de l'outil de paie, le jeu de données fictives et quatorze modules de tests. Il lit dans la variable library T5_VL_HRIS le nom du Lakehouse. Il est appelé par le pipeline T5_PL_HRIS_BOOTSTRAP dans l'activité Tests, quand la variable bootstrap_run_tests ou le paramètre p_run_tests du pipeline le demande.

**Fonctionnement et logique de traitement**

Après la configuration, le notebook refuse un schéma de test vide, puis prépare les schémas de test et dirige son journal vers le schéma isolé. Chaque module de test est enregistré avec la vérification de son empreinte. La campagne exécute ensuite les tests unitaires et, si l'intégration est demandée, les scénarios qui écrivent dans le schéma isolé. Le rapport de campagne est conservé dans la table zz_test_report du schéma de test. Si l'échec doit être bloquant et qu'un test échoue, une cellule dédiée arrête le notebook après avoir enregistré le rapport.

**Paramètres**

Le paramètre p_test_schema désigne le schéma isolé ; sa valeur par défaut est hris_tst et il ne doit jamais être vide. Le paramètre p_run_integration vaut true pour exécuter aussi les scénarios avec écriture dans le schéma de test. Le paramètre p_only filtre les tests par fragments de leur identifiant, séparés par des virgules. Le paramètre p_fail_on_error vaut false pour un simple rapport et true pour faire échouer le notebook si un test échoue, comme le demande le premier chargement. Le paramètre eventpath reçoit les paramètres d'un événement Fabric.

**Sorties produites**

Le notebook recrée ses tables dans le schéma isolé à chaque campagne d'intégration, ajoute le rapport à la table zz_test_report de ce schéma et écrit son journal dans log.notebook_log du même schéma. Il renvoie au pipeline les compteurs de la campagne et les identifiants des tests en échec.

**Limitations connues et points d'attention**

En production, les tests ne sont pas lancés par le premier chargement : ils tournent en développement et en recette. Lancé dans un workspace de production, le scénario de publication vérifie que la garde est satisfaite mais que les interrupteurs, laissés à false par le test, empêchent tout appel. Les tests de parité Spark sont ignorés hors Fabric et le test du document de contrat ADP est ignoré tant que ce document n'est pas versionné.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
