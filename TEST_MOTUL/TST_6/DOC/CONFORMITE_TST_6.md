# Conformité de TST_6 aux demandes, indépendance et préparation à la production

Établi à l'issue de l'étape 2 (locale). Légende : **[V]** vérifié par un contrôle rejouable (`LOCAL_TESTS`), **[L]** vérifié par lecture de fichiers, **[T]** à prouver dans Fabric. Aucune ligne ne signifie « validé en PRD » : rien n'a été exécuté dans Fabric.

## 1. Indépendance

| Contrôle | Résultat |
|---|---|
| Références d'exécution à `TST_3`, `TST_4`, `TST_5`, `TEST_1` (noms `T3_` à `T5_`, chemins) | **Aucune** [V] (`verify_static.py` : résidus de nommage). Seuls le README et les outils locaux citent `TST_5`, pour l'historique |
| Items et pipelines | Les 4 pipelines ne désignent que des items de `TST_6` ; chaque référence se résout [V] |
| Identifiants logiques et noms | Aucun commun avec `TST_3`, `TST_4`, `TST_5`, `TEST_1` [V] (`T6_*` uniquement) |
| Bibliothèque chargée | Les notebooks ne chargent que `T6_NB_HRIS_LIB` [V] |
| Références mortes corrigées à l'étape 2 | Générateur absent du dossier, rapports de sondes absents, chemins `sys.path` vers `src/`, `tools/`, `fixtures/` ; test des marqueurs de contrat, toujours ignoré dans Fabric, retiré |
| Lakehouse et variable library | Dédiés : `T6_LH_HRIS`, `T6_VL_HRIS` [V] |
| Dépendances d'**environnement** (non de code) | Coffre de production, tenant TalentSoft de production, API ADP et INSEE : partagés par nature, listés dans les prérequis |
| Réserve de dépôt | Le dossier est dans un dépôt Git dont l'historique contient d'anciens identifiants TalentSoft (versions antérieures). Associer **uniquement** ce dossier à un workspace, et renouveler ces identifiants, ou publier `TST_6` dans un dépôt neuf |

**Réponse : indépendant pour le code et les items [V] ; non indépendant pour l'historique Git du dépôt (réserve ci-dessus).**

## 2. Demandes et état

| Demande | État | Preuve ou limite |
|---|---|---|
| Composants : notebook, pipeline, Key Vault, Lakehouse | Respecté | Aucune Function, Logic App, Warehouse, Eventhouse ni Dataflow [L] |
| Variable library, avec justification de pertinence | Autorisée | `DOC/README/t6_vl_hris.md`. Jeu actif non déployé : activation manuelle [T] |
| SQL → PySpark, sans SQL encapsulé | **Partiel** | Transformations en code et DataFrames. Restent des `spark.sql` : `CREATE SCHEMA` (limite technique), `CREATE TABLE` généré, tests. Pyspark absent du poste : non rejouable ici |
| Règle métier préservée (types, NULL, jointures, arrondis, erreurs) | Hérité de `TST_5` | Oracle Python + parité sur données fictives ; **jamais démontré sur données réelles** |
| Exécution en un clic | Respecté, avec réserves | `T6_PL_HRIS_ORCHESTRATOR` (planifié) ; `T6_PL_HRIS_MAIN` avec notification (non planifié) ; statut final de MAIN [T] |
| Plus de dépôt de fichiers dans un stockage externe | **Respecté** | Import historique ADP exclu, module et tests retirés [V]. **Conséquence** : l'historique des journaux de l'ancienne Function n'a plus de chemin de reprise (arbitrage à faire) |
| Tables Lakehouse par schéma (cfg, ref, stg, ctl, log, pub) | Hérité | Schémas créés par `T6_NB_HRIS_SETUP` [L] |
| Courriels HTML vert / rouge, par l'activité Outlook | **Préparé, non envoyé** | HTML et tests locaux [V] ; l'activité Outlook est à créer dans le portail ; rendu et expéditeur [T] |
| Secrets dans Key Vault, rien en clair | Respecté | Aucun secret détecté [V] ; identité de lecture en run pipeline [T] |
| Règles de code maintenables | **Partiel** | Notebook neuf conforme `RC-01` à `RC-12` ; `eventpath` conservé dans les notebooks hérités (étape 6) |
| Journaux et diagnostic en tables | Hérité | `ctl.*`, `log.*` ; requêtes d'exploitation dans le README |
| Un dossier ONESHOT et un dossier QUOTIDIEN | Respecté | `ONESHOT` (setup, premier chargement, saisie des codes ADP) et `QUOTIDIEN` |
| Tests et non-régression | **Partiel** | 14 tests locaux [V] ; campagne `T6_NB_HRIS_TESTS` jamais exécutée dans Fabric ; protocole de non-régression rédigé, non exécuté |
| Git et déploiement par environnement | Documenté | Jeux de valeurs, GUID, connexions à paramétrer (étape 8) |

## 3. Préparation à la production (skill `fabric-prd-readiness`)

- **Prêt au déploiement** : oui pour le quotidien et le oneshot **hors notification** : emballage, noms, références, structure des pipelines, empreintes, planifications désactivées, interrupteurs de publication à `false`, aucun secret [V].
- **Validé en PRD** : **non**. Restent à prouver dans Fabric : chargement `%run` entre dossiers, lecture de la valeur de sortie d'un notebook par le pipeline, identité d'exécution (coffre, variable library), statut final de `T6_PL_HRIS_MAIN`, création de schéma, taille d'une sortie de notebook, rendu des couleurs, expéditeur du courriel, capacité.
- **Bloquants avant PRD** : renouvellement des identifiants TalentSoft de l'historique Git ; arbitrage de la reprise de l'historique des journaux ADP ; non-régression de 10 jours ouvrés ; décision sur les données personnelles (rétention, rôles OneLake).
