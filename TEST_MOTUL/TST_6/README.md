# TST_6 — Hub HRIS TalentSoft → ADP sur Microsoft Fabric (contraintes de migration Fabric)

`TST_6` est une **copie indépendante de `TST_5`**, renommée `T6_*`, que l'on fait évoluer pour respecter les contraintes de migration Azure → Fabric :
- composants limités aux **notebooks**, **pipelines de données**, **Azure Key Vault** et **Lakehouse**, plus une **variable library** autorisée à titre d'exception justifiée (voir `DOC/README/t6_vl_hris.md`) ;
- traitements en **PySpark** (pas de SQL encapsulé dans `spark.sql`), avec les limites techniques déclarées plus bas ;
- plus de dépôt ni d'échange de **fichiers métier dans un stockage externe** : tables Lakehouse ;
- lancement du processus complet depuis **un seul pipeline** ;
- notifications par **courriel HTML** coloré (vert, rouge, orange), envoyées par l'activité Outlook d'un pipeline.

`TST_6` ne dépend d'aucune autre version (`TST_3`, `TST_4`, `TST_5`) ni d'aucun autre dossier du dépôt. Chaque item a un identifiant logique propre et un nom `T6_*`.

**Rien n'a été déployé ni exécuté dans Fabric.** La version est validée en local seulement : contrôles statiques, compilation, empreintes des modules et tests du notebook de notification sur données fictives, sans réseau.

---

## État d'avancement

| Étape | Contenu | État |
|---|---|---|
| 1 | Base issue de `TST_5` ; import de l'historique des journaux ADP exclu ; variable library nettoyée ; `exit` par `notebookutils` ; notebook de notification et squelette du pipeline d'envoi ; documentation ; outils de contrôle locaux | **Faite** (locale) |
| 2 | Indépendance et code dormant : module `hris.legacy_logs` et son test retirés, deux tests dépendant de dossiers extérieurs retirés, chemins orphelins et références mortes supprimés ; pipeline `T6_PL_HRIS_MAIN` (orchestrateur, message de fin de run, envoi, propagation de l'échec) ; recherche du run par heure de déclenchement ; contrôle de structure des pipelines | **Faite** (locale) |
| 3 | Première exécution en DEV : `%run` d'un dossier à l'autre, valeur de sortie d'un notebook lue par le pipeline, identité d'exécution (Key Vault, variable library), statut final de `T6_PL_HRIS_MAIN` dans chaque cas | **En cours** : premier run du OneShot le 2026-11-04 ; Setup, Plan et RunStart au vert (rapport de l'utilisateur), puis échec de l'orchestrateur sur un `IfCondition` imbriqué (interdit par Fabric), **corrigé** (activité `RunVerdict`, contrôle ajouté à `verify_static.py`). Nouveau run à faire après libération du verrou orphelin (voir Exploitation) |
| 4 | Création de l'activité Outlook dans le portail ; **test de rendu** des couleurs (Outlook web, poste, mobile) ; expéditeur selon le mode d'authentification | À faire (accord et boîte de test requis) |
| 5 | Table de suivi des envois contre les doublons ; remplacement de la planification de l'orchestrateur par celle de `T6_PL_HRIS_MAIN` | À faire (après 3 et 4) |
| 6 | Alignement sur les règles de code `RC-xx` : retrait de `eventpath` ; création des tables sans DDL généré (non testable sans Spark local) ; tables historiques inutilisées | À faire |
| 7 | Non-régression Azure / Fabric (protocole du rapport d'audit) | À faire |
| 8 | Git et déploiement par environnement (identifiants, connexions, jeu de valeurs actif) | À faire |

## Contraintes du projet et position actuelle

| Contrainte | État dans `TST_6` |
|---|---|
| Notebook, pipeline, Key Vault, Lakehouse uniquement | Respectée. Variable library autorisée (exception justifiée). Aucune Function, Logic App, Warehouse, Eventhouse ni Dataflow |
| SQL → PySpark | **Partielle.** Transformations en code Python et DataFrames. Restent des `spark.sql` : `CREATE SCHEMA` (limite technique, aucune API DataFrame confirmée), `CREATE TABLE` à partir d'un DDL généré (étape 6) et les tests |
| Un clic | `T6_PL_HRIS_ORCHESTRATOR` (planifié) et `T6_PL_HRIS_MAIN` (avec notification, non planifié). La configuration initiale (premier chargement, saisie des codes ADP) est distincte et décrite plus bas |
| Plus de fichiers dans un stockage externe | Respectée : import de l'historique ADP **exclu** (voir T6-02) et module correspondant retiré |
| Courriels HTML par compte de service | **Préparés, non envoyés** : `T6_NB_HRIS_NOTIFY` fournit le HTML et `T6_PL_HRIS_MAIN` l'enchaîne ; l'activité Outlook reste à créer dans le portail et à tester |
| Secrets dans Key Vault | Respectée : aucun identifiant dans le code ni dans la variable library |

## Organisation

| Dossier | Objet (nom dans Fabric) | Rôle |
|---|---|---|
| `QUOTIDIEN` | `T6_PL_HRIS_ORCHESTRATOR` | Pipeline quotidien, planifié à 06:00 (Paris), désactivé dans le dépôt |
| `QUOTIDIEN` | `T6_NB_HRIS_LIB` | Bibliothèque commune (rôle de `nb_shareddef`) : modules métier à empreinte vérifiée, logger, contexte |
| `QUOTIDIEN` | `T6_NB_HRIS_RUN_CONTROL` | Ouverture (verrou, registre), clôture, échec d'un run |
| `QUOTIDIEN` | `T6_NB_HRIS_TS_ACQUIRE` | Extraction TalentSoft PRD, complétude, catalogue des codes TalentSoft |
| `QUOTIDIEN` | `T6_NB_HRIS_REMUNERATION` | Flux rémunération en tables, J / J-1, aucun envoi |
| `QUOTIDIEN` | `T6_NB_HRIS_TRANSFORM` | Règles SQL de l'existant : correspondances, rejets, format paie |
| `QUOTIDIEN` | `T6_NB_HRIS_COMPARE` | Comparaison à la référence, contrôles, décision |
| `QUOTIDIEN` | `T6_NB_HRIS_PROMOTE` | Référence et liste à publier |
| `QUOTIDIEN` | `T6_NB_HRIS_PUBLISH_ADP` | Publication ADP : garde du workspace, interrupteurs, intention durable |
| `QUOTIDIEN` | `T6_PL_HRIS_MAIN` | **Nouveau.** Point d'entrée un clic avec notification ; propage l'échec du run. Non planifié |
| `QUOTIDIEN` | `T6_NB_HRIS_NOTIFY` | **Nouveau.** Prépare le sujet, les destinataires et le HTML du message de fin de run ; n'envoie rien |
| `QUOTIDIEN` | `T6_PL_HRIS_NOTIFY_MAIL` | **Nouveau, squelette.** Pipeline d'envoi : l'activité Outlook est à créer dans le portail |
| `QUOTIDIEN` | `T6_VL_HRIS` | Paramètres d'environnement non secrets, jeux de valeurs DEV (défaut), UAT, PRD |
| `QUOTIDIEN` | `T6_LH_HRIS` | Lakehouse dédié, schémas activés |
| `ONESHOT` | `T6_PL_HRIS_BOOTSTRAP` | Premier chargement guidé (à la main, jamais planifié) |
| `ONESHOT` | `T6_NB_HRIS_SETUP` | Schémas, tables, graines (idempotent) |
| `ONESHOT` | `T6_NB_HRIS_BOOTSTRAP` | Décisions du premier chargement (`PLAN`, `STATUS`) |
| `ONESHOT` | `T6_NB_HRIS_REFERENCE_ADMIN` | Saisie tracée des codes ADP (grille collée, aucun fichier) |
| `TESTS` | `T6_NB_HRIS_TESTS` | Campagne de tests sur données fictives, schéma isolé |
| `DOC/README` | `<displayname>.md` | Documentation de chaque objet (règle R21) |
| `LOCAL_TESTS` | *(hors Fabric)* | Contrôles et tests lancés sur le poste, voir plus bas |

Retiré par rapport à `TST_5` : `T5_NB_HRIS_LEGACY_LOGS_IMPORT`.

## Décisions de l'étape 1

| # | Décision | Justification |
|---|---|---|
| T6-01 | `TST_6` part d'une copie de `TST_5` et non d'une réécriture | `TST_5` porte l'audit d'équivalence et les tests déjà faits ; refaire la bibliothèque (22 modules protégés par empreinte) perdrait ces preuves |
| T6-02 | `T6_NB_HRIS_LEGACY_LOGS_IMPORT` est exclu, ainsi que ses trois variables | Il lit un compte de stockage externe par le SDK Blob, ce que la contrainte interdit. L'import ponctuel de l'historique ADP (décision D-18) **n'a plus de chemin conforme** : **blocage à arbitrer** (exception explicite, ou historique non repris) |
| T6-03 | La variable library est conservée | Pertinence : fichiers versionnés et déployés (pas les tables d'un Lakehouse), lisible par notebooks et pipelines. Limites : jeu actif non déployé, aucun secret, type nombre non supporté dans les pipelines |
| T6-04 | Courriels par activité Outlook plutôt que par l'API Graph | Décision de l'utilisateur, **sous réserve** du test de rendu des couleurs |
| T6-05 | La notification est branchée dans un pipeline **à côté** de l'orchestrateur (`T6_PL_HRIS_MAIN`), jamais dans l'orchestrateur | La sémantique des branches « Terminée / Échec » et le statut final d'un pipeline ne sont pas documentés : un branchement dans l'orchestrateur pourrait **masquer** l'échec du run et supprimer `HRIS_RUN_FAILED`, `HRIS_RUN_BLOCKED`. Le pipeline d'entrée n'est pas planifié tant qu'il n'est pas validé |
| T6-06 | `eventpath` est conservé dans les notebooks hérités ; absent de `T6_NB_HRIS_NOTIFY` | Son retrait touche la bibliothèque protégée et les tests (étape 6) |
| T6-07 | `mssparkutils.notebook.exit` remplacé par `notebookutils.notebook.exit` dans 11 notebooks | `mssparkutils` est l'ancien nom (règle `RC-11`) |
| T6-08 | Retrait du module `hris.legacy_logs`, de son test, du test `test_contract_markers` et des chemins `sys.path` vers `src/`, `tools/`, `fixtures/` | Ces éléments lisaient un stockage externe ou supposaient des dossiers qui ne sont pas dans `TST_6` (le test des marqueurs de contrat était toujours ignoré dans Fabric). Les tables `log.log_file_import*`, devenues sans usage, restent à retirer (étape 6) |
| T6-09 | Le notebook de notification retrouve le run par l'heure de déclenchement (`p_since_utc`, marge de 5 minutes) | La propriété qui donnerait l'identifiant du run lancé par une activité de pipeline n'est pas documentée |

## Comportement hérité de `TST_5` (résultats de l'audit d'équivalence)

| Sujet | Comportement |
|---|---|
| Rejet sur la catégorie professionnelle | Comme l'existant : exclue de la somme du `WHERE`. Une catégorie inconnue seule ne rejette plus le salarié ; sa ligne de rejet reste émise quand un autre contrôle rejette (E-01) |
| Référence de rémunération | Avance à chaque run, comme le J-1 de l'existant (E-02) |
| Envoi de rémunération vers TalentSoft | Supprimé (D-06) ; le mode `VALIDATE` lit en lecture seule les imports de l'existant |
| Publication ADP | Aucun verrou de code (D-16). Deux interrupteurs de la variable library, `adp_read_enabled` et `adp_write_enabled`, restreignent seulement la garde « nom réel du workspace contenant `PRD` ». Trois modes : désactivée, **lecture seule** (`log.adp_dry_run`), lecture-écriture |
| Seuils de publication | Aucun seuil bloquant, comme l'existant (D-17) |
| Identifiants TalentSoft | Lus dans le coffre (`get_parameter`) (B-01) |
| Journaux | `log.notebook_log` ; chaque notebook journalise ses étapes et ses erreurs expurgées |
| Run bloqué | Le pipeline échoue avec le code `HRIS_RUN_BLOCKED` |
| Bail du verrou | 600 minutes (A-02) |
| Concurrence TalentSoft | 10 / 10 comme l'existant, réglable dans la variable library (A-03) |
| Saisie des codes ADP | Refusée pendant un run (A-04) |
| Bilan | Colonne `ds_rejets` de l'existant, valeurs sensibles masquées (E-08) |
| Premier chargement en PRD | Tests non lancés (`bootstrap_run_tests = false`) (B-07) |

## Règles de développement

Les notebooks hérités suivent les règles `R00`–`R21` de `TST_5` (ordre des cellules, `%run T6_NB_HRIS_LIB`, logger, configuration par `initialize_notebook_context`, paramètres, fonctions une par cellule, `try/except` avec `flush_logger` contextuel puis `raise`, clôture). Les règles généralisées pour les prochaines évolutions sont celles du skill `fabric-notebook-coding-rules` (`RC-01` à `RC-19`). Écarts actuels :

| Règle | Écart | Prévu |
|---|---|---|
| `RC-08` (pas d'`eventpath`) | Conservé dans les notebooks hérités | Étape 6 |
| `RC-13` / `RC-14` (pas de SQL encapsulé) | `CREATE SCHEMA` et DDL de tables par `spark.sql` dans la bibliothèque | Étape 6 ; `CREATE SCHEMA` restera une limite technique à confirmer |
| `RC-05` | Les modules `hris.*` suivent les conventions Python, protégés par empreinte | Maintenu : voir « Maintenir la bibliothèque » |
| `RC-07` | `pipeline_id` / `execution_id` non transmis ; le run est identifié par `p_run_id` | Maintenu |
| `T6_NB_HRIS_NOTIFY` | Respecte `RC-01` à `RC-12` ; cellule de schéma par la fonction partagée comme les autres | — |

## Maintenir la bibliothèque (empreintes)

`T6_NB_HRIS_LIB` et `T6_NB_HRIS_TESTS` embarquent des modules Python dont l'empreinte SHA-256 est vérifiée au chargement ; le générateur d'origine n'est plus dans le dépôt. Après toute modification du texte d'un module, recalculer l'empreinte, **sur le texte normalisé en fins de ligne LF** (les fichiers du poste sont en CRLF, l'empreinte attendue est celle du dépôt Git) :

```text
python -I LOCAL_TESTS/digest_tools.py QUOTIDIEN/NB_HRIS_LIB.Notebook/notebook-content.py   # contrôle
```

`digest_tools.refresh(<fichier>)` recalcule les empreintes d'un notebook ; il n'est pas appelé en ligne de commande par sécurité.

## Contrôles réalisés en local

```text
python -B -I LOCAL_TESTS/verify_static.py <dossier TST_6>   # empreintes, résidus T5, logicalId, références, structure des pipelines, compilation, mssparkutils, secrets
python -B -I LOCAL_TESTS/test_notify_html.py                # 14 tests du notebook de notification (sans Spark, sans réseau)
python -B -I LOCAL_TESTS/render_preview.py                  # écrit LOCAL_TESTS/apercu/*.html (données fictives)
```

Résultat à l'issue de l'étape 2 : 19 items, 21 + 14 modules aux empreintes valides, 185 cellules et 35 modules compilés, 4 pipelines dont les dépendances et les références d'activités sont résolues, aucune référence orpheline, 14 tests sur 14 réussis. **Spark n'est pas installé sur le poste** : aucun test dépendant de Spark n'a pu être rejoué (voir `DOC/CONFORMITE_TST_6.md`). Les aperçus HTML dans un navigateur **ne remplacent pas** le test dans Outlook.

## Prérequis PRD (hors code)

1. **Workspace PRD** dans le **tenant Motul**, dont le nom contient `PRD`. **Aucun autre workspace** ne contient `PRD`, y compris la recette.
2. **Runtime Fabric 1.3** par défaut dans le workspace.
3. **Coffre** `kvt-mge-azr-hris-ts-prd` lisible par l'identité d'exécution des notebooks (**à vérifier**, voir plus bas). Secrets : TalentSoft `client-id-ts`, `client-secret-ts` ; ADP `client-id-adp`, `client-secret-adp`, `MOTUL-SA-cer`, `motulsa-auth-key`.
4. **Renouvellement** des identifiants TalentSoft présents dans l'historique Git des versions précédentes.
5. **Sorties HTTPS** vers `motul.talent-soft.com`, `accounts.eu.adp.com` et `geo.api.gouv.fr`.
6. **Rôles d'accès** du workspace et du Lakehouse restreints : onze tables contiennent des données personnelles.
7. **Notification** : une connexion Fabric de type Office 365 Outlook, créée dans chaque environnement par le compte d'envoi, et une liste de diffusion pour `notify_recipients`.
8. **Chemin de déploiement** : ce dossier seul (`TEST_MOTUL/TST_6`) associé au workspace, ou un dépôt d'équipe. Ne jamais associer un workspace PRD à la racine de ce dépôt, qui contient d'autres versions.

## Mise en service

1. **DEV** : synchroniser ; vérifier la création de `T6_LH_HRIS` à schémas ; **activer et enregistrer le jeu de valeurs voulu dans `T6_VL_HRIS`** ; lancer `T6_PL_HRIS_BOOTSTRAP` (tests compris). Le premier passage s'arrête sur `HRIS_ENTER_ADP_CODES`.
2. **Codes ADP** : dans `T6_NB_HRIS_REFERENCE_ADMIN`, coller le résultat des `SELECT * FROM dwh.lu_hr_*` de Synapse (13 tables), passer en `CHECK`, puis en `APPLY` avec `p_change_ref`. Faire trancher par le métier le code de situation matrimoniale en conflit. Relancer le premier chargement.
3. **UAT** : même procédure, avec le jeu de valeurs **UAT** activé et enregistré.
4. **PRD** : synchroniser ; activer et enregistrer le jeu de valeurs **PRD** ; vérifier les prérequis. Laisser la planification désactivée.
5. **Non-régression en parallèle d'Azure**, qui reste la référence et continue de publier : lancer `T6_PL_HRIS_ORCHESTRATOR` juste après le run Azure de 02:00 ; comparer extraction, transformation, rejets et rémunération aux sorties Azure du même jour ; puis passer `adp_read_enabled = true` (commit tracé du jeu PRD) pour comparer les opérations planifiées (`log.adp_dry_run`) au journal de la Function ADP ; critère : 10 jours ouvrés sans écart inexpliqué.
6. **Bascule** : après le dernier run Azure de 02:00, arrêter `DailyTrigger` et l'activité `Azure Function ADP` ; passer `adp_write_enabled = true` par commit tracé du jeu PRD ; lancer aussitôt `T6_PL_HRIS_ORCHESTRATOR` en `NORMAL`, puis activer la planification à 06:00.
7. **Retour arrière** : désactiver la planification et les interrupteurs, puis réactiver le déclencheur et la Function Azure. Les tables Fabric restent intactes (historique Delta de 30 jours).

## Exploitation

```sql
SELECT run_id, run_mode, status, business_date, error_code FROM ctl.run ORDER BY started_at_utc DESC;
SELECT step_code, attempt_no, status, rows_out, message FROM ctl.run_step WHERE run_id = '<run_id>' ORDER BY started_at_utc;
SELECT control_code, severity, outcome, message FROM log.anomaly WHERE run_id = '<run_id>';
SELECT item_name, seq_no, level, message, error_type, error_location FROM log.notebook_log WHERE run_id = '<run_id>' ORDER BY flushed_at_utc, seq_no;
SELECT status, COUNT(*) FROM pub.adp_outbox WHERE run_id = '<run_id>' GROUP BY status;
```

| Situation | Que faire |
|---|---|
| Run bloqué (`HRIS_RUN_BLOCKED`) | Lire `log.anomaly`, corriger la cause, relancer. La référence est intacte |
| Publication partielle | Relancer avec `p_run_mode = RETRY_PUBLICATION` et `p_target_run_id` |
| Résultat incertain | Réconciliation ou décision manuelle ; jamais de renvoi automatique |
| Verrou orphelin (un run a pris le verrou puis le pipeline s'est arrêté sans appeler `RunFail`, par exemple erreur de structure du pipeline) | **Ne jamais supprimer la ligne de verrou.** Chemin prévu : lancer `T6_NB_HRIS_RUN_CONTROL` avec `p_mode = FAIL` et `p_run_id` = identifiant du run orphelin (colonne `holder_run_id` de `ctl.process_lock`, ou identifiant du run de l'orchestrateur dans le suivi). Le run passe en `FAILED` (`PIPELINE_FAILURE`) et le verrou n'est libéré que s'il appartient bien à ce run. À défaut, attendre l'expiration du bail (600 minutes) |
| Tables vides ou absentes dans le point de terminaison SQL alors que les notebooks sont verts | La synchronisation des métadonnées du point de terminaison SQL peut avoir du retard sur les tables écrites par Spark. Lire les tables dans l'explorateur du Lakehouse (aperçu de la table) ou avec `spark.read.table`, ou actualiser le point de terminaison |

## Ce qui reste à démontrer

Hérité de `TST_5` :
- le chargement par `%run` d'un dossier à l'autre en exécution par pipeline (**observé au vert** au premier run du 2026-11-04, sur rapport de l'utilisateur ; à confirmer par `ctl.run_step` une fois un run complet obtenu) ;
- l'équivalence sur données réelles, par le protocole de non-régression ;
- les hypothèses de correspondance de la rémunération (mode `VALIDATE`) ;
- le comportement face aux réponses réelles d'ADP.

Propre à `TST_6` (non documenté par Microsoft, à tester dans Fabric) :
- l'expression du pipeline qui lit la valeur de sortie d'un notebook (ici `json(activity('X').output.result.exitValue)`, forme déjà utilisée dans l'orchestrateur hérité) ;
- l'identité réellement utilisée par un notebook lancé par un pipeline pour lire le coffre et la variable library ;
- la taille maximale d'une valeur de sortie de notebook (le HTML est limité à 40 000 octets) ;
- le rendu des couleurs dans l'activité Outlook, l'expéditeur selon l'authentification, et le schéma JSON de l'activité ;
- le statut final d'un pipeline dont une branche « Terminée » suit un échec, avant le branchement de la notification ;
- la création de schéma sans `spark.sql`.
