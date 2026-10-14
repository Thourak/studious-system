# TST_5 — Hub HRIS TalentSoft → ADP sur Microsoft Fabric (version prête pour la PRD)

`TST_5` reprend le **comportement de l'ancien traitement Azure**. Les deux flux sont concernés :
- l'extraction TalentSoft, la mise en forme pour la paie et la publication vers ADP ;
- le calcul des changements de rémunération.

Les contraintes du projet sont les suivantes :
- **aucun fichier CSV** dans le traitement ;
- des **journaux de fonctionnement** dans un **Lakehouse dédié** ;
- des traitements adaptés à Fabric ;
- le respect des règles de développement des notebooks ;
- **aucune dépendance** à une autre version (`TST_3`, `TST_4`) ni à un autre dossier du dépôt.

Rien n'a été déployé ni exécuté dans Fabric. La version a été validée en local : contrôles statiques, compilation et tests sur données fictives, réseau coupé.

---

## Organisation

| Dossier | Objet (nom dans Fabric) | Rôle |
|---|---|---|
| `QUOTIDIEN` | `T5_PL_HRIS_ORCHESTRATOR` | Pipeline quotidien, planifié à 06:00 (Paris), désactivé dans le dépôt |
| `QUOTIDIEN` | `T5_NB_HRIS_LIB` | Bibliothèque commune (rôle de `nb_shareddef`) : modules métier à empreinte vérifiée, logger, contexte |
| `QUOTIDIEN` | `T5_NB_HRIS_RUN_CONTROL` | Ouverture (verrou, registre), clôture, échec d'un run |
| `QUOTIDIEN` | `T5_NB_HRIS_TS_ACQUIRE` | Extraction TalentSoft PRD, complétude, catalogue des codes TalentSoft |
| `QUOTIDIEN` | `T5_NB_HRIS_REMUNERATION` | Flux rémunération en tables, J / J-1, aucun envoi |
| `QUOTIDIEN` | `T5_NB_HRIS_TRANSFORM` | Règles SQL de l'existant : correspondances, rejets, format paie |
| `QUOTIDIEN` | `T5_NB_HRIS_COMPARE` | Comparaison à la référence, contrôles, décision |
| `QUOTIDIEN` | `T5_NB_HRIS_PROMOTE` | Référence et liste à publier |
| `QUOTIDIEN` | `T5_NB_HRIS_PUBLISH_ADP` | Publication ADP : garde du workspace, interrupteurs, intention durable |
| `QUOTIDIEN` | `T5_VL_HRIS` | Paramètres d'environnement, jeux de valeurs DEV (défaut), UAT, PRD |
| `QUOTIDIEN` | `T5_LH_HRIS` | Lakehouse dédié, schémas activés |
| `ONESHOT` | `T5_PL_HRIS_BOOTSTRAP` | Premier chargement guidé (à la main, jamais planifié) |
| `ONESHOT` | `T5_NB_HRIS_SETUP` | Schémas, tables, graines (idempotent) |
| `ONESHOT` | `T5_NB_HRIS_BOOTSTRAP` | Décisions du premier chargement (`PLAN`, `STATUS`) |
| `ONESHOT` | `T5_NB_HRIS_REFERENCE_ADMIN` | Saisie tracée des codes ADP (grille collée, aucun fichier) |
| `ONESHOT` | `T5_NB_HRIS_LEGACY_LOGS_IMPORT` | Import unique de l'historique des journaux ADP de l'existant |
| `TESTS` | `T5_NB_HRIS_TESTS` | Campagne de tests sur données fictives, schéma isolé |
| `DOC/README` | `<displayname>.md` | Documentation de chaque objet (règle R21) |

Chaque item a son **propre identifiant logique** et un **nom `T5_*`**. Les pipelines ne désignent que des items de `TST_5`, et les notebooks ne chargent que `T5_NB_HRIS_LIB`.

## Différences avec TST_4 (résultats de l'audit d'équivalence)

| Sujet | TST_5 |
|---|---|
| Rejet sur la catégorie professionnelle | Comme l'existant : exclue de la somme du `WHERE`. Une catégorie inconnue seule ne rejette plus le salarié ; sa ligne de rejet reste émise quand un autre contrôle rejette (E-01). |
| Référence de rémunération | Avance à chaque run, comme le J-1 de l'existant : plus de delta cumulé en PRD (E-02). |
| Envoi de rémunération vers TalentSoft | Supprimé (D-06, rationalisation R-01) ; le mode `VALIDATE` lit en lecture seule les imports de l'existant. |
| Publication ADP | Plus aucun verrou de code (D-16). Deux interrupteurs de la variable library, `adp_read_enabled` et `adp_write_enabled`, ne font que restreindre la garde « nom réel du workspace contenant `PRD` ». Trois modes : désactivée, **lecture seule** (opérations planifiées dans `log.adp_dry_run`, aucune écriture) et lecture-écriture. Les éléments d'un run désactivé ou simulé ont un statut terminal et ne sont jamais envoyés plus tard. |
| Seuils de publication | Aucun seuil bloquant, comme l'existant (D-17) |
| Identifiants TalentSoft | Lus dans le coffre (`get_parameter`), plus aucun identifiant dans le code (B-01) |
| Variable library | Jeux de valeurs UAT et PRD (B-02) ; les notebooks la lisent eux-mêmes (R05) et les pipelines ne transmettent que les paramètres du run |
| Adresses ADP et INSEE | Lues dans la variable library (R12) |
| Journaux | `log.notebook_log` : chaque notebook journalise ses étapes et ses erreurs (expurgées) par `setup_fabric_logger` et `flush_logger` (R04, R07, R10, R11) |
| Run bloqué | Le pipeline échoue avec le code `HRIS_RUN_BLOCKED` : le blocage est visible dans le suivi Fabric (A-01) |
| Bail du verrou | 600 minutes, supérieur aux délais cumulés de 9 h 40 (A-02) |
| Concurrence TalentSoft | 10 / 10 comme l'existant, réglable dans la variable library (A-03) |
| Saisie des codes ADP | Refusée pendant un run (verrou), pour ne pas croiser la synchronisation de l'acquisition (A-04) |
| Bilan | Colonne `ds_rejets` de l'existant ajoutée, valeurs sensibles masquées (E-08) |
| Historique des journaux ADP | Import unique par le SDK de stockage Azure, rejouable (D-18) |
| Premier chargement en PRD | Tests non lancés (variable `bootstrap_run_tests = false`) ; le test d'intégration accepte un workspace PRD (B-07) |

## Règles de développement : application et dérogations

Tous les notebooks métier suivent l'ordre imposé des cellules :
1. documentation (R01) ;
2. imports en trois groupes (R02) ;
3. `%run T5_NB_HRIS_LIB` (R03) ;
4. logger (R04) ;
5. configuration par `initialize_notebook_context`, avec validation des variables obligatoires (R05) ;
6. paramètres avec `eventpath` (R06), puis cellule d'eventpath ;
7. création des schémas (R14) ;
8. une fonction par cellule, avec types et docstrings (R08, R09) ;
9. traitements en `try/except`, avec `logger.error(..., exc_info=True)`, `flush_logger` contextuel puis `raise` (R10, R11) ;
10. clôture, avec `flush_logger(logger, is_error=False)` puis `mssparkutils.notebook.exit` (R07).

**Dérogations documentées :**

| Règle | Dérogation | Justification |
|---|---|---|
| R03 | `%run T5_NB_HRIS_LIB` au lieu de `%run nb_shareddef` | Décision du 14/10/2026 : la bibliothèque du projet joue ce rôle et suit la nomenclature de TST_4 ; le projet ne dépend d'aucun notebook partagé externe. |
| R00, R08 dans la bibliothèque | Les modules `hris.*` sont des modules Python (une cellule par module, constantes en majuscules, conventions PEP 8) | Intégrité du code testé, protégé par empreinte SHA-256 ; découper ou renommer ce code ferait perdre les preuves. La bibliothèque suit R18 (exception du notebook partagé). Les fonctions génériques exposées aux notebooks respectent R00, R08 et R09. |
| R02 | Les modules `hris` ne sont pas importés en cellule 2 | Ils sont chargés par la cellule 3 (`%run`) et utilisés sous le nom `hris.<module>`, sans instruction `import` ailleurs qu'en cellule 2. |
| R12 | Adresse du tenant TalentSoft contrôlée par le code (seule la production est acceptée) ; nom de la variable library ; motifs d'hôtes de la sentinelle réseau | Invariants de sécurité et objets d'infrastructure identiques dans tous les environnements. |
| R11 | Contexte du pipeline (`pipeline_id`, `execution_id`) non transmis | Tous les items qui journalisent sont des notebooks (`item_type = "Notebook"`) ; l'identifiant du run du pipeline est enregistré dans `log.notebook_log.run_id`. |
| R07 | Message « Logs sauvegardés dans le Lakehouse » | Les journaux vont dans `log.notebook_log` et non dans un stockage Blob (décision du 14/10/2026). |

## Prérequis PRD (hors code)

1. **Workspace PRD** dans le **tenant Motul**, dont le nom contient `PRD`. **Aucun autre workspace** ne contient `PRD`, y compris la recette.
2. **Runtime Fabric 1.3** par défaut dans le workspace.
3. **Coffre** `kvt-mge-azr-hris-ts-prd` lisible par l'identité d'exécution des notebooks. Secrets nécessaires :
   - TalentSoft : `client-id-ts`, `client-secret-ts` ;
   - ADP : `client-id-adp`, `client-secret-adp`, `MOTUL-SA-cer`, `motulsa-auth-key` ;
   - import historique : `app-tenant-id`, `app-client-id`, `app-client-secret`.
4. **Renouvellement** des identifiants TalentSoft présents dans l'historique Git des versions précédentes.
5. **Sorties HTTPS** vers `motul.talent-soft.com`, `accounts.eu.adp.com` et `geo.api.gouv.fr`. Pour l'import historique, accès au stockage `stomgeazrhristsprd`, qui n'a que des points de terminaison privés.
6. **Rôles d'accès** du workspace et du Lakehouse restreints : onze tables contiennent des données personnelles.
7. **Chemin de déploiement** : ce dossier seul (`TEST_MOTUL/TST_5`) associé au workspace, ou un dépôt d'équipe. Ne jamais associer un workspace PRD à la racine de ce dépôt, qui contient d'autres versions.

## Mise en service

1. **DEV** : synchroniser ; vérifier la création de `T5_LH_HRIS` à schémas ; lancer `T5_PL_HRIS_BOOTSTRAP` (tests compris). Le premier passage s'arrête sur `HRIS_ENTER_ADP_CODES`.
2. **Codes ADP** : dans `T5_NB_HRIS_REFERENCE_ADMIN`, coller le résultat des `SELECT * FROM dwh.lu_hr_*` de Synapse (13 tables), passer en `CHECK`, puis en `APPLY` avec `p_change_ref`. Faire trancher par le métier le code de situation matrimoniale en conflit. Relancer le premier chargement.
3. **UAT** : même procédure, avec le jeu de valeurs **UAT** activé et enregistré dans la variable library.
4. **PRD** : synchroniser ; activer et enregistrer le jeu de valeurs **PRD** ; vérifier les prérequis. Laisser la planification désactivée.
5. **Non-régression en parallèle d'Azure**, qui reste la référence et continue de publier :
   - lancer `T5_PL_HRIS_ORCHESTRATOR` juste après le run Azure de 02:00 ;
   - comparer extraction, transformation, rejets et rémunération aux sorties Azure du même jour ;
   - puis passer `adp_read_enabled = true` (commit tracé du jeu PRD) pour comparer les opérations planifiées (`log.adp_dry_run`) au journal de la Function ADP ;
   - critère : 10 jours ouvrés sans écart inexpliqué.
6. **Bascule** :
   - après le dernier run Azure de 02:00, arrêter `DailyTrigger` et l'activité `Azure Function ADP` ;
   - passer `adp_write_enabled = true` par commit tracé du jeu PRD ;
   - lancer aussitôt `T5_PL_HRIS_ORCHESTRATOR` en `NORMAL`, puis activer la planification à 06:00.

   Ce premier run réel ne réinitialise pas la référence. Son delta part de la référence de la dernière marche à blanc : il couvre donc toute la fenêtre de bascule, y compris les changements saisis après le dernier run Azure. Les changements déjà publiés par Azure ne sont pas réécrits, car la valeur en place dans ADP est lue avant chaque écriture (D-10). Les éléments des jours de marche à blanc ont un statut terminal : ils ne sont jamais envoyés.
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
| Run bloqué (`HRIS_RUN_BLOCKED`) | Lire `log.anomaly`, corriger la cause, relancer. La référence est intacte. |
| Publication partielle | Relancer avec `p_run_mode = RETRY_PUBLICATION` et `p_target_run_id`. |
| Résultat incertain | Réconciliation ou décision manuelle ; jamais de renvoi automatique. |
| Verrou orphelin | Attendre l'expiration du bail ; ne jamais supprimer la ligne de verrou. |

## Ce qui reste à démontrer

- Le chargement par `%run` d'un dossier à l'autre en exécution par pipeline. Le premier chargement le vérifie dès sa première activité.
- L'équivalence sur données réelles, par le protocole de non-régression du rapport d'audit.
- Les hypothèses de correspondance de la rémunération (mode `VALIDATE`, puis comparaison aux archives de l'existant).
- Le comportement face aux réponses réelles d'ADP, observé pendant la phase de lecture seule puis à la bascule.
