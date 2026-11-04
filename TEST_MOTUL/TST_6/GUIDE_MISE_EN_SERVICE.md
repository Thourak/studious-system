# Guide de mise en service d'un environnement (`TST_6`)

Ce guide décrit, étape par étape, comment installer et démarrer le hub HRIS (TalentSoft vers Lakehouse vers ADP) dans **un** environnement Fabric. Les mêmes étapes s'appliquent à **chaque** environnement (DEV, UAT, PRD) ; les rares différences sont regroupées au paragraphe 1.2 et signalées par **[PRD]**, **[DEV/UAT]** dans le texte.

Établi le 2026-11-04 à partir du code, des variables et des pipelines du dossier. Légende :

- **[V]** : vérifié dans les fichiers ou observé dans Fabric (premier run du 2026-11-04, sur rapport du développeur).
- **[T]** : à prouver dans Fabric ; non documenté par Microsoft ou jamais exécuté.
- Rien n'est « validé en production » tant que les points **[T]** du paragraphe 12 ne sont pas levés.

## Sommaire

1. [Principes et différences entre environnements](#1-principes-et-différences-entre-environnements)
2. [Prérequis (à réunir avant de commencer)](#2-prérequis-à-réunir-avant-de-commencer)
3. [Étape A : préparer le workspace](#3-étape-a--préparer-le-workspace)
4. [Étape B : connecter Git et synchroniser](#4-étape-b--connecter-git-et-synchroniser)
5. [Étape C : activer le bon jeu de valeurs](#5-étape-c--activer-le-bon-jeu-de-valeurs)
6. [Étape D : accès au coffre (Key Vault)](#6-étape-d--accès-au-coffre-key-vault)
7. [Étape E : créer les tables (`T6_NB_HRIS_SETUP`)](#7-étape-e--créer-les-tables-t6_nb_hris_setup)
8. [Étape F : reprendre les codes ADP](#8-étape-f--reprendre-les-codes-adp)
9. [Étape G : premier chargement (`T6_PL_HRIS_BOOTSTRAP`)](#9-étape-g--premier-chargement-t6_pl_hris_bootstrap)
10. [Étape H : publication vers ADP (garde et interrupteurs)](#10-étape-h--publication-vers-adp-garde-et-interrupteurs)
11. [Étape I : planification, notification, exploitation](#11-étape-i--planification-notification-exploitation)
12. [Ce qui reste à prouver dans Fabric](#12-ce-qui-reste-à-prouver-dans-fabric)
13. [Spécifique à la production : non-régression, bascule, retour arrière](#13-spécifique-à-la-production--non-régression-bascule-retour-arrière)
14. [Check-list de recette d'un environnement](#14-check-list-de-recette-dun-environnement)
15. [Pièges déjà rencontrés](#15-pièges-déjà-rencontrés)
16. [Annexes](#16-annexes)

---

## 1. Principes et différences entre environnements

### 1.1 Ce que fait le process

| Pipeline | Rôle | Lancement |
|---|---|---|
| `T6_PL_HRIS_BOOTSTRAP` | **Premier chargement** : setup, tests (DEV/UAT), choix du mode, un run complet, action suivante | À la main, jamais planifié, rejouable |
| `T6_PL_HRIS_ORCHESTRATOR` | **Run quotidien** : extraction TalentSoft, rémunération, transformation, comparaison, promotion, publication ADP | Planifié à 06:00 (Paris), **désactivé dans le dépôt** |
| `T6_PL_HRIS_MAIN` | Point d'entrée avec notification de fin de run | Non planifié ; **non validé** ; optionnel |

Tout est lu depuis **TalentSoft de production**, dans tous les environnements (valeur imposée par le code). Seule la **publication vers ADP** dépend de l'environnement (paragraphe 1.3).

### 1.2 Ce qui change d'un environnement à l'autre

| Sujet | DEV | UAT | PRD |
|---|---|---|---|
| Nom du workspace | **ne contient pas** `PRD` | **ne contient pas** `PRD` | **contient `PRD`** en majuscules |
| Jeu de valeurs actif de `T6_VL_HRIS` | valeurs de base (« DEV ») | `UAT` | `PRD` |
| `env_code` | `DEV` | `UAT` | `PRD` |
| Coffre ADP (`key_vault_url_adp`) | vide | vide | renseigné par le jeu `PRD` |
| Tests du premier chargement (`bootstrap_run_tests`) | `true` | `true` | `false` |
| Publication ADP | jamais (garde) | jamais (garde) | `false` jusqu'à décision, puis lecture seule, puis écriture |
| Planification | à décider | à décider | seulement après non-régression et bascule |
| Objet des courriels de fin de run | `[DEV] [HRIS] …` | `[UAT] [HRIS] …` | `[PRD] [HRIS] …` |

### 1.3 Règle de la publication ADP (à connaître avant toute chose)

Des données ne sont publiées dans ADP **que si le nom réel du workspace qui exécute le notebook de publication contient `PRD`**. La règle est littérale : **sous-chaîne, sensible à la casse**, lue au moment de la décision dans le contexte d'exécution ; aucun paramètre ni variable ne peut la forcer **[V]**.

| Nom du workspace | `adp_read_enabled` | `adp_write_enabled` | Résultat du run |
|---|---|---|---|
| Sans `PRD` (ex. `HRIS-DEV`, `HRIS-UAT`) | quelconque | quelconque | `NOT_PUBLISHED_NON_PRD` : aucun secret lu, aucun appel ADP |
| Avec `PRD` | `false` | `false` | `NOT_PUBLISHED_DISABLED` : aucun secret lu, aucun appel ADP |
| Avec `PRD` | `true` | `false` | `DRY_RUN` : lecture réelle d'ADP, opérations **planifiées** dans `log.adp_dry_run`, rien d'envoyé |
| Avec `PRD` | `true` | `true` | Publication réelle (`SUCCEEDED` ou `PUBLISHED_PARTIAL`) |
| Nom introuvable | quelconque | quelconque | Publication bloquée, éléments en attente, rien n'est envoyé |

Conséquences pour le nommage :

- Le workspace de production doit contenir `PRD` **en majuscules** (`prd` ou `Prd` ne satisfont pas la règle : aucune publication n'aura lieu).
- **Aucun autre workspace ne doit contenir `PRD`**, y compris la recette ou un nom comme `PREPRD`, qui satisferait la règle.
- `adp_write_enabled` exige `adp_read_enabled`. Les deux sont à `false` à la livraison dans tous les jeux de valeurs.

---

## 2. Prérequis (à réunir avant de commencer)

Cocher chaque ligne avant l'étape A.

- [ ] **Workspace** créé, avec le nom conforme au paragraphe 1.3, sur une capacité Fabric disponible. La capacité nécessaire n'est pas dimensionnée **[T]**.
- [ ] **Runtime Fabric 1.3** défini par défaut dans le workspace (paramètres du workspace, Spark).
- [ ] **Rôles** : un rôle du workspace qui permet de créer et d'exécuter les items pour le développeur ; accès au Lakehouse restreint pour les autres (voir « Données personnelles » plus bas).
- [ ] **Git** : accès au dépôt et à la branche qui contient `TEST_MOTUL/TST_6`.
- [ ] **Coffre `kvt-mge-azr-hris-ts-prd`** : droit de **lecture des secrets** pour l'identité qui exécutera les notebooks (voir étape D). Secrets utilisés :
  - TalentSoft : `client-id-ts`, `client-secret-ts` (tous les environnements) ;
  - ADP, **[PRD] seulement** : `client-id-adp`, `client-secret-adp`, `MOTUL-SA-cer`, `motulsa-auth-key`.
- [ ] **Réseau** : sorties HTTPS vers `motul.talent-soft.com`, `accounts.eu.adp.com`, `geo.api.gouv.fr`. Si le coffre est derrière un pare-feu, prévoir un point de terminaison privé géré (limite de 64 par coffre).
- [ ] **Fichiers de référentiels de l'existant** (17 CSV de **production**), récupérables par vous : SFTP `/ExportCarriere/Data/Reference/`, ou conteneur `files`, dossier `talentsoft/reference`, du compte de stockage du projet. Ils portent les codes ADP choisis par le métier, qui n'existent nulle part ailleurs.
- [ ] **Données personnelles** : onze tables contiennent des données personnelles. Restreindre les rôles d'accès du workspace et du Lakehouse (rôles OneLake, non stockés dans Git). La **durée de conservation** n'est pas décidée : à trancher avant la production.
- [ ] **Identifiants TalentSoft renouvelés** si le dépôt Git contient dans son historique d'anciennes versions avec des identifiants. Associer **uniquement** le dossier `TST_6` au workspace (jamais la racine du dépôt).
- [ ] **[Optionnel] Notification** : un compte d'envoi pour la connexion Outlook, et une liste de diffusion pour `notify_recipients`.

Décisions encore ouvertes (à régler avant la production) : reprise de l'historique des journaux ADP de l'existant (aucun chemin n'est prévu, voir README, décision T6-02) ; rétention des données personnelles ; capacité.

---

## 3. Étape A : préparer le workspace

1. Créer le workspace avec le **nom exact** décidé (paragraphe 1.3), l'associer à la capacité.
2. Vérifier le runtime par défaut : **1.3**.
3. Donner les rôles (développeur, exploitation). Ne donner aucun accès large au Lakehouse.
4. **Contrôle** : le nom du workspace contient `PRD` en majuscules **si et seulement si** c'est la production.

---

## 4. Étape B : connecter Git et synchroniser

1. Dans les paramètres du workspace, connecter Git sur le dépôt et la branche voulus, avec comme **dossier Git** `TEST_MOTUL/TST_6` uniquement.
2. Synchroniser (mettre à jour le workspace depuis Git).
3. **Contrôle** : les **20 items** suivants existent, sans erreur de synchronisation.

| Type | Nom dans Fabric |
|---|---|
| Lakehouse | `T6_LH_HRIS` |
| Variable library | `T6_VL_HRIS` |
| Pipelines | `T6_PL_HRIS_ORCHESTRATOR`, `T6_PL_HRIS_BOOTSTRAP`, `T6_PL_HRIS_MAIN`, `T6_PL_HRIS_NOTIFY_MAIL` |
| Notebooks quotidiens | `T6_NB_HRIS_LIB`, `T6_NB_HRIS_RUN_CONTROL`, `T6_NB_HRIS_TS_ACQUIRE`, `T6_NB_HRIS_REMUNERATION`, `T6_NB_HRIS_TRANSFORM`, `T6_NB_HRIS_COMPARE`, `T6_NB_HRIS_PROMOTE`, `T6_NB_HRIS_PUBLISH_ADP`, `T6_NB_HRIS_NOTIFY` |
| Notebooks du premier chargement | `T6_NB_HRIS_SETUP`, `T6_NB_HRIS_BOOTSTRAP`, `T6_NB_HRIS_REFERENCE_ADMIN`, `T6_NB_HRIS_REFERENCE_IMPORT` |
| Notebook de tests | `T6_NB_HRIS_TESTS` |

4. **Contrôle** : `T6_LH_HRIS` est un Lakehouse **à schémas** (sa définition porte `defaultSchema: dbo` **[V]** ; la création par la synchronisation est à confirmer **[T]**). Aucun lakehouse par défaut n'est attaché aux notebooks : c'est voulu, les tables sont adressées par nom complet à quatre parties.
5. **Contrôle** : `T6_PL_HRIS_ORCHESTRATOR` n'a **aucune planification active** (le dépôt la livre désactivée) et `T6_PL_HRIS_BOOTSTRAP` comme `T6_PL_HRIS_MAIN` n'en ont aucune.

Si un item est en erreur : lire le message de synchronisation, ne pas le recréer à la main. Un item recréé à la main n'a plus le même identifiant logique et casse les références entre pipelines et notebooks.

---

## 5. Étape C : activer le bon jeu de valeurs

Le **jeu de valeurs actif d'une variable library n'est ni dans Git ni déployé** : après la synchronisation, c'est le jeu de base (« DEV ») qui est actif. À faire **dans chaque workspace**, à chaque nouvel environnement.

1. Ouvrir `T6_VL_HRIS`, choisir le jeu de valeurs de l'environnement (`UAT` ou `PRD` ; rien à faire en DEV), **enregistrer**.
2. Vérifier les valeurs qui diffèrent :

| Variable | DEV (base) | UAT | PRD |
|---|---|---|---|
| `env_code` | `DEV` | `UAT` | `PRD` |
| `key_vault_url_adp` | *(vide)* | *(vide)* | `https://kvt-mge-azr-hris-ts-prd.vault.azure.net/` |
| `bootstrap_run_tests` | `true` | `true` | `false` |

3. Vérifier les valeurs communes à tous les environnements (ne pas les changer sans décision) :

| Variable | Valeur | Remarque |
|---|---|---|
| `lakehouse_name` | `T6_LH_HRIS` | Même nom partout |
| `key_vault_url` | `https://kvt-mge-azr-hris-ts-prd.vault.azure.net/` | Coffre des secrets TalentSoft |
| `ts_base_url` | `https://motul.talent-soft.com` | Seule valeur acceptée par le code |
| `ts_request_concurrency` / `ts_employee_concurrency` | `10` / `10` | Comme l'existant ; plage 1 à 40 |
| `business_timezone` | `Europe/Paris` | Date métier |
| `lock_lease_minutes` | `600` | Bail du verrou, supérieur à la somme des délais des activités |
| `remuneration_enabled` | `true` | Flux rémunération à chaque run |
| `adp_read_enabled` / `adp_write_enabled` | `false` / `false` | **Ne pas changer ici** (étape H) |
| `adp_base_url`, `insee_api_url` | `https://accounts.eu.adp.com/`, `https://geo.api.gouv.fr/communes` | Comme l'existant |
| `adp_max_attempts`, `adp_http_timeout_seconds`, `adp_pause_seconds` | `3`, `60`, `1` | Reprise explicite seulement |
| `notify_enabled` | `false` | Reste `false` tant que l'envoi n'est pas validé |
| `notify_recipients` | *(vide)* | Liste de diffusion, point-virgule comme séparateur |
| `notify_subject_prefix` | `[HRIS]` | Préfixe applicatif ; l'étiquette `[DEV]`, `[UAT]` ou `[PRD]` est ajoutée seule **devant** |

Toutes les variables sont de type texte (le type nombre n'est pas supporté dans les pipelines). Aucun secret dans la variable library.

4. **Contrôle de l'activation** : dans un notebook jetable du workspace (voir annexe B), lire `env_code` et vérifier qu'il correspond à l'environnement. **Rien dans le code ne contrôle la cohérence entre `env_code` et le nom du workspace** : c'est ce contrôle manuel qui évite de lancer un workspace PRD avec les valeurs DEV (sans coffre ADP).

---

## 6. Étape D : accès au coffre (Key Vault)

Les notebooks lisent les secrets par `notebookutils.credentials.getSecret` avec **l'identité de l'utilisateur courant** **[V]** (documentation Microsoft). L'identité réellement utilisée par un notebook lancé par un **pipeline** ou par une **planification** n'est **pas documentée** : elle doit être testée **[T]**.

1. Donner à l'identité d'exécution le droit de **lire les secrets** (rôle « Key Vault Secrets User » ou stratégie d'accès « Get », selon le mode d'autorisation du coffre). Lecture seule, jamais d'écriture.
2. Tester **vos droits** : lancer le notebook de l'annexe B (il n'affiche que « OK » ou « VIDE » et une longueur, jamais la valeur). Résultat attendu : `OK` pour `client-id-ts` et `client-secret-ts`.
3. Tester **le run lancé par pipeline** : lancer `T6_PL_HRIS_BOOTSTRAP` (étape G). Si la lecture échoue, l'extraction s'arrête avec le code `TS_SECRET_RESOLUTION` ou `SECRET_EMPTY` : corriger les **droits sur le coffre**, ne jamais mettre de valeur dans le code ni dans la variable library.
4. **[PRD]** Les secrets ADP ne sont lus que si la garde et un interrupteur le permettent (étape H) : les tester seulement à ce moment-là.

Tant que le droit de lecture n'est pas accordé, **les étapes G et suivantes sont en attente** (l'acquisition ne peut pas tourner). Les étapes E et F, elles, n'ont pas besoin du coffre.

---

## 7. Étape E : créer les tables (`T6_NB_HRIS_SETUP`)

Le setup crée les 6 schémas (`cfg`, `stg`, `ref`, `pub`, `ctl`, `log`), toutes les tables du modèle et les lignes de départ. Il **ne supprime ni ne modifie jamais** une table existante ; il est rejouable sans risque. Les tables de correspondance sont créées **vides**.

1. Ouvrir `T6_NB_HRIS_SETUP` et le lancer **seul**, sans paramètre (`p_test_schema` vide = tables réelles).
2. **Résultat attendu** : sortie avec `status = OK`.
3. **Si `status` n'est pas `OK`** : une table existante diffère du modèle (message « INCOMPATIBLE » avec le détail). Ne pas la supprimer à la main : analyser l'écart. Le pipeline de premier chargement s'arrêtera sur ce cas avec le code `HRIS_SETUP_INCOMPATIBLE`.
4. **Contrôle** : exécuter la requête de diagnostic de l'annexe A. La section `COMPTES` doit montrer toutes les tables, avec **0** ligne partout sauf `cfg.process`, `ctl.process_lock` et `cfg.ts_remuneration_mapping`, qui contiennent leurs lignes de départ.

Les tables s'affichent dans le point de terminaison SQL avec un éventuel retard ; en cas de doute, lire la table dans l'explorateur du Lakehouse (aperçu) ou actualiser le point de terminaison.

---

## 8. Étape F : reprendre les codes ADP

La correspondance **code TalentSoft vers code ADP** n'existe que dans 17 fichiers CSV de l'existant. Elle doit être reprise **une fois par environnement**. Deux voies passent par les mêmes contrôles ; la première est recommandée.

### Voie 1 : import des fichiers (`T6_NB_HRIS_REFERENCE_IMPORT`)

Exception décidée le 2026-11-04 à la règle « pas de fichier » : les fichiers sont déposés **temporairement** dans la zone `Files` du Lakehouse (pas un stockage externe) et **supprimés après l'import**.

1. **Récupérer les 17 CSV de production** : `classes`, `classification`, `contrat`, `contrat_v2`, `convention_collective`, `cost_center`, `diplome`, `entite`, `location_country`, `nationalite`, `organisationalstructure`, `pays`, `raison_debut_contrat`, `sexe`, `situation_matrimoniale`, `titre`, `work_accident` (extension `.csv`). Sur le SFTP, le séparateur est `;` ; dans le datalake de l'existant, `,` avec tous les textes entre guillemets : **les deux sont reconnus**.
2. Dans l'explorateur de `T6_LH_HRIS`, section **Files**, créer le dossier `reference_import` et y déposer les 17 fichiers.
3. **`CHECK`** : lancer `T6_NB_HRIS_REFERENCE_IMPORT` sans paramètre. Il n'écrit rien. Lire le résultat table par table : fichiers absents ou inattendus, colonnes lues par position (les en-têtes diffèrent du modèle pour les nationalités, les pays et les entités : c'est normal), codes ajoutés, cellules renseignées, doublons.
4. **Corriger les erreurs** éventuelles :
   - *Nombre de colonnes différent* : le fichier n'est pas celui attendu.
   - *Code en double avec des valeurs différentes* (connu pour la situation matrimoniale) : le **métier choisit** le bon code. Soit corriger le fichier, soit relancer avec `p_conflict_policy` à `FIRST` ou `LAST` (le choix est tracé dans le journal).
5. **`APPLY`** : relancer avec `p_mode = APPLY` et `p_change_ref` (référence du changement, obligatoire). Refusé tant qu'un run tient le verrou (`RUN_IN_PROGRESS`).
6. **Supprimer les fichiers** du dossier `reference_import`.
7. **Contrôle** : requête de diagnostic, section `COMPTES` : les 17 tables `cfg.lu_hr_*` contiennent des lignes ; section `JOURNAL_PAR_NOTEBOOK` : le notebook d'import apparaît.

Comportement de l'import **[V local, [T] dans Fabric]** : une valeur ADP **déjà saisie n'est jamais remplacée** (sauf `p_overwrite = true`) ; une cellule vide n'efface jamais une valeur ; la partie TalentSoft des tables n'est pas modifiée ; un code absent de la table est ajouté s'il porte une valeur ADP ; un second import identique ne change rien. La lecture Spark des fichiers n'a pas encore été essayée dans Fabric : en cas d'échec, recopier l'erreur.

### Voie 2 : saisie ou collage (`T6_NB_HRIS_REFERENCE_ADMIN`)

Pour les compléments, **après** la première extraction (l'outil n'ajoute aucune ligne : les codes absents de la table sont ignorés). En mode `REPORT`, il affiche la grille des codes TalentSoft sans code ADP, les plus employés d'abord. On la complète (ou on colle le résultat des requêtes `SELECT * FROM dwh.lu_hr_*` de Synapse), puis `CHECK`, puis `APPLY` avec `p_change_ref`. Après `APPLY`, vider `p_grid`.

### Après une modification du métier

Les tables sont un **instantané** : l'existant recopie les fichiers chaque jour depuis le SFTP, pas `TST_6`. Si le métier modifie ensuite un fichier, il faut rejouer l'import (`p_overwrite = true` sur les tables concernées) ou saisir le changement par `REFERENCE_ADMIN`.

---

## 9. Étape G : premier chargement (`T6_PL_HRIS_BOOTSTRAP`)

Pipeline lancé **à la main**. Il enchaîne : `Setup` (rejouable) → tests si demandés → `Plan` (choisit `INIT_REFERENCE` si la référence est vide, sinon `NORMAL`) → **un run complet** de l'orchestrateur → `Status` (action suivante).

**Préconditions** : étapes A à D faites (jeu de valeurs actif, droit sur le coffre) ; étape F faite (sinon le premier passage s'arrête sur `HRIS_ENTER_ADP_CODES`, ce qui est normal) ; **aucun verrou orphelin** (voir paragraphe 15).

1. Lancer `T6_PL_HRIS_BOOTSTRAP`.
   - `p_run_tests` : laisser vide (la variable `bootstrap_run_tests` décide : `true` en DEV et UAT, `false` en PRD). Passer `false` pour sauter les tests ; passer `true` pour les forcer.
   - **[DEV/UAT]** La campagne de tests prend de l'ordre de **40 minutes** (mesure du développeur) pour un délai maximal d'**1 heure** : une capacité chargée peut le dépasser. Voir le paragraphe 15.
   - **[PRD]** Ne pas lancer les tests.
2. Suivre les activités dans le suivi du pipeline : `Setup`, `Tests`, `Plan`, `RunOrchestrator`, `Status`.
3. Lire l'issue : le message de l'activité `Status` ou de l'erreur `HRIS_...` du pipeline.

| `next_action` (code d'erreur `HRIS_...`) | Signification | Que faire |
|---|---|---|
| `ENABLE_SCHEDULE` (pipeline vert) | Run complet abouti (`SUCCEEDED`, `NOT_PUBLISHED_NON_PRD`, `NOT_PUBLISHED_DISABLED` ou `DRY_RUN`) | Passer à l'étape I |
| `ENTER_ADP_CODES` | Correspondances sans code ADP (`MAPPING_TABLE_EMPTY`) | Compléter les codes (étape F, `REPORT` puis `APPLY`), relancer |
| `FIX_REMUNERATION` | Run salariés abouti, flux rémunération en échec | Rétablir l'accès au coffre TalentSoft ou passer `remuneration_enabled` à `false`, relancer |
| `RETRY_LATER` | Un autre run tenait le verrou | Relancer plus tard |
| `INVESTIGATE` | Échec ou blocage | Lire `ctl.run_step` et `log.anomaly` (annexe A) |
| `HRIS_SETUP_INCOMPATIBLE` | Une table existante diffère du modèle | Voir étape E |
| `HRIS_LOCK_BUSY` | Un run tient le verrou | Attendre la fin, ou libérer un verrou orphelin (paragraphe 15) |

4. **Contrôle** : requête de diagnostic de l'annexe A. Attendu : un run `INIT_REFERENCE` ou `NORMAL` avec les étapes `ACQUIRE`, `TRANSFORM`, `COMPARE`, `PROMOTE`, `PUBLISH_ADP`, `REMUNERATION` en `SUCCEEDED` (`PROMOTE` peut être `SKIPPED` si le run est bloqué), un `BILAN`, la référence `ref.ts_employee_adp` remplie.

Un échec laisse la référence **intacte**. Rejouer le pipeline est sans risque : il ne réinitialise jamais la référence.

---

## 10. Étape H : publication vers ADP (garde et interrupteurs)

Hors production, il n'y a **rien à faire** : la garde du nom de workspace interdit toute publication, quelle que soit la valeur des interrupteurs (statut `NOT_PUBLISHED_NON_PRD`).

**[PRD]** : la publication s'active **par paliers**, chacun par un **commit tracé** du jeu de valeurs `PRD` (fichier `QUOTIDIEN/VL_HRIS.VariableLibrary/valueSets/PRD.json`, section `variableOverrides`), puis synchronisation du workspace et **contrôle que le jeu `PRD` est bien actif**. Ne jamais modifier les interrupteurs dans le fichier de base `variables.json` (il s'applique à tous les environnements).

| Palier | Valeurs dans `PRD.json` | Effet | Condition d'entrée |
|---|---|---|---|
| 0 (livraison) | aucune (donc `false` / `false`) | `NOT_PUBLISHED_DISABLED` | Premier chargement réussi |
| 1 : lecture seule | `adp_read_enabled = true` | Lecture réelle d'ADP, opérations planifiées dans `log.adp_dry_run`, rien d'envoyé | Début de la non-régression en parallèle d'Azure (paragraphe 13), secrets ADP lisibles |
| 2 : écriture | `adp_read_enabled = true` et `adp_write_enabled = true` | Publication réelle | Non-régression réussie, bascule décidée et approuvée |

À chaque palier, **contrôler** :

- `log.adp_gate_decision` : une ligne par run, avec `ENV_SATISFIED` en PRD ;
- `pub.adp_outbox` par statut (section `OUTBOX` de l'annexe A) : `NOT_PUSHED_DISABLED`, `DRY_RUN_PLANNED`, `SENT`, `NO_CHANGE`, `FAILED` ;
- `log.run_summary` (section `BILAN`).

Règles de la publication réelle : un résultat **incertain** n'est **jamais renvoyé automatiquement** (réconciliation ou décision manuelle) ; une réponse 429 d'ADP n'est pas reprise seule ; la reprise passe par `T6_PL_HRIS_ORCHESTRATOR` avec `p_run_mode = RETRY_PUBLICATION` et `p_target_run_id` (identifiant d'un run déjà promu).

**Retour arrière immédiat** : remettre les deux interrupteurs à `false` (nouveau commit, synchronisation) ; le prochain run ne publie plus.

---

## 11. Étape I : planification, notification, exploitation

### 11.1 Planification

À faire seulement quand l'issue du premier chargement est `ENABLE_SCHEDULE`.

1. Activer la planification de `T6_PL_HRIS_ORCHESTRATOR` : tous les jours à **06:00, heure de Paris** (fuseau `Romance Standard Time`). Le dépôt la livre désactivée.
2. **[PRD]** Ne l'activer qu'**à la bascule** (paragraphe 13), jamais pendant le premier chargement.
3. Un **seul** point d'entrée planifié à la fois : ne pas planifier à la fois l'orchestrateur et `T6_PL_HRIS_MAIN`.
4. **Contrôle** : le lendemain, requête de diagnostic : un run du jour, `SUCCEEDED` (ou le statut attendu du palier ADP), et un run **par jour** seulement.

L'identité d'un run planifié n'est pas documentée **[T]** : utiliser un compte de service ayant accès au coffre, et vérifier le premier run planifié.

### 11.2 Notification de fin de run (optionnel, non validée)

Désactivée par défaut (`notify_enabled = false`). Pour l'activer dans un environnement :

1. Créer dans le portail une **connexion** de type Office 365 Outlook (par le compte d'envoi). Les connexions ne sont pas déployées par Git : à refaire dans **chaque** environnement.
2. Ouvrir `T6_PL_HRIS_NOTIFY_MAIL` (squelette) et y créer l'activité Outlook, destinataires, objet et corps reçus de `T6_NB_HRIS_NOTIFY`.
3. **Tester le rendu** (vert pour le succès, rouge pour l'échec, texte « OK / ÉCHEC » en plus de la couleur) dans Outlook web, poste et mobile, et l'expéditeur réel **[T]**.
4. Renseigner `notify_recipients` (liste de diffusion), puis `notify_enabled = true`. L'objet des messages est de la forme `[PRD] [HRIS] ÉCHEC - 2026-11-04` : l'étiquette d'environnement (`[DEV]`, `[UAT]`, `[PRD]`) est ajoutée **automatiquement** en tête. Elle vient du nom réel du workspace (`PRD` s'il le contient, comme la garde ADP), sinon de `env_code`. Un workspace sans `PRD` n'est jamais étiqueté `[PRD]` : si `env_code` dit `PRD` par erreur, l'objet porte `[HORS-PRD]`.
5. Planifier `T6_PL_HRIS_MAIN` **à la place** de l'orchestrateur, après validation du statut final qu'il remonte **[T]**.

### 11.3 Exploitation courante

- Lancer la requête de diagnostic (annexe A) après chaque run ou chaque matin.
- Situations courantes :

| Situation | Que faire |
|---|---|
| Run bloqué (`HRIS_RUN_BLOCKED`) | Lire `log.anomaly`, corriger la cause, relancer. La référence est intacte |
| Publication partielle | `RETRY_PUBLICATION` avec `p_target_run_id` |
| Résultat incertain | Réconciliation ou décision manuelle ; jamais de renvoi automatique |
| Verrou orphelin | Voir paragraphe 15 ; ne jamais supprimer la ligne de verrou |
| Codes TalentSoft sans code ADP après un nouveau code métier | `T6_NB_HRIS_REFERENCE_ADMIN` en `REPORT`, puis `CHECK`, `APPLY` (refusé pendant un run) |

---

## 12. Ce qui reste à prouver dans Fabric

À vérifier au premier run complet de **DEV**, puis rejouer les points qui dépendent de l'environnement.

| # | Point | État | Comment le lever |
|---|---|---|---|
| 1 | Chargement `%run` d'un dossier à l'autre, valeur de sortie d'un notebook lue par le pipeline, appel d'un sous-pipeline, création des schémas | **Observé au vert** au premier run (Setup, Plan, `RunStart`, appel de l'orchestrateur), à confirmer par `ctl.run_step` | Un run complet |
| 2 | Identité d'exécution pour lire le coffre et la variable library dans un run par pipeline et par planification | **[T]** | Étape D, point 3 ; premier run planifié |
| 3 | Lecture Spark des CSV de la zone `Files` | **[T]** | Étape F, `CHECK` |
| 4 | Statut final de `T6_PL_HRIS_MAIN` quand l'orchestrateur échoue ; taille d'une sortie de notebook | **[T]** | Avant d'activer la notification |
| 5 | Rendu des couleurs de l'activité Outlook, expéditeur, schéma de l'activité | **[T]** | Paragraphe 11.2 |
| 6 | Capacité : durée des tests (≈ 40 min pour 1 h de délai), durée de l'acquisition (délai de 3 h) | **[T]** | Mesurer à chaque environnement |
| 7 | Équivalence des résultats avec l'existant sur données réelles | **Jamais démontrée** | Non-régression (paragraphe 13) |
| 8 | Réponses réelles d'ADP (statuts, 429, résultats incertains) | **Jamais observées** | Palier 1 puis 2 |
| 9 | Création de schéma sans `spark.sql` | Limite technique connue | — |

---

## 13. Spécifique à la production : non-régression, bascule, retour arrière

Azure **reste la référence et continue de publier** pendant la non-régression. Fabric tourne **sans publier** (palier 0, puis lecture seule au palier 1).

### 13.1 Non-régression en parallèle (10 jours ouvrés)

1. Lancer `T6_PL_HRIS_ORCHESTRATOR` **juste après** le run Azure de 02:00 (mêmes entrées).
2. Comparer, chaque jour, au contenu (champ par champ, pas seulement les volumes) : extraction, transformation et delta, rejets, rémunération. Écarts neutralisables : identifiants de run, horodatages, ordre des lignes, espaces finaux des colonnes à longueur fixe. **Jamais neutralisables** : `NULL` contre vide, zéros de tête, troncatures, dates, montants. Comparer sans valeurs personnelles (empreintes, compteurs).
3. Passer au **palier 1** (lecture seule ADP) et comparer les opérations planifiées (`log.adp_dry_run`) au journal de la Function ADP du même jour.
4. **Critère de sortie** : 10 jours ouvrés consécutifs, comprenant une fin de mois, une embauche, une sortie, un changement de contrat et un changement de correspondance, **sans écart inexpliqué**. Un défaut corrigé remet le compteur à zéro. Le protocole détaillé est dans le rapport final de l'audit d'équivalence (§5, §11, §13).

### 13.2 Bascule

À faire après le **dernier** run Azure de 02:00, une fois la non-régression signée :

1. Arrêter le déclencheur Azure `DailyTrigger` et l'activité `Azure Function ADP`.
2. Passer au **palier 2** (écriture) par commit tracé du jeu `PRD`, synchroniser, vérifier que le jeu `PRD` est actif.
3. Lancer aussitôt `T6_PL_HRIS_ORCHESTRATOR` en `NORMAL`, sous surveillance. Contrôler l'écart de fenêtre entre l'extraction Fabric et l'export Azure du jour, et **republier** les salariés modifiés entre les deux lectures.
4. Activer la planification à 06:00 (paragraphe 11.1).

### 13.3 Retour arrière

1. Désactiver la planification et remettre les deux interrupteurs à `false` (commit, synchronisation).
2. Réactiver le déclencheur et la Function Azure.
3. Les tables Fabric restent intactes (historique Delta : 30 jours par défaut). **Les données ne reviennent pas avec un retour du code** : prévoir la restauration Delta si nécessaire.

---

## 14. Check-list de recette d'un environnement

| # | Contrôle | Preuve attendue | OK |
|---|---|---|---|
| 1 | Nom du workspace conforme (`PRD` si et seulement si production, en majuscules) | Nom relevé | |
| 2 | Runtime 1.3 par défaut | Paramètres du workspace | |
| 3 | Git connecté sur `TEST_MOTUL/TST_6` uniquement ; 20 items sans erreur | Liste des items | |
| 4 | `T6_LH_HRIS` à schémas ; aucun lakehouse par défaut attaché | Explorateur | |
| 5 | Jeu de valeurs actif correct ; `env_code` relu dans un notebook | Annexe B | |
| 6 | Droit de lecture du coffre testé (utilisateur, puis run par pipeline) | Annexe B ; premier run | |
| 7 | Aucune planification active au départ | Paramètres des pipelines | |
| 8 | `T6_NB_HRIS_SETUP` : `status = OK` ; 6 schémas, tables du modèle | Annexe A, `COMPTES` | |
| 9 | 17 tables `cfg.lu_hr_*` remplies ; fichiers de la zone `Files` supprimés | Annexe A ; explorateur | |
| 10 | Premier chargement : issue `ENABLE_SCHEDULE` | Sortie de `Status` | |
| 11 | Étapes du run en `SUCCEEDED` ; `ref.ts_employee_adp` remplie | Annexe A | |
| 12 | Garde ADP : `NOT_PUBLISHED_NON_PRD` hors PRD, `NOT_PUBLISHED_DISABLED` en PRD au palier 0 | `log.adp_gate_decision`, `pub.adp_outbox` | |
| 13 | Rôles d'accès restreints sur les tables à données personnelles | Rôles OneLake | |
| 14 | **[PRD]** Non-régression de 10 jours ouvrés signée | Compte rendu | |
| 15 | **[PRD]** Bascule et retour arrière décrits et approuvés | Procédure signée | |
| 16 | Planification activée seulement au bon moment, un seul point d'entrée | Paramètres des pipelines | |
| 17 | Si la notification est activée : l'objet du premier message commence par l'étiquette de l'environnement | Message reçu | |

---

## 15. Pièges déjà rencontrés

- **Une condition ne peut pas en contenir une autre** dans un pipeline Fabric (erreur au run, pas à la synchronisation). Corrigé dans l'orchestrateur le 2026-11-04 ; le contrôle statique du dépôt le vérifie désormais. Si un pipeline est modifié à la main dans le portail, ne pas imbriquer de conditions.
- **Verrou orphelin.** Un run qui prend le verrou puis dont le pipeline s'arrête sans passer par l'étape d'échec laisse un verrou tenu jusqu'à l'expiration du bail (600 minutes). Un nouveau premier chargement s'arrête alors avec `HRIS_LOCK_BUSY`, et `APPLY` des codes ADP est refusé. **Ne jamais supprimer la ligne de verrou.** Chemin prévu : lancer `T6_NB_HRIS_RUN_CONTROL` avec `p_mode = FAIL`, `p_run_id` = valeur de `holder_run_id` de `ctl.process_lock` (ou identifiant du run de l'orchestrateur dans le suivi) et un `p_error_message` libre. Le run passe en `FAILED` et le verrou est libéré s'il appartient à ce run.
- **Tables vides dans le point de terminaison SQL** alors que les notebooks sont verts : retard de synchronisation des métadonnées. Lire les tables dans l'explorateur du Lakehouse ou actualiser le point de terminaison.
- **Journal non écrit en cas d'erreur précoce.** Un notebook ne branche son journal sur le Lakehouse qu'à sa quatrième cellule : une erreur au chargement de la bibliothèque ou à la lecture de la variable library apparaît seulement dans la sortie du notebook dans le suivi.
- **Durée des tests** (≈ 40 minutes) proche du délai d'**1 heure** de l'activité `Tests` : le sauter hors DEV (`p_run_tests = false`).
- **Workspace nommé `PRD` en minuscules** : la garde n'est pas satisfaite, rien n'est jamais publié. **Autre workspace contenant `PRD`** : il pourrait publier (en écriture si les interrupteurs y sont `true`).
- **Jeu de valeurs non activé** : le workspace garde les valeurs de base. En PRD, le coffre ADP est alors vide et la lecture des secrets ADP échoue.
- **Secret dans le code** : jamais. Les valeurs ne sont que dans le coffre ; le dépôt ne contient que des noms de secrets. Toute valeur trouvée dans l'historique Git doit être renouvelée.
- **Fichiers de référentiels laissés dans `Files`** : ce sont des données métier. Les supprimer après l'import.

---

## 16. Annexes

### Annexe A : requête de diagnostic (un seul résultat à copier)

À exécuter dans le **point de terminaison SQL de `T6_LH_HRIS`** (nouvelle requête SQL). Elle prend le dernier run toute seule. Les messages sont tronqués à 300 caractères et expurgés par conception ; les relire avant de les partager. En cas d'erreur « Invalid object name » : actualiser le point de terminaison, ou la table n'existe pas (étape E).

```sql
WITH last_run AS (
    SELECT TOP 1 run_id FROM ctl.run ORDER BY started_at_utc DESC
),
res AS (
    -- 0 : heure du serveur, pour juger l'expiration du verrou
    SELECT 0 AS sec, CAST(1 AS bigint) AS ord, 'HEURE_UTC' AS section,
           CONVERT(varchar(19), SYSUTCDATETIME(), 120) AS c1, '' AS c2, '' AS c3, '' AS c4, '' AS c5, '' AS c6

    -- 1 : nombre de lignes de chaque table
    UNION ALL
    SELECT 1, CAST(ROW_NUMBER() OVER (ORDER BY t.n) AS bigint), 'COMPTES', t.n, CAST(t.v AS varchar(30)), '', '', '', ''
    FROM (
              SELECT 'ctl.run' AS n, COUNT_BIG(*) AS v FROM ctl.run
        UNION ALL SELECT 'ctl.run_step', COUNT_BIG(*) FROM ctl.run_step
        UNION ALL SELECT 'ctl.process_lock', COUNT_BIG(*) FROM ctl.process_lock
        UNION ALL SELECT 'cfg.process', COUNT_BIG(*) FROM cfg.process
        UNION ALL SELECT 'cfg.ts_remuneration_mapping', COUNT_BIG(*) FROM cfg.ts_remuneration_mapping
        UNION ALL SELECT 'ref.ts_employee_adp', COUNT_BIG(*) FROM ref.ts_employee_adp
        UNION ALL SELECT 'ref.ts_remuneration_snapshot', COUNT_BIG(*) FROM ref.ts_remuneration_snapshot
        UNION ALL SELECT 'stg.ts_employee_raw', COUNT_BIG(*) FROM stg.ts_employee_raw
        UNION ALL SELECT 'stg.ts_employee_source', COUNT_BIG(*) FROM stg.ts_employee_source
        UNION ALL SELECT 'stg.ts_employee_enriched', COUNT_BIG(*) FROM stg.ts_employee_enriched
        UNION ALL SELECT 'stg.ts_employee_reject', COUNT_BIG(*) FROM stg.ts_employee_reject
        UNION ALL SELECT 'stg.ts_employee_adp', COUNT_BIG(*) FROM stg.ts_employee_adp
        UNION ALL SELECT 'stg.ts_remuneration_snapshot', COUNT_BIG(*) FROM stg.ts_remuneration_snapshot
        UNION ALL SELECT 'pub.adp_outbox', COUNT_BIG(*) FROM pub.adp_outbox
        UNION ALL SELECT 'log.notebook_log', COUNT_BIG(*) FROM log.notebook_log
        UNION ALL SELECT 'log.anomaly', COUNT_BIG(*) FROM log.anomaly
        UNION ALL SELECT 'log.run_summary', COUNT_BIG(*) FROM log.run_summary
        UNION ALL SELECT 'log.comparison_summary', COUNT_BIG(*) FROM log.comparison_summary
        UNION ALL SELECT 'log.reject', COUNT_BIG(*) FROM log.reject
        UNION ALL SELECT 'cfg.lu_hr_emp_diplome', COUNT_BIG(*) FROM cfg.lu_hr_emp_diplome
        UNION ALL SELECT 'cfg.lu_hr_emp_nationalite', COUNT_BIG(*) FROM cfg.lu_hr_emp_nationalite
        UNION ALL SELECT 'cfg.lu_hr_emp_pays', COUNT_BIG(*) FROM cfg.lu_hr_emp_pays
        UNION ALL SELECT 'cfg.lu_hr_emp_situation_matrimoniale', COUNT_BIG(*) FROM cfg.lu_hr_emp_situation_matrimoniale
        UNION ALL SELECT 'cfg.lu_hr_emp_titre', COUNT_BIG(*) FROM cfg.lu_hr_emp_titre
        UNION ALL SELECT 'cfg.lu_hr_emp_sexe', COUNT_BIG(*) FROM cfg.lu_hr_emp_sexe
        UNION ALL SELECT 'cfg.lu_hr_emp_work_accident', COUNT_BIG(*) FROM cfg.lu_hr_emp_work_accident
        UNION ALL SELECT 'cfg.lu_hr_ppc_convention_collective', COUNT_BIG(*) FROM cfg.lu_hr_ppc_convention_collective
        UNION ALL SELECT 'cfg.lu_hr_ppc_classification', COUNT_BIG(*) FROM cfg.lu_hr_ppc_classification
        UNION ALL SELECT 'cfg.lu_hr_ppc_classes', COUNT_BIG(*) FROM cfg.lu_hr_ppc_classes
        UNION ALL SELECT 'cfg.lu_hr_geo_entite', COUNT_BIG(*) FROM cfg.lu_hr_geo_entite
        UNION ALL SELECT 'cfg.lu_hr_ppc_contrat', COUNT_BIG(*) FROM cfg.lu_hr_ppc_contrat
        UNION ALL SELECT 'cfg.lu_hr_ppc_contrat2', COUNT_BIG(*) FROM cfg.lu_hr_ppc_contrat2
        UNION ALL SELECT 'cfg.lu_hr_ppc_cost_center', COUNT_BIG(*) FROM cfg.lu_hr_ppc_cost_center
        UNION ALL SELECT 'cfg.lu_hr_ppc_location_country', COUNT_BIG(*) FROM cfg.lu_hr_ppc_location_country
        UNION ALL SELECT 'cfg.lu_hr_ppc_raison_debut_contrat', COUNT_BIG(*) FROM cfg.lu_hr_ppc_raison_debut_contrat
        UNION ALL SELECT 'cfg.lu_hr_organisationalstructure', COUNT_BIG(*) FROM cfg.lu_hr_organisationalstructure
    ) t

    -- 2 : configuration du processus
    UNION ALL
    SELECT 2, CAST(ROW_NUMBER() OVER (ORDER BY p.process_code) AS bigint), 'CONFIG_PROCESS',
           p.process_code, CAST(p.enabled AS varchar(5)), ISNULL(p.acquisition_mode, ''), ISNULL(p.publication_target, ''), '', ''
    FROM cfg.process p

    -- 3 : verrou du processus
    UNION ALL
    SELECT 3, CAST(ROW_NUMBER() OVER (ORDER BY k.process_code) AS bigint), 'VERROU',
           k.lock_status, ISNULL(k.holder_run_id, ''), CONVERT(varchar(19), k.acquired_at_utc, 120),
           CONVERT(varchar(19), k.lease_expires_at_utc, 120), CONVERT(varchar(19), k.released_at_utc, 120), ''
    FROM ctl.process_lock k

    -- 4 : les 5 derniers runs
    UNION ALL
    SELECT 4, CAST(ROW_NUMBER() OVER (ORDER BY r.started_at_utc DESC) AS bigint), 'RUNS',
           r.run_id, CONCAT(r.run_mode, ' / ', r.status), CAST(r.business_date AS varchar(10)),
           CONVERT(varchar(19), r.started_at_utc, 120), CONVERT(varchar(19), r.ended_at_utc, 120),
           CONCAT(ISNULL(r.error_code, ''), ' | ', LEFT(ISNULL(r.error_summary, ''), 300))
    FROM (SELECT TOP 5 * FROM ctl.run ORDER BY started_at_utc DESC) r

    -- 5 : étapes du dernier run
    UNION ALL
    SELECT 5, CAST(ROW_NUMBER() OVER (ORDER BY s.started_at_utc, s.attempt_no) AS bigint), 'ETAPES',
           s.step_code, CONCAT(s.status, ' (essai ', s.attempt_no, ')'),
           CONCAT(ISNULL(CAST(s.rows_in AS varchar(20)), '-'), ' -> ', ISNULL(CAST(s.rows_out AS varchar(20)), '-')),
           CONVERT(varchar(19), s.started_at_utc, 120), CONVERT(varchar(19), s.ended_at_utc, 120),
           LEFT(ISNULL(s.message, ''), 300)
    FROM ctl.run_step s JOIN last_run l ON s.run_id = l.run_id

    -- 6 : anomalies et contrôles du dernier run
    UNION ALL
    SELECT 6, CAST(ROW_NUMBER() OVER (ORDER BY a.created_at_utc) AS bigint), 'ANOMALIES',
           a.control_code, ISNULL(a.severity, ''), ISNULL(a.outcome, ''),
           CONCAT(ISNULL(CAST(a.metric_value AS varchar(30)), '-'), ' / ', ISNULL(CAST(a.threshold_value AS varchar(30)), '-')),
           '', LEFT(ISNULL(a.message, ''), 300)
    FROM log.anomaly a JOIN last_run l ON a.run_id = l.run_id

    -- 7 : comparaison puis bilan du dernier run
    UNION ALL
    SELECT 7, CAST(1 AS bigint), 'COMPARAISON', c.run_id,
           CONCAT('staging=', c.nb_staging, ' reference=', c.nb_reference),
           CONCAT('nouveaux=', c.nb_new, ' modifies=', c.nb_modified, ' inchanges=', c.nb_unchanged),
           CONCAT('absents=', c.nb_absent, ' rejetes=', c.nb_rejected),
           CAST(c.is_first_load AS varchar(5)), ''
    FROM log.comparison_summary c JOIN last_run l ON c.run_id = l.run_id
    UNION ALL
    SELECT 8, CAST(1 AS bigint), 'BILAN', r.run_id,
           CONCAT('lues=', r.mt_lignes_lues, ' inserts=', r.mt_lignes_inserts, ' rejets=', r.mt_lignes_rejets),
           CONCAT('outbox=', r.nb_outbox, ' envoyes=', r.nb_sent),
           CONCAT('sans_changement=', r.nb_no_change, ' echecs=', r.nb_failed),
           CONCAT('non_pousses=', r.nb_not_pushed), LEFT(ISNULL(r.ds_type_rejets, ''), 300)
    FROM log.run_summary r JOIN last_run l ON r.run_id = l.run_id

    -- 9 : outbox du dernier run, par statut
    UNION ALL
    SELECT 9, CAST(ROW_NUMBER() OVER (ORDER BY x.status) AS bigint), 'OUTBOX',
           ISNULL(x.status, ''), CAST(x.n AS varchar(20)), '', '', '', ''
    FROM (SELECT o.status, COUNT_BIG(*) AS n
          FROM pub.adp_outbox o JOIN last_run l ON o.run_id = l.run_id
          GROUP BY o.status) x

    -- 10 : quels notebooks ont écrit dans le journal depuis 2 jours, par niveau
    UNION ALL
    SELECT 10, CAST(ROW_NUMBER() OVER (ORDER BY x.item_name, x.lvl) AS bigint), 'JOURNAL_PAR_NOTEBOOK',
           ISNULL(x.item_name, ''), x.lvl, CAST(x.n AS varchar(20)), CONVERT(varchar(19), x.last_flush, 120), '', ''
    FROM (SELECT item_name, UPPER(level) AS lvl, COUNT_BIG(*) AS n, MAX(flushed_at_utc) AS last_flush
          FROM log.notebook_log
          WHERE logged_at_utc >= DATEADD(day, -2, SYSUTCDATETIME())
          GROUP BY item_name, UPPER(level)) x

    -- 11 : les 40 dernières erreurs et avertissements du journal (tous runs confondus)
    UNION ALL
    SELECT 11, CAST(ROW_NUMBER() OVER (ORDER BY j.logged_at_utc DESC) AS bigint), 'JOURNAL_ERREURS',
           ISNULL(j.item_name, ''), CONCAT(j.level, ' #', j.seq_no), CONVERT(varchar(19), j.logged_at_utc, 120),
           CONCAT(ISNULL(j.error_type, ''), ' @ ', ISNULL(j.error_location, '')), ISNULL(j.run_id, ''),
           LEFT(ISNULL(j.message, ''), 300)
    FROM (SELECT TOP 40 * FROM log.notebook_log
          WHERE UPPER(level) IN ('ERROR', 'WARNING', 'CRITICAL')
          ORDER BY logged_at_utc DESC) j
)
SELECT section, c1, c2, c3, c4, c5, c6
FROM res
ORDER BY sec, ord;
```

Lecture rapide : `VERROU` dit si un run tient encore le verrou (comparer à `HEURE_UTC`) ; `RUNS` et `ETAPES` situent où le run s'est arrêté ; `ANOMALIES` donne les contrôles bloquants ; `OUTBOX` donne l'état de la publication ADP ; `JOURNAL_PAR_NOTEBOOK` montre quels notebooks ont écrit dans le journal.

### Annexe B : cellules de contrôle (notebook jetable)

À coller dans un notebook **non versionné** du workspace. Aucune valeur secrète n'est affichée.

Lecture de la variable library (jeu actif, interrupteurs) :

```python
lib = notebookutils.variableLibrary.getLibrary("T6_VL_HRIS")
print("env_code            :", lib.env_code)
print("coffre ADP renseigne:", bool(lib.key_vault_url_adp))
print("adp_read_enabled    :", lib.adp_read_enabled)
print("adp_write_enabled   :", lib.adp_write_enabled)
print("bootstrap_run_tests :", lib.bootstrap_run_tests)
print("workspace           :", notebookutils.runtime.context.get("currentWorkspaceName"))
```

Lecture des secrets TalentSoft (état et longueur seulement) :

```python
kv = lib.key_vault_url
for name in ("client-id-ts", "client-secret-ts"):
    try:
        v = notebookutils.credentials.getSecret(kv, name)
        print(name, "OK" if v else "VIDE", len(v or ""))
    except Exception as e:
        print(name, "ERREUR", type(e).__name__)
```

### Annexe C : statuts d'un run (`ctl.run.status`)

| Statut | Sens |
|---|---|
| `STARTED` | Run ouvert, en cours |
| `BLOCKED` | Un contrôle a bloqué le run ; rien n'a été promu ni publié ; le pipeline échoue avec `HRIS_RUN_BLOCKED` |
| `FAILED` | Échec d'une étape ou du pipeline |
| `SKIPPED_CONCURRENT` | Un autre run tenait le verrou ; rien n'a été fait |
| `PROMOTED` / `SUCCEEDED` | Référence mise à jour ; publication terminée |
| `NOT_PUBLISHED_NON_PRD` | Workspace sans `PRD` : aucune publication |
| `NOT_PUBLISHED_DISABLED` | Workspace `PRD`, interrupteurs à `false` |
| `DRY_RUN` | Workspace `PRD`, lecture seule : opérations planifiées, rien d'envoyé |
| `PUBLISHED_PARTIAL` | Publication réelle avec des éléments en échec : `RETRY_PUBLICATION` |

### Annexe D : documents du dossier

- `README.md` : état d'avancement, décisions, comportement hérité, règles de développement, exploitation.
- `DOC/CONFORMITE_TST_6.md` : conformité aux demandes, indépendance, préparation à la production.
- `DOC/README/*.md` : une fiche par item (règle R21).
- `LOCAL_TESTS/` : contrôles et tests lancés sur le poste (`verify_static.py`, `test_notify_html.py`, `test_reference_import.py`).
