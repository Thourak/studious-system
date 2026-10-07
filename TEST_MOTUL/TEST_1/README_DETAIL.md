# Hub RH TalentSoft → ADP sur Microsoft Fabric — fonctionnement détaillé

Ce document détaille chaque étape décrite dans la [vue d'ensemble](README.md). Il s'adresse aux personnes qui
développent, exploitent ou dépannent le hub. Les noms entre accents graves sont ceux des objets, tables, paramètres
et colonnes tels qu'ils apparaissent dans Fabric.

**Sommaire** — [1. Lancement](#etape-1) · [2. Destinataires](#etape-2) · [3. Ouverture](#etape-3) ·
[4. Vagues](#etape-4) · [4a. Ingestion](#etape-4a) · [4b. Transformation](#etape-4b) · [4c. Contrôle](#etape-4c) ·
[4d. Consolidation et publication](#etape-4d) · [5. Clôture](#etape-5) · [6. Notification](#etape-6) ·
[7. Statut final](#etape-7) · [Environnements](#environnements) · [Garde-fous](#garde-fous) ·
[Variables](#variables) · [Tables](#tables) · [Suivi et diagnostic](#suivi) · [Reprise et rejeu](#reprise) ·
[Fichiers](#fichiers) · [À confirmer](#a-confirmer)

---

<a id="etape-1"></a>
## 1. Lancement

**Déclenchement.** Le pipeline `MOTUL_PL_HRIS_Orchestrateur` est prévu pour s'exécuter chaque jour à 03:00, heure de
Paris. Cette planification ne fait pas partie du code versionné : elle se configure dans le portail Fabric, et
aucune exécution n'est autorisée en UAT sans accord explicite. Le pipeline peut aussi être lancé à la main.

**Paramètres du pipeline.**

| Paramètre | Défaut | Rôle |
|---|---|---|
| `p_flux` | `*` | `*` lance tous les flux récurrents actifs ; sinon une liste d'identifiants séparés par des virgules, par exemple `import_csv_ponctuel`. |
| `p_mode` | `incremental` | `incremental` traite ce qui a changé depuis le dernier état ; `complet` retraite tout ; `ponctuel` sert à l'import CSV ponctuel. |
| `p_date_traitement` | vide | Date du traitement au format `aaaa-mm-jj` ; vide signifie la date du jour à Paris. |
| `p_fichier` | vide | Nom du fichier déposé, pour l'import CSV ponctuel uniquement. |
| `p_rejouer_reussis` | `false` | `true` relance aussi les flux déjà réussis pour la même date. |
| `p_execution_id_reprise` | vide | Identifiant d'une exécution à reprendre : seuls ses flux non aboutis sont relancés. |

**Résolution de l'environnement, une seule fois.** L'activité `Resoudre variables env` lit les 19 variables de la
bibliothèque `VL_HRIS` dans le jeu de valeurs actif du workspace. Elle les assemble en un texte JSON stocké dans la
variable de pipeline `variables_env`, transmis tel quel à chaque notebook. Aucun notebook ne relit la bibliothèque
de son côté. La liste des variables figure dans la section [Variables](#variables).

**Identifiant et date.** L'activité `Definir execution id` prend l'identifiant du run Fabric (`RunId`), ou
l'identifiant à reprendre. L'activité `Definir date traitement` calcule la date du jour à Paris si aucune date n'est
fournie. Cette date est fixée une fois, pour que tous les notebooks d'une même exécution travaillent sur la même
journée, même si l'exécution passe minuit.

<a id="etape-2"></a>
## 2. Vérification des destinataires

L'activité `Verifier destinataires` appelle `MOTUL_nb_hris_notify` avec `action = verifier`. Le notebook lit dans le
coffre de l'environnement le secret dont le nom est porté par `env_notif_liste_distribution`, qui contient l'adresse
de la liste de diffusion. Il contrôle aussi les variables d'envoi. Il n'envoie rien et ne journalise jamais
l'adresse, seulement le nombre de destinataires.

L'ouverture dépend de cette activité en mode « terminée » et non « réussie ». Ainsi, un problème de messagerie
n'empêche pas le traitement des données ; il sera signalé par l'alerte technique Fabric au moment de la
notification. Cette vérification répond à un défaut de l'existant : les destinataires étaient lus après le premier
traitement, si bien qu'un échec précoce n'était notifié à personne.

<a id="etape-3"></a>
## 3. Ouverture de l'exécution

L'activité `Ouvrir execution` appelle `MOTUL_nb_hris_control` avec `action = ouvrir`. Le notebook enchaîne les
opérations suivantes.

1. **Socle.** Il crée, si elles n'existent pas, les neuf tables de socle (`cfg_*`, `ctl_*` et `gld_resume`), et
   ajoute les colonnes manquantes. Rejouer cette opération ne modifie aucune donnée.
2. **Configuration de référence.** Il remplace le contenu de `cfg_flux`, `cfg_dependance` et `cfg_qualite` par la
   configuration versionnée dans le notebook. Les trois environnements ont donc toujours la même configuration ;
   une modification manuelle de ces tables est écrasée à l'exécution suivante. Toute évolution passe par Git et le
   pipeline de déploiement.
3. **Détection de cycle.** Il trie tous les flux actifs selon leurs dépendances. Un cycle dans `cfg_dependance`
   arrête l'exécution avant tout traitement.
4. **Sélection des flux.** Il retient les flux demandés par `p_flux`. Le flux d'import ponctuel n'est lancé que
   s'il est demandé explicitement. Il écarte les flux déjà réussis pour la même date, sauf si `p_rejouer_reussis`
   vaut `true`. En reprise, il recharge le plan de l'exécution visée et ne garde que ses flux non aboutis.
5. **Variables requises.** Il vérifie que chaque variable utilisée par les flux retenus existe et n'est pas vide.
   Une variable manquante arrête l'exécution avec la liste des variables absentes.
6. **Plan par vagues.** Il range les flux en vagues : une vague contient les flux dont toutes les dépendances sont
   dans les vagues précédentes. Une vague est découpée si elle dépasse le parallélisme déclaré dans
   `cfg_flux.parallelisme_max`. Le plan est complété par des vagues vides jusqu'aux six vagues du pipeline ; un plan
   plus profond fait échouer l'ouverture.
7. **Journal.** Il écrit dans `ctl_execution` l'environnement, le workspace, le déclencheur, les paramètres, le plan,
   la version du code et l'heure de début.

Le notebook renvoie le plan au pipeline, qui le mémorise dans la variable `plan` :

```json
{"execution_id": "…", "environnement": "DEV", "date_traitement": "2026-10-06",
 "vagues": [[{"flux_id": "ref_correspondances", "notebook": "MOTUL_nb_hris_ingest"}],
            [{"flux_id": "ts_employe", "notebook": "MOTUL_nb_hris_ingest"}, "…"], "…", []]}
```

**Si l'ouverture échoue**, le pipeline envoie immédiatement un courriel d'échec portant le message d'erreur
(`Notifier echec ouverture`), puis se termine en échec (`Echec ouverture`).

<a id="etape-4"></a>
## 4. Exécution par vagues

**Pourquoi des vagues fixes.** Fabric n'autorise pas une boucle `ForEach` à l'intérieur d'une autre. Le pipeline
enchaîne donc six boucles `Vague 1` à `Vague 6`, chacune parallèle sur les flux de sa vague. Une vague vide ne lance
rien.

**Ce que fait chaque boucle.** Pour chaque flux, une activité `Switch` appelle le notebook indiqué par le plan,
avec `execution_id`, `flux_id`, `date_traitement`, `mode` et `variables_env`. Au plus quatre flux tournent en même
temps. Un notebook non prévu fait échouer l'itération avec un message explicite.

**Le plan actuel**, issu de la configuration de référence :

| Vague | Flux | Notebook | Dépend de |
|---|---|---|---|
| 1 | `ref_correspondances` | ingest | — |
| 2 | `ts_employe` | ingest | `ref_correspondances` |
| 2 | `sftp_base_salary`, `sftp_bonus_percentage`, `sftp_bonus_target` | ingest | `ref_correspondances` |
| 3 | `trf_sirh` | transform | `ts_employe`, `ref_correspondances` |
| 3 | `trf_remuneration` | transform | les trois flux `sftp_*` |
| 4 | `ctl_sirh`, `ctl_remuneration` | control | la transformation correspondante |
| 5 | `pub_sirh`, `pub_remuneration` | publish | le contrôle correspondant |
| — | `import_csv_ponctuel` | ingest | inactif tant que sa table cible n'est pas définie |

**Délais et reprises automatiques.** Fabric n'accepte que des valeurs fixes pour les politiques d'activité. Elles
reprennent les valeurs déclarées dans `cfg_flux` pour les flux de chaque notebook et doivent rester alignées sur
elles.

| Notebook | Délai maximal | Reprises automatiques |
|---|---|---|
| ingest | 45 min | 2, à 60 s d'intervalle |
| transform | 1 h | 0 |
| control | 30 min | 0 |
| publish | 30 min | 0 |
| notify | 15 min | 0, le notebook gère ses propres reprises d'envoi |

**Un échec n'arrête pas les flux indépendants.** Chaque vague attend la fin de la précédente, qu'elle ait réussi ou
non. Au démarrage, chaque notebook relit le plan et vérifie que ses dépendances ont abouti dans l'exécution
courante. Si ce n'est pas le cas, il ne traite rien et enregistre le statut `Bloqué` avec le motif, par exemple
`Non traité : dépendance non aboutie (trf_remuneration : Échec)`. Ainsi, un échec de la branche rémunération
n'empêche pas la branche salariés d'aller au bout.

<a id="etape-4a"></a>
### 4a. Ingestion — `MOTUL_nb_hris_ingest`

Le notebook charge les données sans règle métier. Le champ `traitement` de `cfg_flux.parametres` choisit le
traitement.

| Traitement | Flux | Source | Destination |
|---|---|---|---|
| `referentiels` | `ref_correspondances` | 17 fichiers CSV du dossier de référence du SFTP | `cfg_mapping`, et copie brute dans `Files/reference/` |
| `export_json_ts` | `ts_employe` | Export JSON des salariés, déposé par l'Azure Function sur le compte ADLS | `stg_ts_employe` |
| `export_csv_sftp` | `sftp_base_salary`, `sftp_bonus_percentage`, `sftp_bonus_target` | Exports de rémunération du SFTP TalentSoft | `stg_sftp_*` |
| `depot_csv_ponctuel` | `import_csv_ponctuel` | Fichier déposé dans `Files/landing/` | Table `brz_*` déclarée dans `cfg_flux` |

**Référentiels.** Les dix-sept fichiers de correspondance TalentSoft → ADP sont tous obligatoires. Leurs colonnes
sont lues par position, comme le faisaient les tables externes Synapse. Un entier invalide ou un code TalentSoft en
double arrête le chargement. `cfg_mapping` est historisée : une correspondance modifiée ou disparue est clôturée la
veille (`date_fin`), une nouvelle version commence le jour du traitement (`date_debut`).

**Export des salariés.** Le notebook prend le fichier `json_motul_talentsoft_aaaammjj.json` le plus récent dont la
date ne dépasse pas la date de traitement. Il lit la date dans le nom au lieu de comparer les noms comme
l'existant. Il reproduit l'analyse JSON de la vue Synapse d'origine : chaque valeur est un texte tronqué à
50 caractères, un objet imbriqué devient vide, le téléphone professionnel prend le mobile puis le fixe, et la
valeur `None` de la date de fin de contrat devient une chaîne vide.

**Exports de rémunération.** Les fichiers CSV séparés par des virgules sont téléchargés en mémoire, sans copie
disque. Les colonnes attendues sont contrôlées sans tenir compte de la casse, et une valeur vide devient nulle
comme dans la copie Synapse.

**Import ponctuel.** Le nom du fichier est validé avant toute lecture et un fichier déjà chargé est refusé grâce à
son empreinte. Le fichier est lu sans inférence de type, puis son schéma, ses types et les règles de qualité sont
contrôlés. Le chargement Delta est transactionnel, puis le fichier part en archive, ou en rejet avec un fichier
`.motif.json`. Ce flux reste inactif tant que sa table cible et ses colonnes ne sont pas définies.

Chaque table `stg_*` est remplacée à chaque ingestion et porte les colonnes de traçabilité `execution_id`,
`date_traitement`, `fichier_source`, `date_fichier_source` et `horodatage_ingestion`.

<a id="etape-4b"></a>
### 4b. Transformation — `MOTUL_nb_hris_transform`

Le notebook reporte en PySpark les procédures stockées Synapse, en reproduisant volontairement leurs
particularités, à valider par le métier lors de la réconciliation.

**Salariés (`trf_sirh`).**

1. **Delta.** La staging `stg_ts_employe` est comparée à l'état de la veille `slv_ts_employe_etat`. Seules les lignes
   nouvelles ou modifiées sont retenues, comme le faisait l'instruction `EXCEPT` de l'existant. En mode `complet`,
   toute la staging est retenue.
2. **Correspondances.** Quinze jointures sur `cfg_mapping` ajoutent les codes ADP, en ignorant les espaces de fin
   comme SQL Server. Le résultat est écrit dans `slv_ts_employe_prepare`.
3. **Rejets.** Les 36 règles de `cfg_qualite` reprennent les trois familles d'origine : information obligatoire
   vide (`Null_Rejet`), format de matricule, de date ou de pourcentage (`Format_Rejet`) et correspondance absente
   (`Référence_Rejet`). Chaque défaut produit une ligne dans `rjt_ts_employe`, avec la colonne, la valeur et le
   motif.
4. **Format paie.** Un salarié qui porte au moins un rejet est exclu. Les autres forment le lot de l'exécution dans
   `slv_ts_employe_adp_lot` : dates converties, et salaire mensuel égal au salaire de base arrondi au centime divisé
   par douze, sur 13 décimales dont la dernière est tronquée, comme le faisait SQL Server.
5. **État.** `slv_ts_employe_etat` n'est remplacée qu'en dernier, après le succès de toutes les écritures.

**Rémunération (`trf_remuneration`).** Le notebook compare chaque staging `stg_sftp_*` à son état
`slv_sftp_*_etat`, puis reproduit les deux vues de l'existant :

- **Salaire de base.** Une évolution de montant ou de date produit une ligne avec le pourcentage d'augmentation.
  Le résultat va dans `slv_base_salary_changes_lot`.
- **Bonus.** Le notebook produit les nouvelles entrées, la clôture de l'ancienne entrée la veille de la nouvelle
  date, et la clôture d'une entrée disparue. Le résultat va dans `slv_bonus_changes_lot`.
- **Format.** Les montants sont arrondis au centime et écrits avec une virgule décimale.

Un montant non numérique arrête le traitement, comme l'existant.

Chaque table de lot porte `execution_id` : rejouer une exécution remplace ses propres lignes, sans doublon. Le
volume de la source est journalisé, ce qui permet ensuite de distinguer un lot vide d'une ingestion vide.

<a id="etape-4c"></a>
### 4c. Contrôle — `MOTUL_nb_hris_control`, action `controler`

- **Résumé salariés (`ctl_sirh`).** Le notebook écrit une ligne dans `gld_resume`. Elle contient les salariés lus,
  insérés et rejetés, le taux de rejet à quatre décimales, les types de rejet sur 50 caractères et les motifs sur
  500 caractères, comme la procédure de résumé d'origine.
- **Résumé rémunération (`ctl_remuneration`).** Le notebook reproduit les indicateurs de l'ancienne requête
  `mail_variables` : volumes source et nombres d'évolutions, dans la colonne `indicateurs` de `gld_resume`.
- **Traçabilité qualité.** `ctl_qualite` reçoit une ligne par règle, avec le nombre de lignes en défaut.
- **Contrat ADP.** Les règles exhaustives du contrat ADP ne sont pas encore fournies. En PRD, le contrôle est donc
  marqué `Bloqué` et la publication ADP est refusée. Hors PRD, il est marqué « Non applicable ». Aucune règle n'est
  inventée.
- **Seuil de rejet.** Aucun seuil n'est appliqué tant que `cfg_flux.seuil_rejet_pct` n'est pas renseigné.

<a id="etape-4d"></a>
### 4d. Consolidation et publication — `MOTUL_nb_hris_publish`

Pour chaque jeu de données, trois tables ont des rôles distincts :

| Jeu | Lot de l'exécution | Table cumulative | Table finale |
|---|---|---|---|
| Salariés au format paie | `slv_ts_employe_adp_lot` | `gld_ts_employe_adp_cumul` | `gld_ts_employe_adp` |
| Évolutions de salaire de base | `slv_base_salary_changes_lot` | `gld_base_salary_changes_cumul` | `gld_base_salary_changes` |
| Évolutions de bonus | `slv_bonus_changes_lot` | `gld_bonus_changes_cumul` | `gld_bonus_changes` |

1. **Validation du lot.** Le notebook vérifie que la transformation et le contrôle du flux ont réussi pour cette
   exécution. Il vérifie aussi que la source n'était pas vide : une source vide signale probablement une anomalie
   d'ingestion. Un lot non validé ne touche ni la table cumulative, ni la table finale, ni les systèmes externes.
2. **Lecture du lot.** Les doublons exacts sont supprimés. Deux lignes contradictoires portant la même clé
   arrêtent le traitement, au lieu d'en choisir une arbitrairement.
3. **Table cumulative, en upsert uniquement.** Une clé nouvelle est insérée ; une clé existante n'est mise à jour
   que si ses valeurs changent ; une clé absente du lot est conservée. Il n'y a jamais de suppression ni de
   remplacement global. Un lot plus ancien que celui qui a écrit une ligne ne l'écrase pas. Cette consolidation est
   de type SCD 1 : elle garde la dernière valeur connue, pas l'historique des anciennes valeurs. Une historisation
   SCD 2 est disponible par configuration (`strategie_cumul = scd2`) si ce besoin est confirmé. Les colonnes
   `cumul_*` indiquent l'exécution, la date et l'heure du lot qui a inséré ou modifié chaque ligne.
4. **Table finale, remplacée par le dernier lot.** Toutes les lignes du lot courant, y compris celles qui n'ont pas
   changé dans la table cumulative, remplacent le contenu de la table finale par `INSERT OVERWRITE`. La table n'est
   ni supprimée ni recréée : son nom, son identifiant, sa structure et ses propriétés sont conservés. L'identité du
   lot est inscrite dans le même commit Delta. Le notebook refuse ainsi qu'une exécution plus ancienne ou
   concurrente écrase un résultat plus récent. Un lot valide mais vide vide la table finale
   (`politique_lot_vide = vider`, comme l'existant) ou la conserve (`conserver`).
5. **Publication depuis la table finale.** Elle n'a lieu qu'après toutes les écritures :

| Environnement | Salariés au format paie | Évolutions de rémunération |
|---|---|---|
| DEV, UAT | Table finale du Lakehouse local | Table finale du Lakehouse local |
| PRD | Envoi ADP bloqué tant que la tâche MIG-027 n'est pas réalisée | Dépôt sur le SFTP d'import TalentSoft |

En PRD, les fichiers `RemunSalaryHistoIE_InsertAndUpdate_fr-FR_1_<jjmmaaaa>.csv` et
`RemunTargetBonusHistoIE_InsertAndUpdate_fr-FR_2_<jjmmaaaa>.csv` reprennent le format de l'existant. Ils sont séparés
par des points-virgules, entièrement entre guillemets, avec des fins de ligne Windows. Ils sont déposés sous un nom
temporaire, puis renommés. Un fichier déjà déposé pour l'exécution n'est jamais renvoyé.

**Pas de transaction entre les deux tables.** Si la consolidation réussit et que le remplacement de la table finale
échoue, l'étape échoue sans rien publier. Une reprise de la même exécution rejoue alors la consolidation sans effet,
puis remplace la table finale et publie.

<a id="etape-5"></a>
## 5. Clôture

L'activité `Cloturer execution` appelle `MOTUL_nb_hris_control` avec `action = cloturer`, quelle que soit l'issue
des vagues. Pour chaque flux planifié, le notebook retient le statut de sa dernière tentative. Un flux qui n'a
laissé aucune trace est marqué `Bloqué` si l'une de ses dépendances a échoué, sinon `Échec`.

| Statut global | Condition |
|---|---|
| `Succès` | Tous les flux ont réussi, sans rejet. |
| `Succès avec rejets` | Tous les flux ont réussi, au moins un avec des lignes rejetées. |
| `Échec partiel` | Au moins un flux en échec ou bloqué, et au moins un flux réussi. |
| `Échec` | Aucun flux n'a réussi. |

Le statut, l'heure de fin et une synthèse par statut sont écrits dans `ctl_execution`.

<a id="etape-6"></a>
## 6. Notification

L'activité `Notifier resultat` appelle `MOTUL_nb_hris_notify` avec `action = envoyer`.

- **Contenu.** En cas de succès, le courriel donne le statut, l'environnement, la date, les volumes par flux, les
  résumés et le lien du rapport (`env_powerbi_rapport_url`). En cas d'échec, il donne en plus le diagnostic réel de
  chaque étape en échec ou bloquée, issu de `ctl_execution_etape`. Il ne contient jamais de donnée personnelle.
- **Objet.** `[HRIS][<environnement>] Statistiques disponibles — <date>`, ou `… Échec de l'exécution — <date>`.
- **Envoi.** Le courriel part par l'API Microsoft Graph `sendMail`, depuis la boîte technique
  `env_notif_bal_technique`. L'identité d'exécution est choisie par `env_notif_auth_mode` : `identite_workspace`, ou
  `certificat` avec une application Entra ID.
- **Fiabilité.** Seules les erreurs transitoires (429, 5xx, réseau) sont réessayées, trois fois au plus, jamais une
  erreur 401 ou 403. Une notification déjà envoyée pour l'exécution n'est jamais renvoyée. Un échec d'envoi laisse
  les données intactes et porte le statut `Notification en échec`.
- **Pièce jointe.** La pièce jointe des rejets n'est produite que si `pieces_jointes = rejets`. Elle contient des
  données nominatives et exige une validation préalable du métier.

<a id="etape-7"></a>
## 7. Statut final du pipeline

L'activité `Verifier statut global` lit le statut renvoyé par la clôture. S'il ne vaut ni `Succès` ni
`Succès avec rejets`, l'activité `Statut global en echec` termine le pipeline en échec avec le statut dans son
message. L'alerte technique native de Fabric prend alors le relais pour l'exploitation. Une clôture en échec
termine aussi le pipeline en échec (`Echec cloture`).

---

<a id="environnements"></a>
## Environnements

Les workspaces `WS-HRIS-DEV`, `WS-HRIS-UAT` et `WS-HRIS-PRD` contiennent exactement les mêmes huit objets, avec le
même code. La promotion DEV → UAT → PRD passe uniquement par le pipeline de déploiement Fabric. Le pipeline appelle
les notebooks par leur identifiant logique : le déploiement relie automatiquement chaque appel au notebook du
workspace cible. Après le premier déploiement dans un workspace, il faut activer une fois le jeu de valeurs de
`VL_HRIS` correspondant à l'environnement.

<a id="garde-fous"></a>
## Garde-fous

- **Environnement exact.** `env_nom` doit valoir exactement `DEV`, `UAT` ou `PRD`.
- **Pas de croisement.** Un notebook refuse de s'exécuter si le nom du workspace porte le marqueur d'un autre
  environnement, par exemple des valeurs DEV dans un workspace PRD.
- **Publication externe en PRD seulement.** Une publication externe n'est autorisée que si `env_nom` vaut `PRD` et
  si le nom du workspace contient le mot `PRD`. Sinon, le résultat reste dans le Lakehouse local.
- **Secrets jamais exposés.** Les secrets sont lus à l'exécution dans le coffre de l'environnement, par leur nom.
  Ils ne sont jamais écrits dans une table, un fichier ou un journal.

<a id="variables"></a>
## Variables d'environnement (`VL_HRIS`)

Toutes les variables sont déclarées de type String, car les pipelines ne prennent pas en charge le type Number.
Aucune ne porte la valeur d'un secret.

| Variable | Rôle |
|---|---|
| `env_nom` | Environnement : `DEV`, `UAT` ou `PRD` |
| `env_lakehouse_nom` | Nom du Lakehouse |
| `env_kv_url` | Adresse du coffre de secrets de l'environnement |
| `env_adls_compte`, `env_adls_filesystem` | Compte et conteneur ADLS où l'Azure Function dépose l'export TalentSoft |
| `env_sftp_hote`, `env_sftp_port`, `env_sftp_utilisateur` | Connexion au SFTP TalentSoft |
| `env_sftp_export_chemin`, `env_sftp_import_chemin`, `env_sftp_reference_chemin` | Dossiers d'export, d'import et de référentiels du SFTP |
| `env_secret_sftp` | Nom du secret du mot de passe SFTP |
| `env_notif_liste_distribution` | Nom du secret contenant l'adresse de la liste de diffusion |
| `env_notif_bal_technique` | Boîte technique expéditrice |
| `env_powerbi_rapport_url` | Lien du rapport inséré dans le courriel de succès |
| `env_notif_auth_mode` | `identite_workspace` ou `certificat` |
| `env_tenant_id`, `env_notif_client_id`, `env_notif_cert_secret` | Locataire, application et nom du secret de certificat, en mode `certificat` seulement |

<a id="tables"></a>
## Tables du Lakehouse

| Zone | Tables | Écrite par | Mode d'écriture |
|---|---|---|---|
| Configuration | `cfg_flux`, `cfg_dependance`, `cfg_qualite` | control (ouverture) | Remplacées par la configuration de référence |
| Configuration | `cfg_mapping` | ingest | Recalculée et historisée par date de début et de fin |
| Staging | `stg_ts_employe`, `stg_sftp_*` | ingest | Remplacées à chaque ingestion |
| État de la veille | `slv_ts_employe_etat`, `slv_sftp_*_etat` | transform | Remplacées après une transformation réussie |
| Préparé | `slv_ts_employe_prepare` | transform | Lignes remplacées par exécution |
| Lots | `slv_*_lot` | transform | Lignes remplacées par exécution |
| Rejets | `rjt_ts_employe` | transform | Lignes remplacées par exécution, partition par date |
| Cumulatif | `gld_*_cumul` | publish | Upsert uniquement |
| Final | `gld_ts_employe_adp`, `gld_base_salary_changes`, `gld_bonus_changes` | publish | Contenu remplacé par `INSERT OVERWRITE` |
| Résumé | `gld_resume` | control | Ligne remplacée par exécution et par flux |
| Suivi | `ctl_execution`, `ctl_execution_etape`, `ctl_qualite`, `ctl_fichier_traite` | tous | Voir [Suivi et diagnostic](#suivi) |

<a id="suivi"></a>
## Suivi et diagnostic

- `ctl_execution` contient une ligne par exécution : environnement, paramètres, plan, version du code, début, fin
  et statut global.
- `ctl_execution_etape` contient deux lignes par tentative d'étape : une au début (`En cours`) et une à la fin,
  avec la durée, les volumes lus, écrits et rejetés, et le message d'erreur réel. Une étape restée `En cours`
  au-delà de son délai signale un traitement bloqué.

Les étapes principales s'appellent `ingest`, `transform`, `control` et `publish`. Les sous-étapes, comme
`publish:final:<table>` ou `publish:sftp:<fichier>`, détaillent la publication. Les étapes `ouvrir`, `cloturer` et
`notify:*` concernent l'exécution entière.

Pour diagnostiquer une exécution, depuis le point de terminaison SQL du Lakehouse :

```sql
SELECT flux_id, etape, tentative, statut, lignes_lues, lignes_ecrites, lignes_rejetees, message_erreur
FROM dbo.ctl_execution_etape
WHERE execution_id = '<identifiant>' AND fin IS NOT NULL
ORDER BY horodatage;
```

<a id="reprise"></a>
## Reprise et rejeu

- **Reprendre une exécution en échec** : relancer le pipeline avec `p_execution_id_reprise` égal à son
  identifiant. Seuls les flux non aboutis sont relancés, sous le même identifiant et à la même date. L'heure de
  début d'origine est conservée, car elle ordonne les lots.
- **Relancer une date déjà traitée** : par défaut, les flux réussis sont écartés. `p_rejouer_reussis = true` force
  leur relance.
- **Idempotence** : chaque table de lot est remplacée pour l'exécution, la table cumulative n'est mise à jour que
  si les valeurs changent, un fichier SFTP ou un courriel déjà envoyé ne l'est pas une seconde fois, et un fichier
  ponctuel déjà chargé est refusé.
- **Rattrapage** : `p_mode = complet` ignore les états de la veille et retraite toute la staging.

<a id="fichiers"></a>
## Fichiers encore utilisés

Les échanges entre traitements passent tous par des tables Delta. Les fichiers Parquet sous-jacents aux tables ne
sont jamais manipulés directement. Les fichiers restants sont imposés par les sources ou par les contrats
d'échange :

| Fichier | Notebook | Rôle | Nécessaire ? |
|---|---|---|---|
| Export JSON des salariés sur ADLS | ingest | Source déposée par l'Azure Function TalentSoft | Oui, tant que l'extraction n'est pas reprise dans Fabric (MIG-026) |
| Trois exports CSV de rémunération sur le SFTP | ingest | Source TalentSoft, lue en mémoire | Oui, contrat TalentSoft |
| Dix-sept référentiels CSV sur le SFTP | ingest | Source des correspondances | Oui, contrat TalentSoft |
| Copie des référentiels dans `Files/reference/` | ingest | Trace brute du dernier chargement | Non indispensable : remplaçable par `cfg_mapping` seul |
| Import ponctuel dans `Files/landing`, `archive`, `rejet` | ingest | Processus d'import ponctuel | Oui, pour les imports ponctuels |
| Deux CSV déposés sur le SFTP d'import, en PRD | publish | Retour des évolutions de rémunération à TalentSoft | Oui, contrat TalentSoft |
| CSV des rejets joint au courriel | notify | Pièce jointe produite en mémoire, sur demande | Optionnel |

La publication ADP n'utilise aucun fichier à ce stade : son canal reste à définir (MIG-027).

<a id="a-confirmer"></a>
## Points à confirmer et limites

- **Clés de consolidation** de chaque jeu : `id_unique` ; `employeenumber` et `type` ; `employeenumber`, `type` et
  `start_date`.
- **Stratégie de cumul** : upsert de type SCD 1 retenu par défaut, ou historisation SCD 2.
- **Lignes contradictoires** dans un lot : règle de déduplication à définir ; elles arrêtent aujourd'hui le
  traitement.
- **Lot valide mais vide** : vider la table finale, comme aujourd'hui, ou conserver le résultat précédent.
- **Contrat ADP** : règles exhaustives et canal de publication à fournir (MIG-018, MIG-027).
- **Réseau** : connectivité de Fabric vers le compte de stockage privé et le SFTP, à établir (MIG-006).
- **Messagerie** : droits d'envoi de la boîte technique et choix de l'identité d'exécution (MIG-005, MIG-028).
- **Exploitation** : parallélisme, délais et reprises des étapes de transformation, de contrôle et de publication
  sont des valeurs provisoires. La durée de conservation des tables de lot, cumulatives et de suivi reste à fixer.
- **Particularités reproduites de l'existant**, à valider lors de la réconciliation :
  - une valeur nulle n'est pas un rejet d'information obligatoire ;
  - une date vide devient le 1er janvier 1900 ;
  - le pays fiscal n'est jamais contrôlé ;
  - la clôture d'un bonus disparu porte une date de fin vide.
- **Validation dans Fabric** : ce code a été testé sur un Spark local avec des données synthétiques. Il n'a pas
  encore été exécuté dans un workspace Fabric.
