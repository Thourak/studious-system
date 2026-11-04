**Nom de l'objet**

T6_NB_HRIS_NOTIFY

**Type d'objet**

Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.

**Chemin dans le dépôt**

TEST_MOTUL/TST_6/QUOTIDIEN/NB_HRIS_NOTIFY.Notebook/notebook-content.py

**Description fonctionnelle**

Ce notebook prépare le message de fin de run destiné à l'équipe d'exploitation. Il lit le registre du run, ses étapes et ses contrôles, puis construit un objet de courriel et un corps HTML lisible dans les clients courants : un bandeau vert quand le run a réussi, rouge quand il a échoué, a été bloqué ou s'est arrêté avant sa fin, orange pour les cas à surveiller (publication partielle, publication désactivée ou en lecture seule, run ignoré). Chaque étape et chaque contrôle porte une pastille de la même couleur accompagnée d'un libellé écrit, afin que l'information ne repose jamais sur la couleur seule. Le notebook n'envoie rien lui-même : il renvoie au pipeline le sujet, la liste des destinataires et le HTML, et c'est l'activité Outlook d'un pipeline d'envoi dédié qui expédie le message.

**Dépendances**

Le notebook charge la bibliothèque commune par %run T6_NB_HRIS_LIB et lit les tables ctl.run, ctl.run_step et log.anomaly du Lakehouse T6_LH_HRIS, alimentées par les autres notebooks du run. Il lit dans la variable library T6_VL_HRIS les variables notify_enabled, notify_recipients, notify_subject_prefix et env_code, et lit le nom réel du workspace dans le contexte d'exécution. Il n'utilise aucun secret : l'adresse de destination est celle d'une liste de diffusion, pas celle d'une personne. Il est appelé par T6_PL_HRIS_MAIN, une fois T6_PL_HRIS_ORCHESTRATOR terminé, réussi ou non ; l'orchestrateur lui-même n'est pas modifié.

**Fonctionnement et logique de traitement**

Après la configuration et la préparation du Lakehouse, le notebook vérifie qu'il reçoit p_run_id ou p_since_utc, retrouve le run (le plus récent démarré depuis p_since_utc, avec cinq minutes de marge, quand p_run_id est vide), lit la ligne du run puis les étapes et les contrôles correspondants. Cette recherche évite de dépendre d'une propriété de sortie de l'activité de lancement, qui n'est pas documentée. Pour chaque étape, seule la dernière tentative est retenue. Il classe les statuts en succès, échec, attention ou incomplet, construit le HTML avec des styles en ligne et une mise en page en tableaux, seule technique fiable dans les clients de messagerie, puis vérifie que le message reste sous la limite de taille choisie ; si besoin, il réduit le nombre de lignes affichées. L'envoi n'est demandé que si la variable notify_enabled vaut true et si au moins une adresse valide est configurée ; sinon le notebook renvoie la raison de l'absence d'envoi. Tous les textes issus des tables sont échappés, tronqués et masqués par la fonction d'assainissement de la bibliothèque : le message ne contient ni donnée personnelle ni secret. L'objet du courriel commence par l'étiquette de l'environnement entre crochets, [DEV], [UAT] ou [PRD], suivie du préfixe notify_subject_prefix, du libellé du statut et de la date métier. L'étiquette [PRD] vient du nom réel du workspace, selon la règle de la garde de publication ADP (le nom contient PRD) ; sinon elle reprend env_code, et un workspace sans PRD n'est jamais présenté comme la production ([HORS-PRD]).

**Paramètres**

Le paramètre p_run_id reçoit l'identifiant du run HRIS à décrire. S'il est vide, le paramètre p_since_utc, reçu du pipeline sous la forme de l'heure de déclenchement, permet de retrouver le run le plus récent démarré depuis cet instant ; l'un des deux est obligatoire. Le paramètre p_test_schema isole les tables pendant les tests ; vide, il désigne les tables réelles.

**Sorties produites**

Aucune table n'est écrite, hors le journal du notebook dans log.notebook_log. Le notebook renvoie au pipeline un résultat JSON : status, send, reason, subject, recipients, html, run_status, category, html_bytes et run_id. Quand l'envoi n'est pas demandé, le champ html est vide pour garder la sortie courte.

**Limitations connues et points d'attention**

La taille maximale d'une valeur de sortie de notebook n'est pas documentée par Microsoft : le corps est donc limité à 40 000 octets, à valider sur un run réel. Le rendu des couleurs dans l'activité Outlook de Fabric n'est pas non plus documenté et doit être vérifié dans Outlook sur le web, sur le poste et sur mobile. Aucun enregistrement n'empêche encore un double envoi si le pipeline est rejoué avec le même run : la table de suivi des envois est prévue à l'étape suivante. Les valeurs réelles des colonnes outcome de log.anomaly autres que FAIL n'ont pas été observées dans Fabric.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
