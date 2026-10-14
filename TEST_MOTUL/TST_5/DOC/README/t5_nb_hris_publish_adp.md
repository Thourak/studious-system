**Nom de l'objet**

T5_NB_HRIS_PUBLISH_ADP

**Type d'objet**

Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/QUOTIDIEN/NB_HRIS_PUBLISH_ADP.Notebook/notebook-content.py

**Description fonctionnelle**

Ce notebook transmet à l'outil de paie ADP les salariés nouveaux ou modifiés du run, avec la même logique que l'ancienne fonction Azure de publication : décision entre embauche et mise à jour, contrôle du numéro de sécurité sociale, lecture de chaque valeur en place dans ADP et envoi seulement de ce qui diffère. La publication n'a lieu que si le nom réel du workspace d'exécution contient PRD, lu au moment de la décision. Deux interrupteurs de la variable library permettent en plus d'activer la publication par étapes : désactivée, lecture seule, puis lecture et écriture. Un refus de publication ne bloque jamais les traitements du Lakehouse.

**Dépendances**

Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules de garde, de transport, d'opérations, de logique ADP, de publication et de journal. Il lit dans la variable library T5_VL_HRIS l'adresse du coffre ADP, l'adresse d'ADP, l'adresse de l'interface des communes, les deux interrupteurs, le nombre de tentatives, le délai des appels et la pause entre deux salariés. Il lit dans le coffre les secrets client-id-adp, client-secret-adp, MOTUL-SA-cer et motulsa-auth-key, seulement si la garde et un interrupteur l'autorisent. Il lit ctl.run, pub.adp_outbox et stg.ts_employee_adp. Il est appelé par le pipeline T5_PL_HRIS_ORCHESTRATOR dans l'activité Publish, après une promotion réussie.

**Fonctionnement et logique de traitement**

Après la configuration et la préparation du Lakehouse, la publication enregistre d'abord la décision de la garde. Hors production, les salariés à transmettre passent au statut NOT_PUSHED_NON_PRD sans aucun appel. En production avec les deux interrupteurs à false, ils passent au statut NOT_PUSHED_DISABLED, sans lecture de secret. En lecture seule, le notebook lit ADP et planifie chaque opération sans l'envoyer, dans log.adp_dry_run, pour la comparer au journal de l'existant pendant la non-régression. En lecture et écriture, il enregistre chaque opération de façon durable avant de l'envoyer, classe chaque réponse et ne renvoie jamais automatiquement une opération dont le résultat est incertain.

**Paramètres**

Le paramètre p_run_id reçoit l'identifiant d'exécution du pipeline et il est obligatoire. Le paramètre p_run_mode vaut NORMAL pour publier le run en cours ou RETRY_PUBLICATION pour republier les éléments en échec d'un run déjà promu, sans recalcul. Le paramètre p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric. Aucun paramètre ne permet de forcer une publication.

**Sorties produites**

Le notebook écrit log.adp_gate_decision, pub.adp_outbox, pub.adp_operation, pub.adp_attempt, log.adp_publication_log, log.adp_dry_run, ctl.run, ctl.run_step et son journal log.notebook_log. Les tables de publication contiennent des données personnelles et restent en accès restreint. Il renvoie au pipeline un résultat court de compteurs.

**Limitations connues et points d'attention**

Aucun workspace autre que celui de production ne doit contenir la suite de lettres PRD dans son nom, par exemple PREPRD. L'activation de l'écriture se fait par un commit tracé du jeu de valeurs PRD, après la non-régression et la bascule. Une réponse 429 d'ADP n'est pas reprise automatiquement : la reprise passe par le mode RETRY_PUBLICATION.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
