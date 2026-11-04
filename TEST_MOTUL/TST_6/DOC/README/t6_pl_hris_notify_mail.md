**Nom de l'objet**

T6_PL_HRIS_NOTIFY_MAIL

**Type d'objet**

Pipeline de données Microsoft Fabric (DataPipeline). État : squelette, sans activité.

**Chemin dans le dépôt**

TEST_MOTUL/TST_6/QUOTIDIEN/PL_HRIS_NOTIFY_MAIL.DataPipeline/pipeline-content.json

**Description fonctionnelle**

Ce pipeline est destiné à envoyer le message de fin de run préparé par T6_NB_HRIS_NOTIFY. Il est volontairement vide : le schéma JSON de l'activité Office 365 Outlook (type Office365Email dans la référence des définitions de pipeline) n'est confirmé que par un résumé de la documentation, sans exemple complet, et une définition erronée dans le dépôt pourrait faire échouer la synchronisation Git. L'activité doit donc être créée dans le portail Fabric, avec une connexion propre au compte d'envoi, puis le pipeline doit être enregistré dans Git. Il isole l'envoi dans un pipeline dédié parce que Microsoft indique que les connexions de messagerie sont rattachées à un utilisateur et ne sont pas faites pour être partagées entre auteurs.

**Dépendances**

Il reçoit ses trois paramètres de T6_NB_HRIS_NOTIFY, via le pipeline appelant. Il dépendra d'une connexion Fabric de type Office 365 Outlook, à créer dans le portail et propre à chaque environnement, car les connexions ne sont pas déployées par le Git de Fabric. Il est appelé par T6_PL_HRIS_MAIN, seulement quand le résultat de T6_NB_HRIS_NOTIFY indique send = true ; l'orchestrateur n'en dépend pas.

**Fonctionnement et logique de traitement**

À compléter à l'étape de branchement : une activité Outlook dont le destinataire est p_recipients, l'objet p_subject et le corps p_html, exécutée seulement quand le résultat du notebook indique send = true. Points à vérifier sur un run réel avant toute mise en service : le rendu des couleurs du corps HTML dans Outlook sur le web, sur le poste et sur mobile ; l'échappement des guillemets par l'éditeur de contenu dynamique ; la taille maximale acceptée pour le corps ; l'adresse d'expéditeur réellement utilisée selon le mode d'authentification (compte utilisateur de service, identité de l'espace de travail ou principal de service).

**Paramètres**

p_subject : objet du message, valeur par défaut vide. p_recipients : adresses séparées par un point-virgule, valeur par défaut vide. p_html : corps HTML, valeur par défaut vide. Les trois sont renvoyés par T6_NB_HRIS_NOTIFY.

**Sorties produites**

Aucune pour l'instant. Une fois l'activité ajoutée, un message électronique et l'état du run dans le suivi de Fabric.

**Limitations connues et points d'attention**

Aucun envoi n'est possible tant que l'activité n'est pas créée. Si le pipeline est rejoué avec le même run, le message est renvoyé : la table de suivi des envois est prévue à l'étape suivante. L'activité est inactive dans un espace de travail cible tant qu'une connexion utilisateur n'y a pas été recréée, et un premier déploiement échoue si la personne qui déploie n'a pas accès à la connexion utilisée.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
