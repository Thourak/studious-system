**Nom de l'objet**

T5_LH_HRIS

**Type d'objet**

Lakehouse.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/QUOTIDIEN/LH_HRIS.Lakehouse

**Description fonctionnelle**

Ce Lakehouse contient toutes les données du hub : paramètres et tables de correspondance, données de chaque run, référence des salariés et de la rémunération, liste à publier, suivi des publications, registre des runs, contrôles et journaux de fonctionnement. Il remplace les dossiers datés et les tables externes du lac de données de l'ancien traitement Azure. Il est propre à TST_5 et ne partage aucune table avec une autre version.

**Dépendances**

Il est créé par la synchronisation du workspace avec le dépôt, avec les schémas activés. Ses tables sont créées par T5_NB_HRIS_SETUP. Tous les notebooks du projet y accèdent par des noms complets à quatre parties, sans Lakehouse par défaut, le nom venant de la variable library T5_VL_HRIS.

**Fonctionnement et logique de traitement**

Les tables sont rangées en six schémas : cfg pour les paramètres et les correspondances, stg pour les données de chaque run, ref pour la référence validée, pub pour la liste à publier et le suivi des opérations, ctl pour le registre, les étapes et le verrou, log pour les contrôles, les bilans et les journaux. Chaque table n'est écrite que par Spark, par remplacement ciblé sur l'identifiant du run ou par fusion, jamais par suppression globale.

**Paramètres**

Aucun.

**Sorties produites**

Les tables du hub. Onze d'entre elles contiennent des données personnelles et doivent rester en accès restreint.

**Limitations connues et points d'attention**

Les rôles d'accès aux données ne sont pas enregistrés dans le dépôt : en production, ils doivent être définis dans le workspace pour restreindre l'accès aux données personnelles. L'historique des tables est conservé trente jours par le format Delta ; une politique de conservation des données de run reste à décider.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
