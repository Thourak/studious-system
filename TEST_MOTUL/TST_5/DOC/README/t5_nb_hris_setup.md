**Nom de l'objet**

T5_NB_HRIS_SETUP

**Type d'objet**

Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/ONESHOT/NB_HRIS_SETUP.Notebook/notebook-content.py

**Description fonctionnelle**

Ce notebook prépare le Lakehouse T5_LH_HRIS dont dépendent tous les autres notebooks du hub. Il crée, s'ils n'existent pas, les six schémas cfg, stg, ref, pub, ctl et log, puis toutes les tables du modèle de données, dont la table du journal de fonctionnement et celles du flux de rémunération. Il insère ensuite, uniquement si elles sont absentes, la ligne de paramétrage du processus, la ligne de verrou à l'état libéré et les trois lignes de correspondance du flux de rémunération. Une table existante n'est jamais supprimée, vidée, remplacée ni modifiée : son schéma est comparé à la définition attendue et tout écart est signalé comme incompatible.

**Dépendances**

Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules d'écriture dans les tables, d'étapes, de modèle de données et de journal. Il lit dans la variable library T5_VL_HRIS le nom du Lakehouse. Il est appelé en premier par le pipeline de premier chargement T5_PL_HRIS_BOOTSTRAP, qui s'arrête si une table est incompatible ; on peut aussi le lancer à la main après une évolution du modèle.

**Fonctionnement et logique de traitement**

Après la configuration et la préparation du Lakehouse, le notebook parcourt la définition de chaque table : il crée la table si elle manque, sinon il compare ses colonnes et leurs types au modèle. Il insère ensuite les graines absentes. Il renvoie un statut OK ou INCOMPATIBLE avec le nombre de tables créées et existantes ; le pipeline de premier chargement lit ce statut.

**Paramètres**

Le paramètre p_test_schema isole les tables pendant les tests ; vide, ce sont les tables réelles. Le paramètre eventpath reçoit les paramètres d'un événement Fabric.

**Sorties produites**

Le notebook crée les schémas et les tables du Lakehouse, insère les graines dans cfg.process, ctl.process_lock et cfg.ts_remuneration_mapping, et écrit son journal dans log.notebook_log. Les tables de correspondance sont créées vides : leur partie TalentSoft est remplie par l'acquisition et leurs codes ADP par T5_NB_HRIS_REFERENCE_ADMIN.

**Limitations connues et points d'attention**

Aucune migration automatique n'est faite : une évolution du modèle sur une table existante doit être appliquée par une opération dédiée et validée. Les seuils du processus sont laissés vides, comme dans l'existant, qui n'en avait pas.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
