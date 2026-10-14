**Nom de l'objet**

T5_NB_HRIS_BOOTSTRAP

**Type d'objet**

Notebook, exécuté dans Microsoft Fabric avec le moteur Synapse PySpark.

**Chemin dans le dépôt**

TEST_MOTUL/TST_5/ONESHOT/NB_HRIS_BOOTSTRAP.Notebook/notebook-content.py

**Description fonctionnelle**

Ce notebook prend les deux décisions du premier chargement du hub. Avant le run, il choisit le mode du run quotidien comme le fait la comparaison : si la référence des salariés est vide, le run se fait en INIT_REFERENCE pour la construire ; sinon il se fait en NORMAL. Le premier chargement peut donc être relancé autant de fois que nécessaire sans jamais réinitialiser la référence. Après le run, il lit l'issue et en déduit une seule action suivante : activer la planification, saisir les codes ADP manquants, corriger le flux de rémunération, relancer plus tard ou analyser un incident.

**Dépendances**

Le notebook charge la bibliothèque commune par %run T5_NB_HRIS_LIB et utilise ses modules de premier chargement, de correspondances, de modèle de données et de journal. Il lit ref.ts_employee_adp, ctl.process_lock, ctl.run, ctl.run_step, log.anomaly et les tables de correspondance. Il est appelé deux fois par le pipeline T5_PL_HRIS_BOOTSTRAP, dans les activités Plan et Status ; on peut aussi le lancer à la main en mode STATUS.

**Fonctionnement et logique de traitement**

Après la configuration et la préparation du Lakehouse, le mode PLAN compte les lignes de la référence et lit le verrou : si un run tient le verrou et que son bail n'a pas expiré, le statut renvoyé arrête le pipeline avant tout run. Le mode STATUS retrouve le run lancé depuis le déclenchement du premier chargement, lit le dernier statut de chaque étape et les contrôles bloquants, déduit l'action suivante et affiche, si des codes ADP manquent, la grille des codes TalentSoft sans code ADP, les plus employés en premier.

**Paramètres**

Le paramètre p_mode vaut PLAN ou STATUS. Le paramètre p_since reçoit l'heure de déclenchement du pipeline de premier chargement, en temps universel, et il est obligatoire en mode STATUS. Le paramètre p_max_lines limite le nombre de codes affichés par table dans la grille et vaut cent par défaut. Le paramètre p_test_schema isole les tables pendant les tests et eventpath reçoit les paramètres d'un événement Fabric.

**Sorties produites**

Le notebook n'écrit que son journal dans log.notebook_log. Il renvoie au pipeline le mode choisi ou l'action suivante et son message ; le pipeline échoue avec ce message pour toute action autre que l'activation de la planification.

**Limitations connues et points d'attention**

Le run retenu par le mode STATUS est le plus récent depuis le déclenchement du premier chargement : la planification quotidienne doit rester désactivée tant que le premier chargement n'est pas terminé. En production, les runs complets sans publication, parce que la garde ou les interrupteurs l'empêchent, sont considérés comme aboutis.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
