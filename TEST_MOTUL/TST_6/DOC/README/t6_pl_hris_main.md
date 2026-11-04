**Nom de l'objet**

T6_PL_HRIS_MAIN

**Type d'objet**

Pipeline de données Microsoft Fabric (DataPipeline).

**Chemin dans le dépôt**

TEST_MOTUL/TST_6/QUOTIDIEN/PL_HRIS_MAIN.DataPipeline/pipeline-content.json

**Description fonctionnelle**

Ce pipeline est le point d'entrée « un clic » avec notification. Il lance T6_PL_HRIS_ORCHESTRATOR et attend sa fin, prépare le message de fin de run avec T6_NB_HRIS_NOTIFY, l'envoie par T6_PL_HRIS_NOTIFY_MAIL seulement si la notification est activée dans la variable library, puis fait échouer le pipeline si le run a échoué, afin que l'échec reste visible dans le suivi de Fabric. Il a été créé à côté de l'orchestrateur, sans le modifier, parce que la façon dont Fabric calcule le statut final d'un pipeline quand une branche « Terminée » suit un échec n'est pas documentée : un branchement dans l'orchestrateur aurait pu masquer les codes HRIS_RUN_FAILED et HRIS_RUN_BLOCKED. Il n'est pas planifié : l'orchestrateur reste le point d'entrée planifié tant que ce pipeline n'a pas été validé dans Fabric.

**Dépendances**

Il appelle T6_PL_HRIS_ORCHESTRATOR (activité RunHub), le notebook T6_NB_HRIS_NOTIFY (activité NotifyPrepare) et le pipeline T6_PL_HRIS_NOTIFY_MAIL (activité SendMail). Il n'a pas d'autre dépendance.

**Fonctionnement et logique de traitement**

RunHub exécute l'orchestrateur avec les mêmes quatre paramètres et attend son résultat. NotifyPrepare s'exécute quand RunHub est terminé, réussi ou en échec, et reçoit l'heure de déclenchement : le notebook retrouve ainsi le run le plus récent démarré depuis cet instant, sans lire de propriété de sortie de l'activité de lancement, dont l'existence n'est pas documentée. IfSend n'appelle SendMail que si le résultat du notebook contient send = true ; avec la configuration par défaut (notify_enabled à false), aucun message n'est préparé pour envoi. PropagateFailure fait échouer le pipeline avec le code HRIS_RUN_FAILED quand RunHub a échoué et que l'étape de notification est terminée ; si la préparation du message échoue elle-même, c'est cette activité qui fait échouer le pipeline.

**Paramètres**

p_process_code (TS_ADP_EMPLOYEE), p_run_mode (NORMAL), p_target_run_id et p_business_date_override (vides) : identiques à ceux de l'orchestrateur et transmis tels quels.

**Sorties produites**

Aucune table propre. Le message est préparé et, si la notification est activée, envoyé par T6_PL_HRIS_NOTIFY_MAIL une fois l'activité Outlook créée.

**Limitations connues et points d'attention**

À vérifier dans Fabric avant de remplacer la planification de l'orchestrateur : le statut final du pipeline pour chacun des cas (run réussi, échec, run bloqué, échec de la préparation du message, échec de l'envoi) ; l'existence de la propriété error.message de RunHub utilisée dans le message d'échec ; l'expression qui lit la valeur de sortie d'un notebook ; la propagation des codes d'erreur de l'orchestrateur, qui peut perdre leur précision dans le pipeline parent. Aucun enregistrement n'empêche un double envoi si le pipeline est rejoué : la table de suivi des envois reste à créer. Une exécution de l'orchestrateur lancée seule ne déclenche aucune notification.

**Responsable et contact**

Thibault Delalande, développeur du hub. Le responsable définitif de la maintenance en production reste à désigner.
