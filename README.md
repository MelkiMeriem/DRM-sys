# Système de Gestion de Ressources Distribuées (DRM)

Mini-orchestrateur distribué simplifié: un client envoie un job, le cluster élit un leader, la requête est redirigée si besoin, le job est exécuté une seule fois et son état est traçable via API, scripts et dashboard web.

## 1) Objectif du projet

Ce projet répond à un énoncé de système distribué avec:
- communication inter-processus via `gRPC` + `Protocol Buffers`
- élection dynamique d'un leader (style Raft simplifié)
- cohérence de traitement côté leader
- prévention des doublons par idempotence (`client_request_id`)
- tolérance aux pannes basique (failover + reprise à partir d'un log local)

## 2) Définitions théoriques (soutenance)

- **gRPC**: framework RPC haute performance. Il définit des services/méthodes dans un fichier `.proto`, puis génère automatiquement les stubs client/serveur. Avantages: contrat fort, sérialisation binaire rapide, interopérabilité.
- **Protocol Buffers (Protobuf)**: format de sérialisation compact et typé. Les messages (`SubmitJobRequest`, `Job`, etc.) sont normalisés par schéma.
- **Consensus distribué**: famille d'algorithmes qui permettent à plusieurs nœuds de se mettre d'accord sur un état commun malgré pannes partielles.
- **Paxos**: algorithme de consensus classique (proposer/acceptor/learner), robuste mais complexe à implémenter correctement.
- **Raft**: algorithme de consensus conçu pour être plus compréhensible; basé sur termes, vote majoritaire et heartbeats d'un leader.
- **Idempotence**: même requête métier répétée (même identifiant) produit le même résultat logique sans dupliquer le traitement.
- **Tolérance aux pannes**: capacité du système à continuer à traiter après panne d'un nœud (avec dégradation contrôlée).

## 3) Choix d'implémentation dans ce projet

Le système implémente **Raft simplifié** (pas Paxos), avec:
- `requestVote` pour l'élection
- `appendEntries` comme heartbeat leader
- redirection d'un follower vers le leader
- idempotence côté jobs via `client_request_id -> job_id`
- journal local par nœud pour recharger l'état minimal au redémarrage

Important: ce projet vise la clarté pédagogique et les scénarios de l'énoncé, pas une implémentation Raft complète de production.

### Note sur l'exécution des jobs

Dans la version actuelle, le **leader exécute les jobs** après redirection/forwarding.
Il ne s'agit pas d'un ordonnanceur multi-workers qui répartit réellement l'exécution sur tous les nœuds.
Donc voir `executedByNodeId = node1` (si `node1` est leader) est attendu et correct pour ce design.

## 4) Architecture

Monorepo Maven multi-modules:
- `proto`: contrat gRPC (`drm.proto`) + classes générées
- `node`: nœud serveur Spring Boot (gRPC + dashboard web)
- `client`: client CLI gRPC
- `scripts`: démarrage/arrêt cluster et tests de validation

Composants principaux côté nœud:
- `RaftNode`: rôle/terme/leader, élection, heartbeats
- `JobServiceImpl`: API jobs, forwarding vers leader
- `ClusterServiceImpl`: RPC inter-nœuds + mode panne simulée
- `JobManager`: exécution jobs, idempotence, persistance locale
- `DashboardController`: endpoints web/API du dashboard

## 5) Prérequis

- Java 17+
- Maven 3.9+
- Linux/macOS (scripts bash)

## 6) Compilation

Depuis la racine:

```bash
mvn -DskipTests package
```

Artefacts principaux:
- `node/target/node-1.0.0-SNAPSHOT.jar`
- `client/target/client-1.0.0-SNAPSHOT-all.jar`

## 7) Exécution - Méthode Console (CLI)

### Démarrage cluster

```bash
bash scripts/start-cluster.sh
```

Ce script:
- build le projet
- lance `node1`, `node2`, `node3`
- écrit PID/logs dans `.run/`

### Envoi de jobs avec le client

```bash
# Message
java -jar client/target/client-1.0.0-SNAPSHOT-all.jar msg '{"message":"bonjour"}' req-msg-1

# Somme
java -jar client/target/client-1.0.0-SNAPSHOT-all.jar sum '{"numbers":[10,10]}' req-sum-1

# SHA-256
java -jar client/target/client-1.0.0-SNAPSHOT-all.jar sha256 '{"text":"hello"}' req-hash-1
```

### Arrêt cluster

```bash
bash scripts/stop-cluster.sh
```

## 8) Exécution - Méthode Dashboard Web

Dashboard disponible sur:
- `http://localhost:8081`

Le dashboard centralise:
- état du nœud local (role, leader, compteur de jobs)
- état du cluster (table des nœuds, termes, leader, jobs, mode panne ON/OFF)
- test manuel de soumission (`msg`, `sum`, `sha256`)
- rejeu idempotent ("Rejouer même requête")
- suivi d'un job par `job_id`
- historique jobs récents
- logs locaux du nœud dashboard

### Utilisation rapide du panneau "Test manuel"

- choisir cible (`random`, `node1`, `node2`, `node3`)
- choisir type:
  - `msg` attend `{"message":"bonjour"}`
  - `sum` attend `{"numbers":[1,2,3]}`
  - `sha256` attend `{"text":"hello"}`
- cliquer `Envoyer`
- copier le `jobId`, puis `Suivre`
- pour vérifier l'idempotence: cliquer `Rejouer (même requête)`

## 9) Scénarios de test fournis

Exécution complète:

```bash
bash scripts/run_all_tests.sh
```

Ce script valide:
1. redirection follower -> leader puis succès
2. même `client_request_id` soumis deux fois -> un seul job logique
3. panne leader -> réélection -> nouvelle requête réussie
4. restart d'un nœud -> rechargement état minimal depuis log local
5. clients concurrents avec même `request_id` -> pas de doublon

Le dashboard propose aussi des presets de scénario, mais ils restent **manuels**: pour les cas failover/restart/concurrence, il faut réaliser l'action correspondante côté terminal.

## 10) Flux d'une requête (vue système)

1. Le client contacte un nœud choisi aléatoirement.
2. Si ce nœud est follower, il forward la requête au leader.
3. Le leader enregistre ou retrouve le `job` via idempotence.
4. Le job passe `PENDING -> RUNNING -> SUCCEEDED/FAILED`.
5. Le résultat est récupéré par polling (`GetJob`) et affiché (CLI/dashboard).

## 11) Limites connues (assumées)

- consensus simplifié (pas de réplication complète de log Raft)
- persistance locale par nœud, pas stockage distribué fort
- exécution de job centralisée autour du leader (pas scheduler multi-workers avancé)

Ces choix sont volontaires pour rester lisible et conforme à un projet académique simplifié.

## 12) Dépannage rapide

- ports occupés:
  - `bash scripts/stop-cluster.sh`
- cluster instable juste après démarrage:
  - attendre 3-5 secondes (temps d'élection)
- erreurs de payload:
  - vérifier le JSON attendu par le type (`msg`, `sum`, `sha256`)

## 13) Résumé

Le projet fournit une base distribuée propre et démontrable:
- communication gRPC structurée
- élection leader dynamique
- redirection transparente
- idempotence robuste
- failover et recovery basiques
- observabilité via dashboard et scripts de tests

# RDC
