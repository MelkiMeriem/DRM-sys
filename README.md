# ️ Distributed Resource Manager (DRM)
### Guide de présentation — Concepts, Démo & Explications des outputs

---

##  Table des matières
1. [Vue d'ensemble du projet](#1-vue-densemble-du-projet)
2. [Concepts fondamentaux](#2-concepts-fondamentaux)
    - [gRPC](#21-grpc)
    - [Raft Consensus](#22-raft-consensus)
    - [Mutex Distribué](#23-mutex-distribué)
3. [Architecture du projet](#3-architecture-du-projet)
4. [Démarrer le cluster](#4-démarrer-le-cluster)
5. [Guide de démo — Dashboard](#5-guide-de-démo--dashboard)
6. [Explication des outputs du dashboard](#6-explication-des-outputs-du-dashboard)
7. [Fichiers importants à montrer](#7-fichiers-importants-à-montrer)
8. [Scénarios de démonstration](#8-scénarios-de-démonstration)

---

## 1. Vue d'ensemble du projet

Le **DRM (Distributed Resource Manager)** est un système distribué de gestion de jobs qui implémente :

- **Consensus distribué** via l'algorithme Raft
- **Exclusion mutuelle** via un mutex distribué
- **Tolérance aux pannes** — le cluster survit à la perte d'un nœud
- **Idempotence** — un même job ne s'exécute jamais deux fois
- **Communication inter-nœuds** via gRPC

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   node1     │◄──►│   node2     │◄──►│   node3     │
│ HTTP :8081  │    │ HTTP :8082  │    │ HTTP :8083  │
│ gRPC :50051 │    │ gRPC :50052 │    │ gRPC :50053 │
└─────────────┘    └─────────────┘    └─────────────┘
       ▲
       │
   Client / Dashboard
```

---

## 2. Concepts fondamentaux

### 2.1 gRPC

**gRPC** (Google Remote Procedure Call) est un framework de communication entre services.

**Pourquoi gRPC et pas REST ?**

| Critère | REST (HTTP/JSON) | gRPC (HTTP/2 + Protobuf) |
|---|---|---|
| Format | JSON (texte) | Protobuf (binaire) |
| Performance | Moyen | Très rapide |
| Typage | Non strict | Strict (schéma .proto) |
| Streaming | Non | Oui |

**Dans le projet**, gRPC est utilisé pour :
- La réplication du log Raft entre nœuds (`AppendEntries`, `RequestVote`)
- La soumission de jobs (`SubmitJob`, `ForwardJob`)
- Le mutex distribué (`AcquireMutex`, `ReleaseMutex`)

**Le fichier de schéma** : `proto/src/main/proto/drm.proto` définit tous les messages et services.

```protobuf
// Exemple simplifié
service JobService {
  rpc SubmitJob(SubmitJobRequest) returns (SubmitJobResponse);
  rpc GetJob(GetJobRequest) returns (GetJobResponse);
}

service ClusterService {
  rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
  rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
  rpc AcquireMutex(MutexRequest) returns (MutexResponse);
  rpc ReleaseMutex(MutexRequest) returns (Ack);
}
```

---

### 2.2 Raft Consensus

**Raft** est un algorithme de consensus distribué. Son but : faire en sorte que tous les nœuds d'un cluster **s'accordent sur le même état**, même en cas de panne.

#### Les 3 rôles d'un nœud Raft

```
FOLLOWER  →  CANDIDATE  →  LEADER
    ▲              │
    └──────────────┘
    (élection échouée)
```

| Rôle | Comportement |
|---|---|
| **FOLLOWER** | Reçoit les ordres du leader, réplique le log |
| **CANDIDATE** | Demande des votes pour devenir leader |
| **LEADER** | Reçoit les requêtes clients, réplique sur les followers |

#### Le Term (mandat)
Chaque élection incrémente le **term**. C'est le "numéro de mandat" du leader. Si `term=4`, il y a eu 4 élections depuis le démarrage.

#### Le Log Raft — le cahier partagé

```
Index :  0      1          2          3
         ┌──────┬──────────┬──────────┬──────────┐
node1    │ init │  job_A   │  job_B   │  job_C   │  ← LEADER
         ├──────┼──────────┼──────────┼──────────┤
node2    │ init │  job_A   │  job_B   │  job_C   │  ← copie identique
         ├──────┼──────────┼──────────┼──────────┤
node3    │ init │  job_A   │  job_B   │  job_C   │  ← copie identique
         └──────┴──────────┴──────────┴──────────┘
                     ↑
              CommitIndex = 3 (confirmé par 2/3 nœuds)
```

#### Le cycle de vie d'un job dans Raft

```
1. Client soumet un job au leader
         ↓
2. Leader écrit dans son log  →  logLastIndex++
         ↓
3. Leader envoie AppendEntries aux followers
         ↓
4. Les followers répondent "OK"
         ↓
5. Majorité (2/3) confirmée  →  commitIndex++
         ↓
6. Job exécuté
```

#### Failover automatique

Si le leader tombe, les followers déclenchent une **élection** après un timeout aléatoire :
```
node1 (LEADER) tombe
       ↓
node2 et node3 détectent l'absence de heartbeat
       ↓
node2 (timeout le plus court) se déclare CANDIDATE
       ↓
node2 demande les votes → node3 vote OUI
       ↓
node2 devient LEADER (term++)
       ↓
Cluster opérationnel en ~2 secondes 

```

---

### 2.3 Mutex Distribué

**Problème** : Si 5 clients envoient la même requête simultanément, sans protection, 5 jobs identiques seraient créés.

**Solution** : Le mutex distribué garantit qu'**un seul job est créé**, même sous forte concurrence.

#### Comment ça marche

```
5 requêtes arrivent avec le même requestId
         ↓
Toutes forwardées au LEADER
         ↓
Le leader gère un verrou en mémoire : Map<requestId, lock>
         ↓
Requête 1 : acquiert le mutex → crée job A → libère
Requêtes 2,3,4,5 : mutex occupé → attendent → reçoivent job A
         ↓
Résultat : 1 seul job créé, 5 clients reçoivent la même réponse 
```

#### Idempotence
Grâce à la `Map<clientRequestId, jobId>`, même si une requête est rejouée (retry réseau), le même jobId est retourné — **le job ne s'exécute jamais deux fois**.

---

## 3. Architecture du projet

```
distributed-resource-manager/
├── proto/          ← Schémas gRPC (.proto) + classes générées
├── node/           ← Le nœud principal (Spring Boot)
│   └── src/main/java/.../node/
│       ├── raft/
│       │   ├── RaftNode.java         ← Algorithme Raft complet
│       │   ├── RaftLog.java          ← Log répliqué
│       │   ├── DistributedMutex.java ← Mutex (verrous en mémoire)
│       │   └── PeerClients.java      ← Clients gRPC vers les pairs
│       ├── grpc/
│       │   ├── JobServiceImpl.java   ← SubmitJob, GetJob
│       │   └── ClusterServiceImpl.java ← AppendEntries, Mutex RPC
│       ├── jobs/
│       │   └── JobManager.java       ← Exécution et stockage des jobs
│       ├── web/
│       │   └── DashboardController.java ← API REST du dashboard
│       └── config/
│           └── NodeConfig.java       ← Configuration (nodeId, ports, peers)
├── client/         ← Client CLI pour tester
└── scripts/
    ├── start-cluster.sh   ← Démarre les 3 nœuds
    ├── stop-cluster.sh    ← Arrête le cluster
    └── run_all_tests.sh   ← Lance tous les tests automatisés
```

---

## 4. Démarrer le cluster

```bash
# 1. Démarrer les 3 nœuds
cd ~/distributed-resource-manager
bash scripts/start-cluster.sh

# 2. Attendre 5 secondes puis vérifier
sleep 5
curl -s http://localhost:8081/api/dashboard/status | python3 -m json.tool | grep -E "nodeId|role|term"

# 3. Ouvrir le dashboard
# → http://localhost:8081

# 4. Arrêter le cluster
bash scripts/stop-cluster.sh
```

**Ce qui se passe au démarrage :**
- 3 instances Java démarrent sur les ports 8081, 8082, 8083 (HTTP) et 50051, 50052, 50053 (gRPC)
- Raft démarre une élection automatique → un leader est élu en ~1-2 secondes
- Le leader commence à envoyer des heartbeats toutes les 500ms

---

## 5. Guide de démo — Dashboard

Ouvre **http://localhost:8081** dans ton navigateur.

### Zone 1 — En-tête (métriques du nœud)
```
Node: node1  |  Role: LEADER  |  Leader: node1
Jobs: 5  |  Log Index: 12  |  Commit Index: 12
```

### Zone 2 — Tableau Cluster
Affiche l'état de chaque nœud en temps réel (refresh automatique).

### Zone 3 — Test manuel
Permet de soumettre un job manuellement avec différents presets.

### Zone 4 — Jobs récents
Liste des jobs exécutés sur le cluster.

### Zone 5 — Test Mutex
Lance N requêtes simultanées avec le même requestId.

### Zone 6 — Log Raft
Métriques de réplication du nœud courant.

### Zone 7 — Multi-client
Simule plusieurs clients en parallèle.

---

## 6. Explication des outputs du dashboard

### 6.1 Tableau Cluster

```
Node   | Target          | Role     | Term | Leader | Log Index | Commit Index | Jobs | Running
node1  | localhost:50051 | LEADER   |  1   | node1  |    12     |     12       |  5   |   0
node2  | localhost:50052 | FOLLOWER |  1   | node1  |    12     |     12       |  2   |   0
node3  | localhost:50053 | FOLLOWER |  1   | node1  |    12     |     12       |  0   |   0
```

| Colonne | Explication |
|---|---|
| **Role** | LEADER = chef actuel, FOLLOWER = réplique, CANDIDATE = en élection |
| **Term** | Numéro d'élection — tous les nœuds doivent avoir le même term |
| **Leader** | L'ID du leader vu par ce nœud — doit être identique partout |
| **Log Index** | Dernière entrée écrite dans le log Raft |
| **Commit Index** | Dernière entrée confirmée par la majorité et exécutable |
| **Log = Commit** |  Tout est synchronisé — si différent, des entrées attendent confirmation |
| **Jobs** | Nombre de jobs stockés localement sur ce nœud |
| **Running** | Jobs en cours d'exécution (0 = idle) |

**Ce qu'il faut surveiller :**
- `Log Index = Commit Index` sur tous les nœuds → cluster sain 
- Un seul nœud en `ROLE = LEADER` → Raft respecté 
- Tous les nœuds avec le même `Term` → pas de split-brain 

---

### 6.2 Output du Test Mutex

```json
{
  "distinctJobIds": 1,          ← UN seul job créé malgré 5 requêtes
  "concurrency": 5,             ← 5 requêtes simultanées
  "mutexOk": true,              ← Mutex a fonctionné
  "verdict": " MUTEX OK",
  "results": [
    { "sender": 0, "jobId": "abc-123", "executedBy": "node1", "ok": true },
    { "sender": 1, "jobId": "abc-123", "executedBy": "node1", "ok": true },
    { "sender": 2, "jobId": "abc-123", "executedBy": "node1", "ok": true },
    { "sender": 3, "jobId": "abc-123", "executedBy": "node1", "ok": true },
    { "sender": 4, "jobId": "abc-123", "executedBy": "node1", "ok": true }
  ]
}
```

**Comment expliquer à ton prof :**
> "Les 5 senders ont le même `jobId` → le mutex a garanti qu'un seul job a été créé. Sans mutex, on aurait 5 jobIds différents."

---

### 6.3 Output du Failover Leader

```json
{
  "oldLeaderId": "node1",       ← Ancien leader (mis en mode failure)
  "newLeaderId": "node2",       ← Nouveau leader élu par Raft
  "probe": {
    "executedByNodeId": "node2",
    "status": "JOB_STATUS_SUCCEEDED",
    "leaderNodeId": "node2"     ← Le cluster fonctionne avec le nouveau leader
  }
}
```

**Comment expliquer :**
> "On a simulé la panne de node1. En moins de 2 secondes, node2 a été élu nouveau leader via l'algorithme Raft. Le cluster a continué à accepter des jobs sans interruption — c'est la tolérance aux pannes."

---

### 6.4 Output du Multi-client

```json
{
  "totalSubmitted": 6,
  "ok": 6,
  "failed": 0,
  "results": [
    { "client": 1, "task": 1, "acceptedBy": "node2", "executedByNodeId": "node1", "ok": true },
    { "client": 1, "task": 2, "acceptedBy": "node3", "executedByNodeId": "node1", "ok": true },
    ...
  ]
}
```

**Points importants à montrer :**
- `acceptedBy` ≠ `executedByNodeId` → un follower reçoit et redirige au leader 
- `ok: true` pour tous → pas de perte de requête sous charge 
- Jobs distribués sur plusieurs nœuds → load balancing 

---

### 6.5 Table Jobs récents

```
Job ID    | Status              | Type             | Exécuté par | Stocké sur | Résultat
abc-123   | JOB_STATUS_SUCCEEDED| JOB_TYPE_MESSAGE | node1       | node1      | {"accepted":true,"echo":"bonjour"}
def-456   | JOB_STATUS_SUCCEEDED| JOB_TYPE_SUM     | node2       | node2      | {"sum":42}
```

| Colonne | Explication |
|---|---|
| **Status** | PENDING → RUNNING → SUCCEEDED / FAILED |
| **Type** | MESSAGE (echo), SUM (addition), SHA256 (hash) |
| **Exécuté par** | Nœud qui a exécuté le job (choisi par round-robin) |
| **Stocké sur** | Nœud qui garde le job en mémoire locale |
| **Résultat** | Output JSON du job |

---

### 6.6 Log Raft (métriques)

```
Log Last Index : 12   ← Dernière entrée écrite
Commit Index   : 12   ← Dernière entrée confirmée par majorité
Rôle actuel    : LEADER
Term actuel    : 1
```

**Situation saine** : `Log Last Index = Commit Index` → tout committé, rien en attente.

**Situation dégradée** : `Log Last Index > Commit Index` → des entrées attendent la confirmation de la majorité (transitoire, se résout en ~500ms via heartbeat).

---

## 7. Fichiers importants à montrer

### Fichiers clés à ouvrir pendant la présentation

#### 1. `proto/src/main/proto/drm.proto`
> **"Voici le contrat de communication entre les nœuds — tout ce qui passe par gRPC est défini ici."**
- Montre `service JobService`, `service ClusterService`
- Montre `message AppendEntriesRequest` (Raft), `message MutexRequest`

#### 2. `node/src/main/java/.../raft/RaftNode.java`
> **"Voici l'implémentation de l'algorithme Raft."**
- Montre `startElection()` → comment un nœud devient candidat
- Montre `appendCommand()` → comment un job est répliqué avant exécution
- Montre `sendHeartbeats()` → comment le leader maintient son autorité

#### 3. `node/src/main/java/.../raft/DistributedMutex.java`
> **"Le mutex — une ConcurrentHashMap protégée par synchronized. Simple mais efficace."**
- Montre `acquire()` et `release()`

#### 4. `node/src/main/java/.../grpc/JobServiceImpl.java`
> **"Le pipeline complet : réplication Raft → mutex → dispatch du job."**
- Montre les 5 étapes commentées dans `submitJob()`

#### 5. `node/src/main/java/.../jobs/JobManager.java`
> **"L'idempotence — si un requestId existe déjà, on retourne le même job."**
- Montre `requestToJobId` map et le check au début de `submit()`

#### 6. `scripts/start-cluster.sh`
> **"On démarre 3 instances du même JAR avec des variables d'environnement différentes."**

---

## 8. Scénarios de démonstration

### Scénario 1 — Soumission simple (2 min)
1. Ouvre le dashboard sur http://localhost:8081
2. Montre le tableau Cluster → 3 nœuds, 1 LEADER
3. Dans "Test manuel", envoie un message `{"message":"bonjour prof"}`
4. Clique "Suivre" → montre le job SUCCEEDED
5. **Explique** : "Le job a été répliqué sur les 3 nœuds via Raft avant exécution"

### Scénario 2 — Redirection follower (2 min)
1. Sélectionne `Target: node2` (un follower)
2. Envoie un message
3. Montre dans le résultat : `acceptedBy: node2, executedByNodeId: node1` (le leader)
4. **Explique** : "node2 a reçu la requête mais l'a forwardée au leader node1 — seul le leader peut écrire dans le log Raft"

### Scénario 3 — Test Mutex (2 min)
1. Lance le test mutex avec concurrence 5
2. Montre `distinctJobIds: 1`
3. **Explique** : "5 requêtes simultanées, 1 seul job créé — le mutex distribué protège contre les doublons"

### Scénario 4 — Failover leader (3 min)
1. Note le leader actuel dans le tableau
2. Clique "Test3 - failover leader"
3. Montre en temps réel le tableau se mettre à jour — nouveau leader
4. **Explique** : "On a tué le leader. En 2 secondes, Raft a déclenché une élection. Le Term a augmenté. Le cluster est tolérant aux pannes."
5. Envoie un nouveau job → montre qu'il fonctionne avec le nouveau leader

### Scénario 5 — Multi-client (2 min)
1. Lance 3 clients × 2 tâches
2. Montre `failed: 0`, `ok: 6`
3. Montre les différents `acceptedBy` → load balancing
4. **Explique** : "6 requêtes en parallèle, 0 perdu, distribué sur les 3 nœuds"

---

##  Checklist de présentation

- [ ] Cluster démarré (`bash scripts/start-cluster.sh`)
- [ ] Dashboard ouvert sur http://localhost:8081
- [ ] 3 nœuds visibles avec 1 LEADER
- [ ] Log Index = Commit Index sur les 3 nœuds
- [ ] Fichiers clés ouverts dans l'éditeur
- [ ] Tester un submit simple avant la présentation

---

##  Phrases clés pour la présentation

> **Sur Raft :** "Raft garantit que tous les nœuds s'accordent sur le même historique de commandes, même si un nœud tombe. Pas de log différent entre nœuds — c'est le consensus."

> **Sur le Log :** "Avant d'exécuter un job, on l'écrit dans le log et on attend que la majorité (2 nœuds sur 3) confirme. C'est le commit Raft."

> **Sur le Mutex :** "Le mutex est centralisé sur le leader — seul arbitre de vérité du cluster. Personne ne peut prendre le lock deux fois en même temps."

> **Sur gRPC :** "On utilise gRPC plutôt que REST pour la communication inter-nœuds — plus rapide, typé, et bidirectionnel. Les messages sont définis en Protobuf."

> **Sur la tolérance aux pannes :** "Le cluster supporte la perte d'un nœud sur trois. Avec 2 nœuds restants, la majorité (2/3) est toujours atteinte — le système continue."

---

# DRM
