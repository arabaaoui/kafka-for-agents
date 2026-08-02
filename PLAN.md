# Plan d'implémentation — Passe 2 et 3

## Résumé de l'existant

**Infrastructure (Passe 1) :**
- `docker-compose.yml` : Kafka 4.2.1 (KRaft), Kafka UI, kafka-init (5 topics), mcp-confluent, placeholder pour les 3 agents + simulateur
- `.env.example` : LLM, Kafka, MCP, Share Group, Simulation
- `mcp-confluent/` : Serveur MCP Node.js exposant 4 tools (consume-messages, list-topics, get-topic-config, get-consumer-group-lag) via HTTP sur :3000
- `agents/Dockerfile.agent` : Python 3.11-slim avec common/requirements.txt + copies de tous les modules
- `agents/common/config.py` : Charge .env, exporte toutes les constantes
- `agents/common/share_group_client.py` : Émulateur KIP-932 complet (ShareGroupClient avec poll, acknowledge, expiry loop, dead-letter, persistence)

## Ce qui manque

| Composant | Fichiers | Statut |
|---|---|---|
| Agent Détection | `agents/detection/agent.py`, `prompts.py` | ❌ |
| Agent Décision | `agents/decision/agent.py`, `prompts.py` | ❌ |
| Agent Exécution | `agents/execution/__init__.py`, `agent.py`, `prompts.py` | ❌ |
| Simulateur | `agents/simulator/__init__.py`, `app.py` | ❌ |
| SKILL.md | `skills/supply-chain-replenishment/SKILL.md` | ❌ |
| Scripts démo | `scripts/demo-1-*.sh`, `demo-2-*.sh`, `demo-3-*.sh` | ❌ |
| README | `README.md` | ❌ |
| `.gitignore` | `.gitignore` | ❌ |

## Architecture cible

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Simulator  │    │  Simulator  │    │  Detection  │
│  (orders)   │    │  (stocks)   │    │   Agent     │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       ▼                  ▼                  │ MCP HTTP
  [topic:orders]   [topic:stocks]            │ POST /mcp
       │                  │                  │
       │                  └──────┬───────────┘
       │                         │
       │                  ┌──────▼──────┐
       │                  │  Detection  │ lit stocks → détecte anomalies
       │                  │   Agent     │ → produit dans [topic:anomalies]
       │                  └─────────────┘
       │                         │
       │                  ┌──────▼──────┐
       │                  │  Decision   │ lit anomalies → applique SKILL.md
       │                  │   Agent     │ → produit dans [topic:tasks]
       │                  │             │ → log dans [topic:audit]
       │                  └──────┬──────┘
       │                         │
       │                  ┌──────▼──────┐
       │                  │  Execution  │ lit tasks (share group)
       │                  │   Agent ×N  │ → exécute, ACK/RENEW/RELEASE
       │                  └─────────────┘
```

Flux : `stocks → Detection → anomalies → Décision → tasks → Execution`

## Points d'attention / risques

1. **MCP Confluent via HTTP** : Le protocole MCP utilise JSON-RPC. L'agent Détection doit envoyer `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"consume-messages","arguments":{...}},"id":1}` à `POST /mcp`. Le serveur gère le transport StreamableHTTP.

2. **Share Group Client** : C'est un émulateur en couche applicative. Le Consumer sous-jacent est un consumer group standard, donc les partitions sont distribuées entre instances. La couche share group ajoute le lock/ack/release par-dessus. Pour le PoC, c'est acceptable — les 3 partitions de `tasks` seront réparties entre N execution-agents, et le share group gère le retry.

3. **google-adk** : Le package peut ne pas être stable. Alternative : appel direct à l'API LLM (OpenAI-compatible) avec un system prompt + function calling simulé. C'est l'approche retenue — les agents font des appels LLM structurés plutôt que d'utiliser ADK.

4. **Boucles infinies** : Chaque agent tourne en `while True` avec `sleep(POLL_INTERVAL)`. Gestion SIGTERM pour close propre des consumers/producers.

5. **Dockerfile.agent** : Copie tous les modules avant qu'ils n'existent. Le build va fonctionner une fois les fichiers créés.

## Ordre d'implémentation

1. SKILL.md — référencé par l'agent Décision
2. Agent Détection — agent.py + prompts.py
3. Agent Décision — agent.py + prompts.py
4. Agent Exécution — __init__.py + agent.py + prompts.py
5. Simulateur — __init__.py + app.py
6. Scripts de démo — 3 scripts bash
7. README.md + .gitignore
8. git add -A + git commit