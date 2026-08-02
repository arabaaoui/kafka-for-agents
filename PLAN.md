# Plan d'implémentation — Passe 3 : Vrais agents ADK

## 1. Résumé — ce qui existe vs ce qui manque

### Ce qui existe et fonctionne (infra, PAS touché)

| Composant | État |
|---|---|
| `docker-compose.yml` | Kafka 4.2.1 KRaft + Kafka UI + kafka-init (5 topics) + mcp-confluent + simulator + 3 agents (placeholders `LLM_PROVIDER/LLM_MODEL/LLM_API_KEY` globaux) |
| `mcp-confluent/server.js` | Serveur MCP Node.js réel (StreamableHTTP), 4 tools : `consume-messages`, `list-topics`, `get-topic-config`, `get-consumer-group-lag` |
| `agents/Dockerfile.agent` | Python 3.11-slim, copie `common/`, `detection/`, `decision/`, `execution/`, `simulator/` |
| `agents/common/share_group_client.py` | Émulateur KIP-932 complet et correct (ACK/RENEW/RELEASE, expiry loop, dead-letter, persistence JSON) — **rien à changer** |
| `agents/common/config.py` | Charge `.env`, config globale LLM unique |
| `skills/supply-chain-replenishment/SKILL.md` | Procédure métier complète en 6 étapes, bien écrite — **rien à changer** |
| `agents/simulator/app.py` | Génère stocks/orders réalistes, injecte des anomalies périodiquement — **rien à changer côté logique métier**, mais utilise `httpx`/`confluent_kafka` directement (correct, pas de LLM ici donc pas concerné par ADK) |
| `README.md`, scripts démo | Cohérents avec l'archi actuelle, à mettre à jour pour refléter ADK |

### Ce qui est codé mais NE respecte PAS la contrainte "vrais agents ADK"

Les 3 agents (`detection`, `decision`, `execution`) sont fonctionnellement complets (boucle Kafka, parsing, logs `[AGENT]`, graceful shutdown SIGTERM) mais **n'utilisent pas `google-adk`** :

- `detection/agent.py` : pas de LLM du tout — anomalie détectée par une règle Python pure (`quantity < seuil_min`), pas de décision de modèle.
- `decision/agent.py` : appelle l'API en dur via `httpx.post()` sur des endpoints codés en dur (`api.anthropic.com`, `api.openai.com`) avec un format de payload uniquement OpenAI-compatible (cassé pour Anthropic — le payload `messages`/`system` séparé d'Anthropic n'est pas celui envoyé). **C'est exactement l'anti-pattern interdit par la consigne.**
- `execution/agent.py` : n'appelle aucun LLM, exécution simulée par `random.uniform(1,10)` + tirage de succès à 90 %.
- `requirements.txt` : déclare `google-adk>=0.1.0` mais le code ne l'importe jamais.
- Pas de `agents/common/adk_factory.py`.
- `.env.example` / `config.py` : une seule config LLM globale, pas de config par agent.

**Conclusion : les 3 `agent.py` doivent être réécrits pour passer par de vrais objets `google.adk.Agent` + `Runner`, et `config.py`/`.env.example`/`requirements.txt` doivent être enrichis pour le multi-modèle.**

---

## 2. Vérification technique du package `google-adk` réel

Avant d'écrire le code, j'ai téléchargé et inspecté le wheel PyPI `google-adk` (dernière version publiée : **2.6.1**, `requires_python >= 3.10` — compatible avec l'image `python:3.11-slim` du Dockerfile). Deux écarts importants avec le brief à noter :

### ⚠️ Écart 1 — `Runner` est obligatoire, pas d'appel direct `agent.run_async()`

Contrairement à l'exemple simplifié du brief, **`Agent` (alias de `LlmAgent`) n'a pas de méthode `run_async()`**. L'exécution passe obligatoire par un `google.adk.Runner`, couplé à un `SessionService` :

```python
from google.adk import Agent, Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

runner = Runner(agent=agent, app_name="detection-agent", session_service=InMemorySessionService())

async def run_once(user_prompt: str) -> str:
    session = await runner.session_service.create_session(
        app_name="detection-agent", user_id="agent", session_id=str(uuid.uuid4())
    )
    final_text = ""
    async for event in runner.run_async(
        user_id="agent",
        session_id=session.id,
        new_message=types.Content(role="user", parts=[types.Part(text=user_prompt)]),
    ):
        if event.is_final_response() and event.content and event.content.parts:
            final_text = event.content.parts[0].text or final_text
    return final_text
```

Chaque agent créera **une nouvelle session par message traité** (pas de session persistante) : les anomalies/tâches sont indépendantes les unes des autres, et une session partagée ferait grossir le contexte indéfiniment sans bénéfice (pas de besoin de mémoire conversationnelle inter-messages ici).

### ⚠️ Écart 2 — `Claude` (dans `anthropic_llm.py`) est réservé à Vertex AI, pas à l'API Anthropic directe

Le brief propose `from google.adk.models.anthropic_llm import Claude`. En inspectant le code source :

```python
class AnthropicLlm(BaseLlm):
    # _anthropic_client -> AsyncAnthropic()  (lit ANTHROPIC_API_KEY depuis l'env)

class Claude(AnthropicLlm):
    # _anthropic_client -> AsyncAnthropicVertex(project_id=..., region=...)
    # lève une ValueError si GOOGLE_CLOUD_PROJECT / GOOGLE_CLOUD_LOCATION absents
```

`Claude` est donc la variante **Vertex AI** (nécessite un projet GCP) — inutilisable avec une simple clé API Anthropic (`sk-ant-...`) comme le prévoit `DECISION_LLM_API_KEY`. La classe correcte pour l'API Anthropic directe est **`AnthropicLlm`** (classe de base), qui instancie `AsyncAnthropic()` — celle-ci lit la clé depuis la variable d'environnement `ANTHROPIC_API_KEY` du **process courant**.

**Décision retenue** : `adk_factory.py` mappera `provider == "anthropic"` vers `AnthropicLlm(model=model)`, et positionnera `os.environ["ANTHROPIC_API_KEY"] = api_key` juste avant l'instanciation. C'est sans risque de fuite/collision car **chaque agent tourne dans son propre process/container** (un seul provider Anthropic actif par process, jamais plusieurs clés à jongler dans le même interpréteur).

### Confirmations utiles

- `LiteLlm(model: str, **kwargs)` accepte `api_key` comme kwarg direct, transmis tel quel à `litellm.completion(...)` — pas besoin de variable d'env pour `openai`/`gemini`. Formats de modèle : `openai/gpt-4o`, `gemini/gemini-2.5-pro`.
- `tools=[une_fonction_python]` fonctionne tel quel — ADK détecte automatiquement les tools Python (wrap silencieux en `FunctionTool`, schema extrait des type hints + docstring). Pas besoin de `@tool` decorator.
- `litellm` et `anthropic` (SDK) sont des **extras** de `google-adk` (`extra == "extensions"`), **pas des dépendances de base**. Il faut les lister explicitement dans `requirements.txt`.

---

## 3. Architecture cible

```
                 ┌────────────┐
                 │ Simulator  │  (inchangé — pas de LLM)
                 └─────┬──────┘
                        │ stocks, orders
                        ▼
   ┌────────────────────────────────────────────┐
   │           DETECTION AGENT (poll 5s)         │
   │  consumer(stocks) → agent.run via Runner    │
   │  tools: consume_stocks, call_mcp,           │
   │         produce_anomaly                     │
   │  LLM: DETECTION_LLM_*                       │
   └─────────────────────┬────────────────────────┘
                          │ anomalies
                          ▼
   ┌────────────────────────────────────────────┐
   │           DECISION AGENT (poll 3s)          │
   │  consumer(anomalies) → agent.run via Runner │
   │  instruction = SYSTEM_PROMPT + SKILL.md     │
   │  tools: read_skill, get_neighbor_stocks,    │
   │         produce_task, log_audit             │
   │  LLM: DECISION_LLM_*                        │
   └─────────────────────┬────────────────────────┘
                          │ tasks (+ audit)
                          ▼
   ┌────────────────────────────────────────────┐
   │       EXECUTION AGENT ×N (KIP-932)          │
   │  ShareGroupClient.poll() → agent.run        │
   │  tools: execute_transfer, execute_order,    │
   │         ack_task/renew_lock/release_task    │
   │  LLM: EXECUTION_LLM_* (optionnel)           │
   └────────────────────────────────────────────┘
```

**Principe clé : le LLM ne fait plus la boucle Kafka lui-même — c'est le code Python qui pilote la boucle `while True: poll → agent.run() → sleep`, et c'est l'agent ADK (via ses tools) qui décide/agit à l'intérieur d'un cycle.** Les tools Python sont ce qui donne à l'agent la capacité d'écrire dans Kafka — l'agent ne "sait" produire un message que parce qu'on lui expose `produce_anomaly()`/`produce_task()` comme tool, jamais en écrivant du JSON que le code parserait après coup à l'aveugle (même si en pratique, pour un agent mono-tour comme ici, l'agent choisira presque toujours d'appeler le tool terminal puis répondra un court résumé texte).

---

## 4. `agents/common/adk_factory.py` (nouveau fichier)

```python
def create_llm(provider: str, model: str, api_key: str):
    """Retourne l'instance de modèle ADK correspondant au provider."""
    if provider == "openai":
        return LiteLlm(model=f"openai/{model}", api_key=api_key)
    if provider == "gemini":
        return LiteLlm(model=f"gemini/{model}", api_key=api_key)
    if provider == "anthropic":
        os.environ["ANTHROPIC_API_KEY"] = api_key  # lu par AsyncAnthropic()
        return AnthropicLlm(model=model)
    raise ValueError(f"Unknown LLM provider: {provider}")


def build_agent(*, name, description, instruction, tools, provider, model, api_key) -> Agent:
    return Agent(
        model=create_llm(provider, model, api_key),
        name=name,
        description=description,
        instruction=instruction,
        tools=tools,
    )


async def run_agent_once(agent: Agent, app_name: str, user_prompt: str) -> str:
    """Crée une session jetable, envoie user_prompt, retourne le texte de la réponse finale."""
    # Runner + InMemorySessionService + boucle sur run_async (voir §2)
```

`run_agent_once` est **synchrone à appeler** (wrap `asyncio.run(...)`) car le reste du code des agents (confluent_kafka, ShareGroupClient) est synchrone — pas de refonte async de toute la boucle Kafka, juste le point d'appel LLM qui bascule en async ponctuellement via `asyncio.run()`.

---

## 5. Structure de chaque agent

### 5.1 Detection Agent (`agents/detection/agent.py`)

- Garde la boucle `Consumer(stocks)` + `Producer` existante (poll 5s, SIGTERM).
- Tools exposés à l'agent ADK :
  - `consume_stocks(limit: int) -> list[dict]` — relit les derniers messages `stocks` (remplace l'appel MCP direct actuel par un vrai tool ADK).
  - `call_mcp(tool_name: str, arguments: dict) -> dict` — appelle `mcp-confluent` via HTTP JSON-RPC (`POST /mcp`), généralisation du `call_mcp_consume` existant.
  - `produce_anomaly(anomalie_json: str) -> str` — parse le JSON produit par l'agent et publie sur `anomalies`. **C'est ce tool, pas du code Python, qui décide qu'il y a une anomalie** (le code ne fait plus le test `quantity < seuil_min` lui-même : ça devient une instruction dans le prompt).
- Boucle : à chaque message `stocks` reçu, construire un prompt utilisateur avec les données du message, appeler `run_agent_once(agent, ..., user_prompt)`. L'agent décide d'appeler `produce_anomaly` ou de ne rien faire.
- Modèle : `DETECTION_LLM_PROVIDER/MODEL/API_KEY`.

### 5.2 Decision Agent (`agents/decision/agent.py`)

- Charge `SKILL.md` **une fois au démarrage** et l'injecte dans `instruction` (concaténé au `SYSTEM_PROMPT`) — comme demandé, "lu au démarrage et injecté dans l'instruction". Pas de rechargement à chaud par cycle (le hot-reload mentionné dans le README concerne un redémarrage de conteneur, pas un polling de fichier — cohérent avec le existant).
- Tools :
  - `read_skill() -> str` — expose aussi le contenu en tool au cas où l'agent veut le relire explicitement (redondant avec l'instruction mais demandé explicitement au §Tâches).
  - `get_neighbor_stocks(region, product_id, exclude_store) -> list[dict]` — reprend `get_neighbor_stocks` existant, exposé en tool plutôt que pré-calculé côté Python (l'agent décide s'il en a besoin).
  - `produce_task(task_json: str) -> str`
  - `log_audit(decision_json: str) -> str`
- Boucle (poll 3s) : reçoit une anomalie → prompt utilisateur avec les champs de l'anomalie → `run_agent_once`. L'agent doit appeler `get_neighbor_stocks`, décider, puis appeler `produce_task` **et** `log_audit`.
- Modèle : `DECISION_LLM_PROVIDER/MODEL/API_KEY`.
- Fallback conservé : si `run_agent_once` lève une exception (LLM down), on garde la décision de secours actuelle (commande fournisseur par défaut) pour ne pas bloquer le pipeline de démo.

### 5.3 Execution Agent (`agents/execution/agent.py`)

- Garde `ShareGroupClient` intégralement (aucune simulation, comme l'exige la consigne).
- Tools :
  - `execute_transfer(task_json: str) -> dict`
  - `execute_order(task_json: str) -> dict`
  - `ack_task()`, `renew_lock()`, `release_task()` — **mais** : `ack_task`/`renew_lock`/`release_task` ont besoin de la référence exacte au `ShareMessage` en cours (objet Python avec offset/partition), qui n'est pas sérialisable proprement en argument de tool LLM. Je les garde comme **fonctions Python appelées par le code après la réponse de l'agent**, pas comme tools LLM — l'agent ADK décide uniquement du **résultat métier** (succès/échec, éventuellement type d'exécution), et c'est le code Python qui traduit ce résultat en ACK/RENEW/RELEASE sur le bon message. C'est le tool `execute_transfer`/`execute_order` qui fait le travail réel ; le LLM sert à choisir/valider la stratégie d'exécution (utile si on veut un jour un agent qui refuse une tâche incohérente).
  - Le LLM est **optionnel** ici (cf. consigne docker-compose : execution-agent peut tourner sans LLM). Si `EXECUTION_LLM_API_KEY` est vide, l'agent bascule sur l'exécution déterministe actuelle (délai aléatoire + 90% succès) sans jamais appeler ADK — évite de bloquer le PoC si on ne veut pas payer un 3ème provider LLM pour la démo.
- Boucle : `client.poll()` → si LLM configuré, `run_agent_once` avec le JSON de la tâche → sinon exécution déterministe → `client.acknowledge(...)` selon le résultat, `RENEW` si délai > 5s comme avant.

---

## 6. Configuration multi-modèle

### `agents/common/config.py` — ajouts

```python
DETECTION_LLM_PROVIDER = os.getenv("DETECTION_LLM_PROVIDER", "openai")
DETECTION_LLM_MODEL = os.getenv("DETECTION_LLM_MODEL", "gpt-4o")
DETECTION_LLM_API_KEY = os.getenv("DETECTION_LLM_API_KEY", "")

DECISION_LLM_PROVIDER = os.getenv("DECISION_LLM_PROVIDER", "anthropic")
DECISION_LLM_MODEL = os.getenv("DECISION_LLM_MODEL", "claude-sonnet-4-20250514")
DECISION_LLM_API_KEY = os.getenv("DECISION_LLM_API_KEY", "")

EXECUTION_LLM_PROVIDER = os.getenv("EXECUTION_LLM_PROVIDER", "")   # vide = pas de LLM
EXECUTION_LLM_MODEL = os.getenv("EXECUTION_LLM_MODEL", "")
EXECUTION_LLM_API_KEY = os.getenv("EXECUTION_LLM_API_KEY", "")
```

Les anciennes `LLM_PROVIDER/LLM_MODEL/LLM_API_KEY` globales sont **supprimées** (remplacées entièrement par les 3 jeux par agent) — pas de double config à maintenir. `validate()` est mis à jour pour vérifier `DETECTION_LLM_API_KEY` et `DECISION_LLM_API_KEY` (obligatoires), `EXECUTION_LLM_API_KEY` (optionnelle).

### `.env.example` — réécriture de la section LLM

Documente les 3 blocs `DETECTION_LLM_*` / `DECISION_LLM_*` / `EXECUTION_LLM_*` tels que donnés dans le brief, avec commentaires sur les providers supportés et le caractère optionnel du LLM d'exécution.

### `docker-compose.yml` — ajouts

Chaque service agent reçoit ses propres variables (au lieu de `LLM_PROVIDER/LLM_MODEL/LLM_API_KEY` génériques) :

```yaml
detection-agent:
  environment:
    DETECTION_LLM_PROVIDER: ${DETECTION_LLM_PROVIDER:-openai}
    DETECTION_LLM_MODEL: ${DETECTION_LLM_MODEL:-gpt-4o}
    DETECTION_LLM_API_KEY: ${DETECTION_LLM_API_KEY}

decision-agent:
  environment:
    DECISION_LLM_PROVIDER: ${DECISION_LLM_PROVIDER:-anthropic}
    DECISION_LLM_MODEL: ${DECISION_LLM_MODEL:-claude-sonnet-4-20250514}
    DECISION_LLM_API_KEY: ${DECISION_LLM_API_KEY}

execution-agent:
  environment:
    EXECUTION_LLM_PROVIDER: ${EXECUTION_LLM_PROVIDER:-}
    EXECUTION_LLM_MODEL: ${EXECUTION_LLM_MODEL:-}
    EXECUTION_LLM_API_KEY: ${EXECUTION_LLM_API_KEY:-}
```

### `agents/common/requirements.txt`

```
google-adk>=2.0
litellm>=1.84
anthropic>=0.78
confluent-kafka>=2.6.0
python-dotenv>=1.0.0
httpx>=0.27.0
pydantic>=2.0.0
```

(`litellm` et `anthropic` sont ajoutés explicitement car ce sont des extras de `google-adk`, pas des dépendances de base — vérifié sur le manifeste PyPI réel.)

---

## 7. Ordre d'implémentation

1. `agents/common/requirements.txt` — ajouter les deps ADK.
2. `agents/common/config.py` — config multi-modèle par agent.
3. `.env.example` — documenter la config multi-modèle.
4. `agents/common/adk_factory.py` — factory + helper `run_agent_once`.
5. `agents/detection/prompts.py` + `agent.py` — réécriture ADK.
6. `agents/decision/prompts.py` + `agent.py` — réécriture ADK (dépend de SKILL.md, inchangé).
7. `agents/execution/prompts.py` + `agent.py` — réécriture ADK (dépend de `share_group_client.py`, inchangé).
8. `docker-compose.yml` — variables d'env par agent.
9. `README.md` — architecture ADK, config multi-modèle, quickstart.
10. `scripts/demo-1/2/3.sh` — mentions ADK/multi-modèle dans les messages affichés (pas de changement fonctionnel, les 3 démos restent valides telles quelles).
11. `git add -A && git commit`.

---

## 8. Risques et pièges identifiés

1. **`Claude` vs `AnthropicLlm`** (détaillé §2) — utiliser `AnthropicLlm`, pas `Claude`, sous peine de `ValueError` (GCP project manquant) au premier appel Decision Agent.
2. **`Runner` + `SessionService` obligatoires** — pas de `agent.run_async()` direct ; oublier ce détail casse tout au runtime, pas à l'import (les erreurs `AttributeError` n'apparaîtraient qu'au premier message Kafka reçu, donc invisible tant qu'aucune donnée ne transite).
3. **Coût/latence réels** — les 3 agents feront maintenant de VRAIS appels LLM facturés à chaque message (contrairement à l'ancien code qui ne faisait un appel que pour le Decision Agent). Avec le simulateur qui produit ~10 000 messages `stocks` par cycle (200 stores × 50 produits), appeler le LLM sur **chaque** message stock serait ruineux. → Le code Python garde un **pré-filtre déterministe rapide** (`quantity < seuil_min`) *avant* d'invoquer l'agent ADK, pour ne solliciter le LLM que sur les cas déjà suspects (l'agent affine/confirme/qualifie la sévérité plutôt que de scanner 10 000 messages). Ce point sera documenté explicitement dans `detection/agent.py` pour éviter toute ambiguïté sur "où" la règle métier vit.
4. **`execution-agent` sans LLM par défaut** — si `EXECUTION_LLM_PROVIDER` est vide, `adk_factory.create_llm("")` doit être évité : le code de `execution/agent.py` doit tester la config *avant* d'appeler la factory, pas laisser `create_llm` lever une erreur non gérée.
5. **`google-adk>=2.0` résout en réalité vers 2.6.1** (dernière version publiée) — le pin du brief est respecté (`>=2.0`) mais je documenterai la version réellement testée dans le PLAN pour traçabilité.
6. **Sessions ADK et fuite mémoire** — `InMemorySessionService` accumule les sessions en RAM tant que le process tourne ; comme on crée une session par message (jamais réutilisée), il faut soit ignorer ce point pour un PoC de démo courte durée, soit ajouter un nettoyage (`delete_session`) après chaque `run_agent_once`. Je choisis d'ajouter la suppression explicite pour éviter une fuite visible en cas de démo longue.
7. **`ANTHROPIC_API_KEY` en variable de process** — sans risque en conteneur mono-agent, mais si jamais deux agents Anthropic tournaient dans le même process (pas notre cas), il y aurait collision. Non applicable ici mais documenté pour éviter une régression future.
8. **`asyncio.run()` dans une boucle sync répétée** — créer/détruire un event loop à chaque message a un coût mais reste acceptable aux fréquences de polling du PoC (3–5s) ; pas besoin de passer toute la boucle Kafka en async pour ce PoC.

## 9. Ce qui NE change PAS

`docker-compose.yml` (structure des services, uniquement les env vars), `.env.example` (structure, uniquement la section LLM), `mcp-confluent/`, `agents/Dockerfile.agent`, `agents/common/share_group_client.py`, `skills/supply-chain-replenishment/SKILL.md`, `agents/simulator/app.py` (logique métier), infra Kafka.

---

## 10. Ajustements validés en PHASE 2 (supersèdent les points ci-dessus)

Le plan ci-dessus a été validé avec 3 ajustements avant implémentation. Ils **remplacent** les décisions correspondantes prises aux §2/§4/§5.3 :

1. **LiteLLM unifié pour tous les providers** — `adk_factory.create_llm()` ne fait **aucune** branche `AnthropicLlm`/`Claude` (§2 Écart 2 et §4 sont donc obsolètes sur ce point). `anthropic` passe aussi par `LiteLlm(model=f"anthropic/{model}", api_key=api_key)`, exactement comme `openai` et `gemini`. Conséquence : pas de `os.environ["ANTHROPIC_API_KEY"]`, pas de dépendance au SDK `anthropic`, un seul code path pour les 3 providers.
2. **Execution Agent sans LLM = déterministe sans aléatoire** — si `EXECUTION_LLM_API_KEY` est vide, l'exécution n'est **plus** "délai aléatoire 1-10s + 90% succès" (contrairement à §5.3) mais **délai fixe 2s, 100% succès, aucun `random`**. Le chemin aléatoire (1-10s, 90%) est conservé uniquement pour les tools `execute_transfer`/`execute_order` appelés **par l'agent ADK** quand un `EXECUTION_LLM_API_KEY` est configuré.
3. **README.md pédagogique avec diagrammes Mermaid** — architecture globale, séquence par agent (Detection/Decision/Execution), explication des 3 piliers, config multi-modèle, arborescence : tout en Mermaid/Markdown plutôt qu'en ASCII-art.

Implémentation réalisée dans `agents/common/adk_factory.py` (nouveau), `agents/detection/`, `agents/decision/`, `agents/execution/`, `docker-compose.yml`, `.env.example`, `README.md`, `scripts/demo-1-full-pipeline.sh`.
